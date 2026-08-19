"""
Get_ObterEstoque_incremental.py
--------------------------------
Substitui a lógica antiga de "1 chamada de API por produto, sempre".

Em vez disso, usa o endpoint lista.atualizacoes.estoque, que filtra por
dataAlteracao e devolve, numa única série de chamadas paginadas, só os
produtos que tiveram estoque alterado (ou criados) desde a última execução.

- Primeira execução (sem checkpoint salvo): carga completa. dataAlteracao é
  obrigatório neste endpoint, então usamos uma data bem antiga (01/01/2000)
  — diferente do endpoint de produtos, este NÃO tem trava de 30 dias.
- Execuções seguintes: só o que mudou desde a última vez.
- O CSV final é atualizado por upsert (mescla o que mudou com o que já
  existia), não é sobrescrito do zero.

IMPORTANTE: exclusões de produtos no Tiny não aparecem aqui (porque não são
"alterações"). Rode esse mesmo script periodicamente com reset_job("estoque")
(ex.: 1x por semana) pra reconciliar exclusões via carga completa.
"""

import os
import time
import unicodedata

import pandas as pd
import requests

from checkpoint_utils import get_last_run, set_last_run

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")

# Sem ".php" no final - o endpoint real da lista.atualizacoes.estoque não usa essa extensão.
API_URL = "https://api.tiny.com.br/api2/lista.atualizacoes.estoque"
PASTA_DESTINO = "TB_bases"
OUTPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseEstoque_Tiny.csv")

JOB_NAME = "estoque"
REQUEST_TIMEOUT = 30
INTERVALO_SEGUNDOS_API = 0.3

ordem_depositos = ["Padrão", "Varejo", "ML FULL", "Garantia", "Qualidade",
                    "Quarentena", "Shelf Life", "Custo"]

mapa_depositos = {
    "padrao": "Padrão",
    "varejo": "Varejo",
    "ml full": "ML FULL",
    "mercado livre full": "ML FULL",
    "garantia": "Garantia",
    "qualidade": "Qualidade",
    "quarentena": "Quarentena",
    "shelf life": "Shelf Life",
    "custo": "Custo",
}


def normalizar_nome(nome: str) -> str:
    return unicodedata.normalize("NFKD", nome).encode("ASCII", "ignore").decode("utf-8").lower().strip()


# ================= REQUISIÇÃO COM RETRY =================
def fetch_json(params, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=REQUEST_TIMEOUT)
            if response.status_code == 429:
                print("⚠️ 429 Too Many Requests - aguardando 60s...")
                time.sleep(60)
                continue
            response.raise_for_status()
            text = response.text
            if "API Bloqueada" in text:
                print("⚠️ API bloqueada - aguardando 60s...")
                time.sleep(60)
                continue
            return response.json()
        except requests.Timeout:
            print(f"Timeout ({REQUEST_TIMEOUT}s) na requisição.")
        except requests.ConnectionError:
            print("Erro de conexão.")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print(f"Tentativa {tentativa}/{tentativas} falhou. Aguardando 10s...")
        time.sleep(10)
    return None


# ================= COLETA PAGINADA =================
def coletar_atualizacoes(data_alteracao: str | None):
    """
    Retorna a lista de dicts "produto" (já desembrulhados) vindos da API.
    Se data_alteracao for None, faz carga completa (sem filtro de data).
    """
    todos_produtos = []
    pagina = 1
    total_paginas = None
    # dataAlteracao é OBRIGATÓRIO neste endpoint (não pode ser omitido).
    # Sem checkpoint, usamos uma data bem antiga pra simular carga completa
    # — esse endpoint não tem a trava de 30 dias que o de produtos tem.
    data_filtro = data_alteracao or "01/01/2000"

    while True:
        params = {"token": TOKEN_API, "formato": "json", "pagina": pagina, "dataAlteracao": data_filtro}

        print(f"Buscando página {pagina}" + (f" (desde {data_filtro})" if data_alteracao else " (carga completa)") + "...")
        dados = fetch_json(params)

        if not dados:
            print(f"Falha ao buscar página {pagina}. Interrompendo.")
            break

        retorno = dados.get("retorno", {})
        if retorno.get("status") != "OK":
            erros = retorno.get("erros", [{"erro": "desconhecido"}])
            mensagens = [e.get("erro", "") for e in erros]
            if any("não retornou registros" in m.lower() for m in mensagens):
                print("ℹ️ Nenhuma atualização de estoque encontrada nesse período (resultado esperado).")
            else:
                print(f"Erro da API: {erros}")
            break

        if total_paginas is None:
            total_paginas = int(retorno.get("numero_paginas", 1))
            print(f"Total de páginas: {total_paginas}")

        produtos_pagina = retorno.get("produtos", [])
        if not produtos_pagina:
            break

        for p in produtos_pagina:
            produto = p.get("produto")
            if produto:
                todos_produtos.append(produto)

        print(f"Página {pagina}/{total_paginas} - {len(produtos_pagina)} produtos coletados. "
              f"Total até agora: {len(todos_produtos)}")

        if pagina >= total_paginas:
            break

        pagina += 1
        time.sleep(INTERVALO_SEGUNDOS_API)

    return todos_produtos


# ================= PROCESSAMENTO DE CADA PRODUTO =================
def processar_produto(produto: dict) -> dict:
    linha = {
        "ID Produto": produto.get("id", ""),
        "Código": produto.get("codigo", ""),
        "Nome": produto.get("nome", ""),
        "Unidade": produto.get("unidade", ""),
        "Saldo Total": produto.get("saldo", ""),
        "Saldo Reservado": produto.get("saldoReservado", ""),
    }

    depositos_dict = {dep: "" for dep in ordem_depositos}
    for d in produto.get("depositos", []):
        deposito_info = d.get("deposito", {})
        nome_dep = deposito_info.get("nome", "")
        saldo_dep = deposito_info.get("saldo", "")
        nome_norm = normalizar_nome(nome_dep)
        if nome_norm in mapa_depositos:
            coluna = mapa_depositos[nome_norm]
            depositos_dict[coluna] = saldo_dep

    linha.update(depositos_dict)
    return linha


# ================= MERGE COM CSV EXISTENTE (UPSERT) =================
def mesclar_com_csv_existente(linhas_novas: list) -> pd.DataFrame:
    colunas_base = ["ID Produto", "Código", "Nome", "Unidade", "Saldo Total", "Saldo Reservado"]
    colunas_finais = colunas_base + ordem_depositos

    df_novo = pd.DataFrame(linhas_novas)
    if df_novo.empty:
        df_novo = pd.DataFrame(columns=colunas_finais)
    df_novo = df_novo.reindex(columns=colunas_finais, fill_value="")
    df_novo["ID Produto"] = df_novo["ID Produto"].astype(str)

    if os.path.exists(OUTPUT_FILE):
        df_existente = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
        df_existente["ID Produto"] = df_existente["ID Produto"].astype(str)
        # Remove do existente as linhas que serão atualizadas, depois concatena as novas
        df_existente = df_existente[~df_existente["ID Produto"].isin(df_novo["ID Produto"])]
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_final = df_novo

    return df_final.reindex(columns=colunas_finais, fill_value="")


# ================= FUNÇÃO PRINCIPAL =================
def main():
    if TOKEN_API in ("", "COLOQUE_SEU_TOKEN_AQUI"):
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        return

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    data_alteracao = get_last_run(JOB_NAME)
    if data_alteracao:
        print(f"🔄 Sincronização incremental: buscando alterações desde {data_alteracao}")
    else:
        print("🚀 Nenhum checkpoint encontrado: fazendo carga completa (primeira execução).")

    inicio = time.time()
    produtos = coletar_atualizacoes(data_alteracao)

    if not produtos:
        print("Nenhuma atualização de estoque encontrada nesse período. Nada a fazer.")
        # Ainda assim, atualiza o checkpoint: rodou com sucesso, só não havia mudanças.
        set_last_run(JOB_NAME)
        return

    linhas = [processar_produto(p) for p in produtos]
    df_final = mesclar_com_csv_existente(linhas)
    df_final.to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")

    # Só marca o checkpoint como concluído DEPOIS que o CSV foi salvo com sucesso.
    set_last_run(JOB_NAME)

    elapsed = time.time() - inicio
    print(f"\n✅ Estoque sincronizado! {len(linhas)} produtos atualizados nesta rodada.")
    print(f"📄 CSV salvo: {OUTPUT_FILE} (total de {len(df_final)} produtos na base)")
    print(f"⏱ Tempo total: {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecução interrompida manualmente. Checkpoint NÃO foi atualizado.")
    except Exception as e:
        print(f"Erro inesperado: {e}. Checkpoint NÃO foi atualizado.")