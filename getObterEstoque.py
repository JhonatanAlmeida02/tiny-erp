"""
getObterEstoque.py
------------------
Sincronização de estoque em DOIS MODOS, escolhidos automaticamente (mesmo
padrão do getObterProdutos.py):

MODO CARGA COMPLETA (sem checkpoint salvo):
    Lista TODOS os produtos (produtos.pesquisa.php) e busca o estoque de cada
    um individualmente (produto.obter.estoque.php). É o único jeito de
    reconstruir o catálogo inteiro — o endpoint de "atualizações" abaixo
    NUNCA lista tudo, só o que mudou recentemente.

MODO INCREMENTAL (checkpoint existente): lista.atualizacoes.estoque
    Filtra por dataAlteracao e devolve, paginado, só os produtos que tiveram
    estoque alterado desde a última execução. MUITO mais barato (poucas
    chamadas), mas só aceita dataAlteracao dos últimos ~29 dias (trava não
    documentada oficialmente).

- O CSV final é sempre atualizado por upsert (mescla o que veio de novo com
  o que já existia).

IMPORTANTE: exclusões de produtos no Tiny não aparecem no modo incremental
(não são "alterações"). Apague TB_bases/checkpoints/estoque.json
periodicamente pra reconciliar via carga completa de novo.
"""

import asyncio
import json as _json
import os
import sys
import time
import traceback
import unicodedata
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
import requests

from checkpoint_utils import agora, get_last_run, set_last_run

# Sem isso o Python segura a saida em buffer e o log do GitHub Actions
# parece travado ate o script terminar.
sys.stdout.reconfigure(line_buffering=True)

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")

# Sem ".php" no final - o endpoint real da lista.atualizacoes.estoque não usa essa extensão.
API_URL = "https://api.tiny.com.br/api2/lista.atualizacoes.estoque"
URL_PESQUISA = "https://api.tiny.com.br/api2/produtos.pesquisa.php"
URL_OBTER_ESTOQUE = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
PASTA_DESTINO = "TB_bases"
OUTPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseEstoque_Tiny.csv")

JOB_NAME = "estoque"
REQUEST_TIMEOUT = 30
INTERVALO_SEGUNDOS_API = 0.3

# A API só aceita dataAlteracao dentro dos últimos ~29 dias (mesma trava do
# endpoint de produtos, só que não documentada na página oficial deste aqui).
LIMITE_DIAS_ATUALIZACOES = 29

# Modo carga completa: mesmo limite de 100 req/min da conta, com chamadas
# assíncronas (igual getObterProdutos.py e getObterTags.py).
LIMIT_PER_MINUTE = 100
MAX_CONCURRENT = 8
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 5

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


# ================= NORMALIZAÇÃO =================
def normalizar_nome(nome) -> str:
    """Remove acentos, caixa e espaços das pontas, pra casar com mapa_depositos.
    Aceita None/NaN sem estourar (a API às vezes devolve nome de depósito nulo)."""
    if not nome:
        return ""
    return (unicodedata.normalize("NFKD", str(nome))
            .encode("ASCII", "ignore")
            .decode("utf-8")
            .lower()
            .strip())


# ================= REQUISIÇÃO COM RETRY =================
def fetch_json(params, tentativas=3, url=None):
    url = url or API_URL
    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
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
            print(f"Erro na requisição: {e}")

        print(f"Tentativa {tentativa}/{tentativas} falhou. Aguardando 10s...")
        time.sleep(10)
    return None


# ================= MODO CARGA COMPLETA: listar todos os produtos =================
def listar_todos_produtos_pesquisa() -> dict:
    """Lista TODOS os produtos via produtos.pesquisa.php (paginado, sem filtro
    de data) — usado só no modo carga completa.

    Retorna {id: {"codigo":..., "nome":..., "unidade":...}}. Guardamos esses
    campos porque produto.obter.estoque.php não devolve código nem unidade;
    sem isso o CSV da carga completa sairia com colunas vazias.
    """
    catalogo = {}
    pagina = 1
    total_paginas = None

    while True:
        params = {"token": TOKEN_API, "formato": "json", "pesquisa": "", "pagina": pagina}
        print(f"[Carga completa] Buscando página {pagina} de produtos...")
        dados = fetch_json(params, url=URL_PESQUISA)

        if not dados:
            print(f"Falha ao buscar página {pagina}. Interrompendo.")
            break

        retorno = dados.get("retorno", {})
        if retorno.get("status") != "OK":
            print(f"Erro da API: {retorno.get('erros', [{'erro': 'desconhecido'}])}")
            break

        if total_paginas is None:
            total_paginas = int(retorno.get("numero_paginas", 1))
            print(f"Total de páginas: {total_paginas}")

        produtos_pagina = retorno.get("produtos", [])
        if not produtos_pagina:
            break

        for p in produtos_pagina:
            produto = p.get("produto")
            if not produto:
                continue
            pid = str(produto.get("id", ""))
            if not pid:
                continue
            catalogo[pid] = {
                "codigo": produto.get("codigo", ""),
                "nome": produto.get("nome", ""),
                "unidade": produto.get("unidade", ""),
            }

        print(f"[Carga completa] Página {pagina}/{total_paginas} - "
              f"total de produtos até agora: {len(catalogo)}")

        if pagina >= total_paginas:
            break
        pagina += 1
        time.sleep(INTERVALO_SEGUNDOS_API)

    return catalogo


# ================= MODO CARGA COMPLETA: estoque por ID (assíncrono) =================
class FastRateLimiter:
    def __init__(self, max_per_minute=100):
        self.max_per_minute = max_per_minute
        self.requests = []
        self.lock = asyncio.Lock()
        self.min_interval = 60 / max_per_minute
        self.last_request = 0

    async def acquire(self):
        async with self.lock:
            now = time.time()
            time_since_last = now - self.last_request
            if time_since_last < self.min_interval:
                await asyncio.sleep(self.min_interval - time_since_last)
                now = time.time()
            self.requests = [t for t in self.requests if now - t < 60]
            if len(self.requests) >= self.max_per_minute:
                wait_time = 61 - (now - self.requests[0])
                print(f"\n⏸ Limite atingido, aguardando {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                return await self.acquire()
            self.requests.append(now)
            self.last_request = now


async def get_estoque_produto(session, produto_id, rate_limiter):
    for attempt in range(MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            params = {"token": TOKEN_API, "id": produto_id, "formato": "json"}
            async with session.get(URL_OBTER_ESTOQUE, params=params) as response:
                text = await response.text()
                try:
                    data = _json.loads(text)
                except Exception:
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(DELAY_BETWEEN_RETRIES)
                        continue
                    return produto_id, None, f"Resposta inválida: {text[:100]}..."
                if data.get("retorno", {}).get("status") == "OK":
                    return produto_id, data["retorno"]["produto"], None
                erro = data.get("retorno", {}).get("erros", [{"erro": "Erro desconhecido"}])[0]["erro"]
                if "bloque" in erro.lower() and attempt < MAX_RETRIES:
                    await asyncio.sleep(DELAY_BETWEEN_RETRIES)
                    continue
                return produto_id, None, erro
        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2)
                continue
            return produto_id, None, str(e)
    return produto_id, None, "Falha após várias tentativas"


async def _worker(queue, session, rate_limiter, results, progress, erros):
    while not queue.empty():
        try:
            produto_id = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        pid, detalhe, erro = await get_estoque_produto(session, produto_id, rate_limiter)
        if detalhe:
            # Garante que o ID volte junto: o endpoint nem sempre repete esse campo.
            detalhe.setdefault("id", pid)
            results.append(detalhe)
        if erro:
            erros.append({"id": pid, "erro": erro})
            print(f"\n❌ Erro no produto {pid}: {erro}")
        progress["processed"] += 1
        if progress["processed"] % 100 == 0 or progress["processed"] == progress["total"]:
            print(f"[Carga completa] {progress['processed']}/{progress['total']} produtos com estoque coletado")
        queue.task_done()


async def buscar_estoques_completos(ids: list) -> list:
    """Retorna a lista de dicts "produto" (com saldo/depósitos) pra cada ID,
    respeitando o limite de 100 req/min — usado só no modo carga completa."""
    if not ids:
        return []

    rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    queue = asyncio.Queue()
    for pid in ids:
        await queue.put(pid)

    results = []
    erros = []
    progress = {"processed": 0, "total": len(ids)}

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(_worker(queue, session, rate_limiter, results, progress, erros))
                   for _ in range(MAX_CONCURRENT)]
        await asyncio.gather(*workers)

    if erros:
        print(f"⚠️ {len(erros)} produtos falharam ao buscar estoque (veja acima).")

    return results


# ================= COLETA PAGINADA (INCREMENTAL) =================
def coletar_atualizacoes(data_alteracao: str):
    """
    Retorna a lista de dicts "produto" (já desembrulhados) vindos da API de
    atualizações, desde a data do checkpoint.

    dataAlteracao é OBRIGATÓRIO e só aceita datas dentro de ~29 dias atrás.
    Se o checkpoint for mais antigo que isso, usamos o mais antigo permitido
    e avisamos — nesse caso pode haver buraco na base, e o certo é apagar o
    checkpoint pra forçar carga completa.
    """
    todos_produtos = []
    pagina = 1
    total_paginas = None

    data_minima_aceita = (agora() - timedelta(days=LIMITE_DIAS_ATUALIZACOES)).strftime("%d/%m/%Y")

    try:
        dt_checkpoint = datetime.strptime(data_alteracao, "%d/%m/%Y %H:%M:%S")
        limite = agora().replace(tzinfo=None) - timedelta(days=LIMITE_DIAS_ATUALIZACOES)
        if dt_checkpoint >= limite:
            data_filtro = data_alteracao
        else:
            data_filtro = data_minima_aceita
            print(f"⚠️ Checkpoint ({data_alteracao}) é mais antigo que o limite de "
                  f"{LIMITE_DIAS_ATUALIZACOES} dias da API. Usando {data_filtro}. "
                  f"Considere apagar o checkpoint pra rodar carga completa.")
    except ValueError:
        data_filtro = data_minima_aceita
        print(f"⚠️ Checkpoint em formato inesperado ({data_alteracao}). Usando {data_filtro}.")

    while True:
        params = {"token": TOKEN_API, "formato": "json", "pagina": pagina, "dataAlteracao": data_filtro}

        print(f"Buscando página {pagina} (desde {data_filtro})...")
        dados = fetch_json(params)

        if not dados:
            raise RuntimeError(f"Falha ao buscar página {pagina} após todas as tentativas.")

        retorno = dados.get("retorno", {})
        if retorno.get("status") != "OK":
            erros = retorno.get("erros", [{"erro": "desconhecido"}])
            mensagens = [e.get("erro", "") for e in erros]
            if any("não retornou registros" in m.lower() for m in mensagens):
                print("ℹ️ Nenhuma atualização de estoque encontrada nesse período (resultado esperado).")
                break
            raise RuntimeError(f"Erro da API na página {pagina}: {erros}")

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
def processar_produto(produto: dict, meta: dict = None) -> dict:
    """Monta a linha do CSV. `meta` traz código/nome/unidade vindos da
    produtos.pesquisa.php (carga completa), usados só quando o endpoint de
    estoque não devolve esses campos."""
    meta = meta or {}

    linha = {
        "ID Produto": produto.get("id", "") or meta.get("id", ""),
        "Código": produto.get("codigo", "") or meta.get("codigo", ""),
        "Nome": produto.get("nome", "") or meta.get("nome", ""),
        "Unidade": produto.get("unidade", "") or meta.get("unidade", ""),
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


# ================= MODOS =================
def executar_carga_completa() -> list:
    """Reconstrói a base inteira: lista todos os produtos e busca o estoque
    de cada um. Caro (1 request por produto), mas é o único jeito de ter o
    catálogo completo."""
    print("🚀 Nenhum checkpoint encontrado: executando CARGA COMPLETA.")
    catalogo = listar_todos_produtos_pesquisa()

    if not catalogo:
        raise RuntimeError("Carga completa não retornou nenhum produto. "
                           "Verifique o token e a API antes de sobrescrever a base.")

    ids = list(catalogo.keys())
    print(f"📦 {len(ids)} produtos encontrados. Buscando estoque de cada um "
          f"(~{len(ids) / LIMIT_PER_MINUTE:.0f} min no limite de {LIMIT_PER_MINUTE} req/min)...")

    produtos = asyncio.run(buscar_estoques_completos(ids))
    return [processar_produto(p, catalogo.get(str(p.get("id", "")), {})) for p in produtos]


def executar_incremental(data_alteracao: str) -> list:
    print(f"🔄 Sincronização incremental: buscando alterações desde {data_alteracao}")
    produtos = coletar_atualizacoes(data_alteracao)
    return [processar_produto(p) for p in produtos]


# ================= FUNÇÃO PRINCIPAL =================
def main():
    if TOKEN_API in ("", "COLOQUE_SEU_TOKEN_AQUI"):
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        sys.exit(1)

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    inicio = time.time()
    data_alteracao = get_last_run(JOB_NAME)

    if data_alteracao:
        linhas = executar_incremental(data_alteracao)
    else:
        linhas = executar_carga_completa()

    if not linhas:
        print("Nenhuma atualização de estoque encontrada nesse período. Nada a fazer.")
        # Ainda assim, atualiza o checkpoint: rodou com sucesso, só não havia mudanças.
        set_last_run(JOB_NAME)
        return

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
        sys.exit(130)
    except Exception as e:
        print(f"Erro inesperado: {e}. Checkpoint NÃO foi atualizado.")
        traceback.print_exc()
        # Exit code != 0 pra que o GitHub Actions marque o job como falho e
        # NÃO publique o CSV antigo no SharePoint como se fosse atualizado.
        sys.exit(1)
