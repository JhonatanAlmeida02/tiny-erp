"""
Get_ObterProdutos_incremental.py
----------------------------------
Sincronização incremental de produtos, em DUAS etapas:

ETAPA 1 (bulk, barata): lista.atualizacoes.produtos
    Filtra por dataAlteracao e devolve, paginado (100/página), os campos
    "rápidos" de todo produto alterado desde o checkpoint: nome, código,
    preço, estoque mínimo/máximo, NCM, GTIN, situação, tipo, etc.
    NÃO traz o campo "marca".

ETAPA 2 (individual, só para quem mudou): produto.obter.php
    Para cada ID que veio na etapa 1 (não para o catálogo inteiro),
    busca o cadastro completo e extrai "marca" (e "categoria", útil
    também pro seu cruzamento no BI).

Isso evita o padrão antigo de chamar produto.obter.php para TODOS os
produtos sempre — agora só chama para quem realmente mudou.

⚠️ LIMITAÇÃO DA API: lista.atualizacoes.produtos só aceita datas de até
~29 dias atrás. Por isso o script tem DOIS MODOS, escolhidos automaticamente:

- MODO CARGA COMPLETA: usado quando não há checkpoint, ou quando o
  checkpoint salvo tem mais de 29 dias. Lista TODOS os IDs (produtos.pesquisa)
  e detalha cada um (produto.obter) — igual ao script antigo, mas só
  acontece nessas duas situações, não em toda execução.
- MODO INCREMENTAL: usado quando o checkpoint está dentro dos últimos
  29 dias. Usa lista.atualizacoes.produtos (bulk) + produto.obter só
  para quem mudou (etapa 2, pra pegar marca/categoria).

- O CSV final é sempre atualizado por upsert (mescla com o que já existia).

IMPORTANTE: exclusões de produtos não aparecem aqui (não são "alterações").
Rode reset_job("produtos") periodicamente (ex.: 1x por mês) pra reconciliar
exclusões via carga completa.
"""

import asyncio
import os
import time
from datetime import datetime, timedelta

import aiohttp
import pandas as pd
import requests

from checkpoint_utils import get_last_run, set_last_run

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")

URL_ATUALIZACOES = "https://api.tiny.com.br/api2/lista.atualizacoes.produtos"
URL_PESQUISA = "https://api.tiny.com.br/api2/produtos.pesquisa.php"
URL_OBTER = "https://api.tiny.com.br/api2/produto.obter.php"

# O endpoint lista.atualizacoes.produtos só aceita datas de até 29 dias atrás.
# Se o checkpoint for mais antigo que isso (ou não existir), cai pra carga completa.
LIMITE_DIAS_ATUALIZACOES = 29

PASTA_DESTINO = "TB_bases"
OUTPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseProdutos_Tiny.csv")

JOB_NAME = "produtos"
REQUEST_TIMEOUT = 30

# Limite da conta: 100 req/min. Etapa 1 é síncrona (poucas páginas, 100
# produtos por página) — intervalo confortável abaixo do limite.
INTERVALO_SEGUNDOS_ETAPA1 = 0.65  # ~92 req/min

# Etapa 2 é assíncrona (pode ser MUITAS chamadas, uma por produto alterado)
LIMIT_PER_MINUTE = 100
MAX_CONCURRENT = 8
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 5

COLUNAS_FINAIS = [
    "id", "nome", "codigo", "unidade", "localizacao",
    "preco", "preco_promocional", "preco_custo",
    "descricao_complementar", "ncm", "origem", "gtin", "gtin_embalagem",
    "peso_liquido", "peso_bruto", "estoque_minimo", "estoque_maximo",
    "id_fornecedor", "codigo_fornecedor", "codigo_pelo_fornecedor",
    "unidade_por_caixa", "situacao", "tipo", "tipo_variacao",
    "classe_ipi", "valor_ipi_fixo", "cod_lista_servicos", "obs",
    "data_alteracao",
    "marca", "categoria",  # vêm da etapa 2 (produto.obter.php)
]


# ================= ETAPA 1: lista.atualizacoes.produtos (síncrono) =================
def fetch_json_sync(url, params, tentativas=3):
    for tentativa in range(1, tentativas + 1):
        try:
            response = requests.post(url, data=params, timeout=REQUEST_TIMEOUT)
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


def checkpoint_dentro_do_limite(data_alteracao: str | None) -> bool:
    """Retorna True se o checkpoint existe e está dentro dos últimos
    LIMITE_DIAS_ATUALIZACOES dias (janela aceita pelo lista.atualizacoes.produtos)."""
    if not data_alteracao:
        return False
    try:
        dt = datetime.strptime(data_alteracao, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return False
    limite = datetime.now() - timedelta(days=LIMITE_DIAS_ATUALIZACOES)
    return dt >= limite


def listar_todos_ids_pesquisa() -> list:
    """Modo carga completa - etapa 1 alternativa: lista TODOS os IDs de produtos
    via produtos.pesquisa.php (paginado), sem filtro de data."""
    ids = []
    pagina = 1
    total_paginas = None

    while True:
        params = {"token": TOKEN_API, "formato": "json", "pesquisa": "", "pagina": pagina}
        print(f"[Carga completa] Buscando página {pagina} de IDs...")
        dados = fetch_json_sync(URL_PESQUISA, params)

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

        ids.extend([str(p["produto"]["id"]) for p in produtos_pagina if "produto" in p])
        print(f"[Carga completa] Página {pagina}/{total_paginas} - total de IDs até agora: {len(ids)}")

        if pagina >= total_paginas:
            break
        pagina += 1
        time.sleep(INTERVALO_SEGUNDOS_ETAPA1)

    return ids


def coletar_atualizacoes(data_alteracao: str | None) -> list:
    todos_produtos = []
    pagina = 1
    total_paginas = None
    data_filtro = data_alteracao or "01/01/2000"  # dataAlteracao é obrigatório

    while True:
        params = {
            "token": TOKEN_API, "formato": "json",
            "dataAlteracao": data_filtro, "pagina": pagina,
        }
        print(f"[Etapa 1] Buscando página {pagina} (desde {data_filtro})...")
        dados = fetch_json_sync(URL_ATUALIZACOES, params)

        if not dados:
            print(f"Falha ao buscar página {pagina}. Interrompendo.")
            break

        retorno = dados.get("retorno", {})
        if retorno.get("status") != "OK":
            erros = retorno.get("erros", [{"erro": "desconhecido"}])
            mensagens = [e.get("erro", "") for e in erros]
            if any("não retornou registros" in m.lower() for m in mensagens):
                # Isso não é um erro de verdade: é a forma da API dizer
                # "nenhuma atualização encontrada nesse período" (normal).
                print("ℹ️ Nenhuma atualização encontrada nesse período (resultado esperado).")
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

        print(f"[Etapa 1] Página {pagina}/{total_paginas} - {len(produtos_pagina)} produtos. "
              f"Total até agora: {len(todos_produtos)}")

        if pagina >= total_paginas:
            break
        pagina += 1
        time.sleep(INTERVALO_SEGUNDOS_ETAPA1)

    return todos_produtos


# ================= ETAPA 2: produto.obter.php (assíncrono, só p/ IDs alterados) =================
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


async def get_detalhe_produto(session, produto_id, rate_limiter):
    for attempt in range(MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            params = {"token": TOKEN_API, "id": produto_id, "formato": "json"}
            async with session.get(URL_OBTER, params=params) as response:
                text = await response.text()
                try:
                    data = __import__("json").loads(text)
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


async def worker(queue, session, rate_limiter, results, progress, erros):
    while not queue.empty():
        try:
            produto_id = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        pid, detalhe, erro = await get_detalhe_produto(session, produto_id, rate_limiter)
        if detalhe:
            results[pid] = detalhe
        if erro:
            erros.append({"id": pid, "erro": erro})
            print(f"\n❌ Erro no produto {pid}: {erro}")
        progress["processed"] += 1
        queue.task_done()


async def buscar_detalhes_completos(ids: list, label: str = "produtos") -> dict:
    """Retorna {id: detalhe_completo} para os IDs informados, respeitando
    o limite de 100 req/min. Reaproveitada tanto pela carga completa quanto
    pela etapa 2 do modo incremental."""
    if not ids:
        return {}

    rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    queue = asyncio.Queue()
    for pid in ids:
        await queue.put(pid)

    results = {}
    erros = []
    progress = {"processed": 0}
    total = len(ids)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(worker(queue, session, rate_limiter, results, progress, erros))
                   for _ in range(MAX_CONCURRENT)]

        while progress["processed"] < total:
            print(f"\r[{label}] {progress['processed']}/{total} produtos detalhados", end="", flush=True)
            await asyncio.sleep(1)

        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    print()  # quebra de linha após o progresso
    if erros:
        print(f"⚠️ {len(erros)} produtos falharam ao buscar detalhe (verifique manualmente).")

    return results


async def buscar_marca_categoria(ids: list) -> dict:
    """Retorna {id: {"marca": ..., "categoria": ...}} só para os IDs informados."""
    detalhes = await buscar_detalhes_completos(ids, label="Etapa 2")
    return {
        str(pid): {"marca": d.get("marca", ""), "categoria": d.get("categoria", "")}
        for pid, d in detalhes.items()
    }


def montar_linha_do_detalhe(detalhe: dict) -> dict:
    """Extrai de um retorno completo de produto.obter.php os campos que
    usamos no CSV final (tolera campos ausentes)."""
    return {
        "id": detalhe.get("id", ""),
        "nome": detalhe.get("nome", ""),
        "codigo": detalhe.get("codigo", ""),
        "unidade": detalhe.get("unidade", ""),
        "localizacao": detalhe.get("localizacao", ""),
        "preco": detalhe.get("preco", ""),
        "preco_promocional": detalhe.get("preco_promocional", ""),
        "preco_custo": detalhe.get("preco_custo", ""),
        "descricao_complementar": detalhe.get("descricao_complementar", ""),
        "ncm": detalhe.get("ncm", ""),
        "origem": detalhe.get("origem", ""),
        "gtin": detalhe.get("gtin", ""),
        "gtin_embalagem": detalhe.get("gtin_embalagem", ""),
        "peso_liquido": detalhe.get("peso_liquido", ""),
        "peso_bruto": detalhe.get("peso_bruto", ""),
        "estoque_minimo": detalhe.get("estoque_minimo", ""),
        "estoque_maximo": detalhe.get("estoque_maximo", ""),
        "id_fornecedor": detalhe.get("id_fornecedor", ""),
        "codigo_fornecedor": detalhe.get("codigo_fornecedor", ""),
        "codigo_pelo_fornecedor": detalhe.get("codigo_pelo_fornecedor", ""),
        "unidade_por_caixa": detalhe.get("unidade_por_caixa", ""),
        "situacao": detalhe.get("situacao", ""),
        "tipo": detalhe.get("tipo", detalhe.get("classe_produto", "")),
        "tipo_variacao": detalhe.get("tipo_variacao", ""),
        "classe_ipi": detalhe.get("classe_ipi", ""),
        "valor_ipi_fixo": detalhe.get("valor_ipi_fixo", ""),
        "cod_lista_servicos": detalhe.get("cod_lista_servicos", ""),
        "obs": detalhe.get("obs", ""),
        "data_alteracao": detalhe.get("data_alteracao", ""),
        "marca": detalhe.get("marca", ""),
        "categoria": detalhe.get("categoria", ""),
    }


# ================= MERGE COM CSV EXISTENTE (UPSERT) =================
def mesclar_com_csv_existente(produtos_novos: list, marca_categoria: dict) -> pd.DataFrame:
    df_novo = pd.DataFrame(produtos_novos)
    if df_novo.empty:
        df_novo = pd.DataFrame(columns=COLUNAS_FINAIS)
    df_novo["id"] = df_novo["id"].astype(str)

    df_novo["marca"] = df_novo["id"].map(lambda i: marca_categoria.get(i, {}).get("marca", ""))
    df_novo["categoria"] = df_novo["id"].map(lambda i: marca_categoria.get(i, {}).get("categoria", ""))

    df_novo = df_novo.reindex(columns=COLUNAS_FINAIS, fill_value="")

    if os.path.exists(OUTPUT_FILE):
        df_existente = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
        df_existente["id"] = df_existente["id"].astype(str)
        df_existente = df_existente[~df_existente["id"].isin(df_novo["id"])]
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_final = df_novo

    return df_final.reindex(columns=COLUNAS_FINAIS, fill_value="")


# ================= FUNÇÃO PRINCIPAL =================
def main():
    if TOKEN_API in ("", "COLOQUE_SEU_TOKEN_AQUI"):
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        return

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    data_alteracao = get_last_run(JOB_NAME)
    inicio = time.time()

    if checkpoint_dentro_do_limite(data_alteracao):
        # ================= MODO INCREMENTAL =================
        print(f"🔄 Sincronização incremental: buscando produtos alterados desde {data_alteracao}")

        produtos = coletar_atualizacoes(data_alteracao)
        if not produtos:
            print("Nenhuma atualização de produto encontrada nesse período. Nada a fazer.")
            set_last_run(JOB_NAME)
            return

        ids_alvo = [str(p["id"]) for p in produtos if p.get("id")]
        print(f"\n[Etapa 2] Buscando marca/categoria para {len(ids_alvo)} produtos...")
        marca_categoria = asyncio.run(buscar_marca_categoria(ids_alvo))

        df_final = mesclar_com_csv_existente(produtos, marca_categoria)
        total_processados = len(produtos)

    else:
        # ================= MODO CARGA COMPLETA =================
        # Sem checkpoint, ou checkpoint com mais de 29 dias: o endpoint de
        # atualizações não aceita datas tão antigas, então buscamos TODOS
        # os IDs e detalhamos cada um (como no script antigo, mas só
        # acontece aqui, não em toda execução).
        if data_alteracao:
            print(f"⚠️ Checkpoint de {data_alteracao} tem mais de {LIMITE_DIAS_ATUALIZACOES} dias — "
                  f"o endpoint incremental não aceita. Fazendo carga completa.")
        else:
            print("🚀 Nenhum checkpoint encontrado: fazendo carga completa (primeira execução).")

        ids_todos = listar_todos_ids_pesquisa()
        if not ids_todos:
            print("Nenhum produto encontrado. Nada a fazer.")
            return

        print(f"\n[Carga completa] Detalhando {len(ids_todos)} produtos...")
        detalhes_completos = asyncio.run(buscar_detalhes_completos(ids_todos))

        produtos_montados = [montar_linha_do_detalhe(d) for d in detalhes_completos.values()]
        # Na carga completa a marca/categoria já vêm no próprio detalhe, então
        # passamos um dicionário vazio pro merge (não precisa buscar de novo).
        df_final = mesclar_com_csv_existente(produtos_montados, {})
        total_processados = len(produtos_montados)

    df_final.to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")
    set_last_run(JOB_NAME)

    elapsed = time.time() - inicio
    print(f"\n✅ Produtos sincronizados! {total_processados} produtos processados nesta rodada.")
    print(f"📄 CSV salvo: {OUTPUT_FILE} (total de {len(df_final)} produtos na base)")
    print(f"⏱ Tempo total: {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecução interrompida manualmente. Checkpoint NÃO foi atualizado.")
    except Exception as e:
        print(f"Erro inesperado: {e}. Checkpoint NÃO foi atualizado.")