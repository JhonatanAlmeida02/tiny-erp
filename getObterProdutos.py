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

from checkpoint_utils import agora, get_last_run, set_last_run

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
LIMIT_PER_MINUTE = int(os.environ.get("TINY_LIMITE_MINUTO", "100"))
MAX_CONCURRENT = 8
# FORCAR_CARGA_COMPLETA=1 refaz a base inteira (preenche os campos novos em todos os produtos).
FORCAR_CARGA_COMPLETA = os.environ.get("FORCAR_CARGA_COMPLETA", "").strip().lower() in ("1", "true", "sim", "yes")
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
    # ---- demais campos do cadastro (produto.obter.php), acrescentados no fim ----
    "data_criacao", "preco_custo_medio", "garantia", "cest",
    "id_produto_pai", "classe_produto", "sob_encomenda", "dias_preparacao",
    "tipo_embalagem", "altura_embalagem", "largura_embalagem",
    "comprimento_embalagem", "diametro_embalagem",
    "grade", "variacoes", "kit",
    "imagem_principal", "anexos", "imagens_externas",
    "seo_title", "seo_keywords", "seo_description", "link_video", "slug",
    "mapeamentos",
]
# Listas viram texto legível numa célula, separadas por " | ".
SEP_LISTA = " | "


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
    limite = agora().replace(tzinfo=None) - timedelta(days=LIMITE_DIAS_ATUALIZACOES)
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


def _primeiro(d: dict, *chaves, padrao=""):
    """Primeiro valor presente entre os nomes possíveis (a API mistura
    snake_case e camelCase: tipo_variacao x tipoVariacao)."""
    for k in chaves:
        v = d.get(k)
        if v not in (None, ""):
            return v
    return padrao


def _desembrulhar(item, chave):
    """A API v2 embrulha itens de lista: {"anexo": "url"}, {"item": {...}}."""
    if isinstance(item, dict) and chave in item:
        return item[chave]
    return item


def _texto_grade(grade) -> str:
    """Grade chave-valor -> "Cor: Azul, Tamanho: P"."""
    if not grade:
        return ""
    if isinstance(grade, dict):
        return ", ".join(f"{k}: {v}" for k, v in grade.items())
    if isinstance(grade, list):
        partes = []
        for g in grade:
            if isinstance(g, dict):
                partes.extend(f"{k}: {v}" for k, v in g.items())
            elif g not in (None, ""):
                partes.append(str(g))
        return ", ".join(partes)
    return str(grade)


def _lista(valores) -> str:
    return SEP_LISTA.join(str(v) for v in valores if v not in (None, ""))


def montar_linha_do_detalhe(detalhe: dict) -> dict:
    """Extrai de um retorno completo de produto.obter.php TODOS os campos do
    cadastro que vão para o CSV final (tolera campos ausentes)."""
    d = detalhe or {}
    anexos = [_desembrulhar(a, "anexo") for a in (d.get("anexos") or [])]
    anexos = [a.get("url", "") if isinstance(a, dict) else a for a in anexos]
    externas = []
    for im in d.get("imagens_externas") or []:
        im = _desembrulhar(im, "imagem_externa")
        externas.append(im.get("url", "") if isinstance(im, dict) else im)
    variacoes = []
    for v in d.get("variacoes") or []:
        v = _desembrulhar(v, "variacao") or {}
        if isinstance(v, dict):
            g = _texto_grade(v.get("grade"))
            variacoes.append(f"{v.get('codigo') or v.get('id', '')}" + (f" ({g})" if g else ""))
    kit = []
    for it in d.get("kit") or []:
        it = _desembrulhar(it, "item") or {}
        if isinstance(it, dict):
            kit.append(f"{it.get('id_produto', '')} x {it.get('quantidade', '')}")
    mapeamentos = []
    for m in d.get("mapeamentos") or []:
        m = _desembrulhar(m, "mapeamento") or {}
        if isinstance(m, dict):
            mapeamentos.append(f"{m.get('idEcommerce', '')}:{m.get('skuMapeamento', '')}")
    return {
        "id": d.get("id", ""),
        "nome": d.get("nome", ""),
        "codigo": d.get("codigo", ""),
        "unidade": d.get("unidade", ""),
        "localizacao": d.get("localizacao", ""),
        "preco": d.get("preco", ""),
        "preco_promocional": d.get("preco_promocional", ""),
        "preco_custo": d.get("preco_custo", ""),
        "descricao_complementar": d.get("descricao_complementar", ""),
        "ncm": d.get("ncm", ""),
        "origem": d.get("origem", ""),
        "gtin": d.get("gtin", ""),
        "gtin_embalagem": d.get("gtin_embalagem", ""),
        "peso_liquido": d.get("peso_liquido", ""),
        "peso_bruto": d.get("peso_bruto", ""),
        "estoque_minimo": d.get("estoque_minimo", ""),
        "estoque_maximo": d.get("estoque_maximo", ""),
        "id_fornecedor": d.get("id_fornecedor", ""),
        "codigo_fornecedor": d.get("codigo_fornecedor", ""),
        "codigo_pelo_fornecedor": d.get("codigo_pelo_fornecedor", ""),
        "unidade_por_caixa": d.get("unidade_por_caixa", ""),
        "situacao": d.get("situacao", ""),
        "tipo": d.get("tipo", d.get("classe_produto", "")),
        # produto.obter devolve "tipoVariacao" (camelCase); antes ficava vazio na carga completa
        "tipo_variacao": _primeiro(d, "tipo_variacao", "tipoVariacao"),
        "classe_ipi": d.get("classe_ipi", ""),
        "valor_ipi_fixo": d.get("valor_ipi_fixo", ""),
        "cod_lista_servicos": d.get("cod_lista_servicos", ""),
        "obs": d.get("obs", ""),
        "data_alteracao": _primeiro(d, "data_alteracao", "dataAlteracao"),
        "marca": d.get("marca", ""),
        "categoria": d.get("categoria", ""),
        # ---- campos novos ----
        "data_criacao": _primeiro(d, "data_criacao", "dataCriacao"),
        "preco_custo_medio": d.get("preco_custo_medio", ""),
        "garantia": d.get("garantia", ""),
        "cest": d.get("cest", ""),
        "id_produto_pai": _primeiro(d, "idProdutoPai", "id_produto_pai"),
        "classe_produto": d.get("classe_produto", ""),
        "sob_encomenda": d.get("sob_encomenda", ""),
        "dias_preparacao": d.get("dias_preparacao", ""),
        "tipo_embalagem": _primeiro(d, "tipoEmbalagem", "tipo_embalagem"),
        "altura_embalagem": _primeiro(d, "alturaEmbalagem", "altura_embalagem"),
        "largura_embalagem": _primeiro(d, "larguraEmbalagem", "largura_embalagem"),
        "comprimento_embalagem": _primeiro(d, "comprimentoEmbalagem", "comprimento_embalagem"),
        "diametro_embalagem": _primeiro(d, "diametroEmbalagem", "diametro_embalagem"),
        "grade": _texto_grade(d.get("grade")),
        "variacoes": _lista(variacoes),
        "kit": _lista(kit),
        "imagem_principal": next((u for u in anexos + externas if u), ""),
        "anexos": _lista(anexos),
        "imagens_externas": _lista(externas),
        "seo_title": d.get("seo_title", ""),
        "seo_keywords": d.get("seo_keywords", ""),
        "seo_description": d.get("seo_description", ""),
        "link_video": d.get("link_video", ""),
        "slug": d.get("slug", ""),
        "mapeamentos": _lista(mapeamentos),
    }


# ================= MERGE COM CSV EXISTENTE (UPSERT) =================
def mesclar_com_csv_existente(produtos_novos: list, marca_categoria: dict) -> pd.DataFrame:
    df_novo = pd.DataFrame(produtos_novos)
    if df_novo.empty:
        df_novo = pd.DataFrame(columns=COLUNAS_FINAIS)
    df_novo["id"] = df_novo["id"].astype(str)

    df_novo = df_novo.reindex(columns=COLUNAS_FINAIS, fill_value="")

    # Só sobrescreve marca/categoria com o dicionário da etapa 2 quando ele
    # vier preenchido (modo incremental). Na carga completa, marca_categoria
    # é {} de propósito — os valores corretos já vieram em produtos_novos
    # (via produto.obter.php), então NÃO mexemos neles aqui (bug anterior:
    # sobrescrevia tudo com "" mesmo quando já estava certo).
    if marca_categoria:
        df_novo["marca"] = df_novo["id"].map(lambda i: marca_categoria.get(i, {}).get("marca", ""))
        df_novo["categoria"] = df_novo["id"].map(lambda i: marca_categoria.get(i, {}).get("categoria", ""))

    if os.path.exists(OUTPUT_FILE):
        df_existente = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
        df_existente["id"] = df_existente["id"].astype(str)
        # produto.obter não devolve data_alteracao (e às vezes tipo_variacao):
        # se o valor novo vier vazio, mantém o que já estava na base
        antigos = df_existente.drop_duplicates("id", keep="last").set_index("id")
        for campo in ("data_alteracao", "tipo_variacao"):
            if campo in antigos.columns:
                vazio = df_novo[campo].fillna("").astype(str).str.strip() == ""
                df_novo.loc[vazio, campo] = df_novo.loc[vazio, "id"].map(antigos[campo]).fillna("")
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

    if FORCAR_CARGA_COMPLETA:
        print("🧹 FORCAR_CARGA_COMPLETA ligado: refazendo a base inteira com todos os campos.")

    if checkpoint_dentro_do_limite(data_alteracao) and not FORCAR_CARGA_COMPLETA:
        # ================= MODO INCREMENTAL =================
        print(f"🔄 Sincronização incremental: buscando produtos alterados desde {data_alteracao}")

        produtos = coletar_atualizacoes(data_alteracao)
        if not produtos:
            print("Nenhuma atualização de produto encontrada nesse período. Nada a fazer.")
            set_last_run(JOB_NAME)
            return

        ids_alvo = [str(p["id"]) for p in produtos if p.get("id")]
        print(f"\n[Etapa 2] Buscando o cadastro completo de {len(ids_alvo)} produtos...")
        detalhes = asyncio.run(buscar_detalhes_completos(ids_alvo, label="Etapa 2"))

        linhas = []
        for p in produtos:
            pid = str(p.get("id", ""))
            if pid in detalhes:
                linha = montar_linha_do_detalhe(detalhes[pid])
                # a etapa 1 traz data_alteracao e tipo_variacao no formato da base: têm prioridade
                for campo in ("data_alteracao", "tipo_variacao"):
                    if p.get(campo) not in (None, ""):
                        linha[campo] = p[campo]
            else:
                linha = dict(p)  # detalhe falhou: fica com os campos da etapa 1 (como antes)
            linhas.append(linha)

        df_final = mesclar_com_csv_existente(linhas, {})
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
