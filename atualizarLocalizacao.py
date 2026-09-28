"""
atualizarLocalizacao.py
------------------------
Atualiza o campo "localizacao" (localização física no estoque) de produtos
no Tiny, em lote, a partir de um CSV de entrada. Mesmo padrão de estrutura
do getObterEstoque.py (token via env, rate limiter assíncrono, retry).

CSV de entrada esperado (separador ";", encoding utf-8-sig):

    ID Produto;Localizacao
    123456;Corredor 3, Prateleira B
    789012;Depósito 2, Nível A

COMO FUNCIONA (2 tentativas por produto):
  1) Tenta alterar SÓ com id + localizacao (produto.alterar.php). Em muitas
     contas isso já basta, mesmo a doc oficial listando nome/unidade/preco/
     origem/situacao/tipo como "obrigatório" nessa tabela.
  2) Se a API reclamar de campo obrigatório faltando, busca os dados atuais
     do produto (produto.obter.php) e tenta de novo, reenviando esses
     campos originais + a nova localizacao — assim não corre risco de
     zerar/sobrescrever nada sem querer.

Cada produto processado gera uma linha no CSV de log de saída (OK/Erro +
mensagem da API), pra você conferir o que passou e o que precisa de
atenção manual.

Configuração:
  - Defina a variável de ambiente TINY_TOKEN antes de rodar.
  - Ajuste ARQUIVO_ENTRADA se seu CSV tiver outro nome/local.

Dica antes de rodar em lote: teste com um CSV de 1-2 linhas primeiro, pra
confirmar que sua conta aceita a alteração mínima (passo 1) ou se sempre
cai no fallback (passo 2) — ambos funcionam, mas é bom saber qual caminho
sua conta usa antes de rodar em produtos de verdade.
"""

import argparse
import asyncio
import json as _json
import os
import sys
import time

import aiohttp
import pandas as pd

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")

URL_ALTERAR = "https://api.tiny.com.br/api2/produto.alterar.php"
URL_OBTER = "https://api.tiny.com.br/api2/produto.obter.php"

PASTA_BASE = "TB_bases"
ARQUIVO_ENTRADA = os.path.join(PASTA_BASE, "localizacoes_novas.csv")
ARQUIVO_LOG = os.path.join(PASTA_BASE, "log_atualizacao_localizacao.csv")

REQUEST_TIMEOUT = 30
LIMIT_PER_MINUTE = 100        # mesmo limite de conta usado nos outros scripts
MAX_CONCURRENT = 8
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 5

# Campos que a doc oficial marca como "obrigatório" na tabela de alterar
# produto. Só são reenviados se a tentativa mínima (passo 1) for recusada
# pedindo algum deles.
CAMPOS_FALLBACK = ["nome", "unidade", "preco", "origem", "situacao", "tipo", "codigo"]

# ================= RATE LIMITER (mesmo padrão do getObterEstoque.py) =====
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


# ================= CHAMADA HTTP GENÉRICA COM RETRY =================
async def _chamar_api(session, method: str, url: str, params: dict, rate_limiter):
    """GET ou POST com retry, tratando 429 e bloqueio de API (mesmo padrão
    de fetch_json do getObterEstoque.py, adaptado para aiohttp async)."""
    for tentativa in range(1, MAX_RETRIES + 2):
        try:
            await rate_limiter.acquire()
            requisicao = session.get(url, params=params) if method == "GET" \
                else session.post(url, data=params)

            async with requisicao as response:
                if response.status == 429:
                    print("⚠️ 429 Too Many Requests - aguardando 60s...")
                    await asyncio.sleep(60)
                    continue
                text = await response.text()
                if "API Bloqueada" in text:
                    print("⚠️ API bloqueada - aguardando 60s...")
                    await asyncio.sleep(60)
                    continue
                try:
                    return _json.loads(text)
                except Exception:
                    raise ValueError(f"Resposta inválida: {text[:150]}...")
        except Exception as e:
            if tentativa <= MAX_RETRIES:
                print(f"Tentativa {tentativa}/{MAX_RETRIES} falhou ({e}). "
                      f"Aguardando {DELAY_BETWEEN_RETRIES}s...")
                await asyncio.sleep(DELAY_BETWEEN_RETRIES)
                continue
            return None
    return None


async def obter_produto(session, produto_id: str, rate_limiter):
    """Busca os dados atuais do produto (produto.obter.php) — usado só como
    fallback quando a alteração mínima é recusada por falta de campo."""
    data = await _chamar_api(session, "GET", URL_OBTER, {
        "token": TOKEN_API, "id": produto_id, "formato": "json",
    }, rate_limiter)

    if not data:
        return None, "Falha de conexão ao buscar dados do produto"

    retorno = data.get("retorno", {})
    if retorno.get("status") != "OK":
        erros = retorno.get("erros", [{"erro": "erro desconhecido"}])
        return None, "; ".join(e.get("erro", "") for e in erros)

    return retorno.get("produto", {}), None


def _extrair_mensagem_erro(retorno: dict) -> str:
    erros = retorno.get("erros")
    if erros:
        return "; ".join(e.get("erro", "") for e in erros)
    for registro in retorno.get("registros", []):
        reg = registro.get("registro", {})
        if reg.get("status") == "Erro":
            for e in reg.get("erros", []):
                return e.get("erro", "erro desconhecido")
    return "erro desconhecido"


def _parece_erro_campo_obrigatorio(msg: str) -> bool:
    msg = (msg or "").lower()
    gatilhos = ["obrigat", "nome", "unidade", "preço", "preco", "origem",
                "situação", "situacao", "tipo"]
    return any(g in msg for g in gatilhos)


async def tentar_alterar(session, payload_produto: dict, rate_limiter):
    """Envia um produto para produto.alterar.php. Retorna (ok, mensagem)."""
    corpo = _json.dumps({"produtos": [{"produto": payload_produto}]})
    data = await _chamar_api(session, "POST", URL_ALTERAR, {
        "token": TOKEN_API, "produto": corpo, "formato": "json",
    }, rate_limiter)

    if not data:
        return False, "Falha de conexão"

    retorno = data.get("retorno", {})
    if retorno.get("status") == "OK":
        return True, None

    return False, _extrair_mensagem_erro(retorno)


# ================= PROCESSAMENTO DE UM PRODUTO =================
async def processar_um(session, produto_id: str, nova_localizacao: str, rate_limiter):
    # Passo 1: tentativa mínima
    ok, msg = await tentar_alterar(session, {
        "sequencia": "1",
        "id": produto_id,
        "localizacao": nova_localizacao,
    }, rate_limiter)

    if ok:
        return produto_id, "OK", "Alterado só com id + localizacao"

    if not _parece_erro_campo_obrigatorio(msg):
        # Erro que não parece ser de campo faltando (ex: produto não
        # encontrado) — reenviar com mais campos não vai resolver.
        return produto_id, "Erro", msg

    # Passo 2: busca os dados atuais e reenvia com os campos obrigatórios preenchidos
    produto_atual, erro_obter = await obter_produto(session, produto_id, rate_limiter)
    if not produto_atual:
        return produto_id, "Erro", (f"Tentativa mínima falhou ({msg}) e não foi "
                                     f"possível buscar dados completos: {erro_obter}")

    # Diagnóstico: mostra no log exatamente o que veio de produto.obter.php
    # pros campos de fallback, pra facilitar identificar campos ausentes/vazios.
    diagnostico = ", ".join(
        f"{c}={produto_atual.get(c, '<ausente>')!r}" for c in CAMPOS_FALLBACK
    )
    print(f"ℹ️ [{produto_id}] Campos obtidos via produto.obter.php: {diagnostico}")

    payload = {"sequencia": "1", "id": produto_id, "localizacao": nova_localizacao}
    for campo in CAMPOS_FALLBACK:
        if campo in produto_atual and produto_atual[campo] not in (None, ""):
            payload[campo] = produto_atual[campo]

    ok2, msg2 = await tentar_alterar(session, payload, rate_limiter)
    if ok2:
        return produto_id, "OK", "Alterado reenviando campos obrigatórios originais"
    return produto_id, "Erro", msg2


# ================= WORKER / FILA =================
async def _worker(queue, session, rate_limiter, resultados, progresso):
    while not queue.empty():
        try:
            produto_id, nova_localizacao = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        pid, status, msg = await processar_um(session, produto_id, nova_localizacao, rate_limiter)
        resultados.append({"ID Produto": pid, "Status": status, "Mensagem": msg})

        emoji = "✅" if status == "OK" else "❌"
        print(f"{emoji} {pid}: {msg}")

        progresso["processed"] += 1
        if progresso["processed"] % 20 == 0 or progresso["processed"] == progresso["total"]:
            print(f"[Progresso] {progresso['processed']}/{progresso['total']}")

        queue.task_done()


async def atualizar_localizacoes_em_lote(itens: list) -> list:
    """itens: lista de tuplas (id, nova_localizacao)."""
    rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=10)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    queue = asyncio.Queue()
    for item in itens:
        await queue.put(item)

    resultados = []
    progresso = {"processed": 0, "total": len(itens)}

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(_worker(queue, session, rate_limiter, resultados, progresso))
                   for _ in range(MAX_CONCURRENT)]
        await asyncio.gather(*workers)

    return resultados


# ================= LEITURA DO CSV DE ENTRADA =================
def ler_itens_do_csv(caminho: str) -> list:
    if not os.path.exists(caminho):
        raise FileNotFoundError(
            f"Arquivo de entrada não encontrado: {caminho}\n"
            f"Crie um CSV com colunas 'ID Produto;Localizacao' (separador ';')."
        )

    df = pd.read_csv(caminho, sep=";", dtype=str, encoding="utf-8-sig")
    df = df.dropna(subset=["ID Produto", "Localizacao"])

    itens = [(str(row["ID Produto"]).strip(), str(row["Localizacao"]).strip())
             for _, row in df.iterrows()]

    if not itens:
        raise RuntimeError("CSV de entrada não tem nenhuma linha válida (ID Produto + Localizacao).")

    return itens


def parse_args():
    parser = argparse.ArgumentParser(
        description="Atualiza o campo localizacao de um ou mais produtos no Tiny."
    )
    parser.add_argument("--id", help="ID de um único produto (modo individual — usado pelo workflow_dispatch do GitHub Actions). Se informado junto com --localizacao, ignora o CSV.")
    parser.add_argument("--localizacao", help="Nova localização a aplicar no produto de --id.")
    return parser.parse_args()


async def atualizar_um_item(produto_id: str, nova_localizacao: str):
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT, connect=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
        return await processar_um(session, produto_id, nova_localizacao, rate_limiter)


# ================= FUNÇÃO PRINCIPAL =================
def main():
    if TOKEN_API in ("", "COLOQUE_SEU_TOKEN_AQUI"):
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        sys.exit(1)

    args = parse_args()

    # ---- Modo individual: --id + --localizacao (chamado pelo formulário do Actions) ----
    if args.id and args.localizacao:
        print(f"📦 Atualizando 1 produto (modo individual): {args.id}")
        inicio = time.time()
        pid, status, msg = asyncio.run(atualizar_um_item(args.id, args.localizacao))
        elapsed = time.time() - inicio

        emoji = "✅" if status == "OK" else "❌"
        print(f"{emoji} {pid}: {msg}")
        print(f"⏱ Tempo total: {elapsed:.1f}s")

        if status != "OK":
            sys.exit(1)  # marca o job do Actions como falho quando a API recusa
        return

    # ---- Modo em lote: CSV (comportamento original, inalterado) ----
    itens = ler_itens_do_csv(ARQUIVO_ENTRADA)
    print(f"📦 {len(itens)} produto(s) para atualizar localização.")

    inicio = time.time()
    resultados = asyncio.run(atualizar_localizacoes_em_lote(itens))

    df_log = pd.DataFrame(resultados)
    os.makedirs(PASTA_BASE, exist_ok=True)
    df_log.to_csv(ARQUIVO_LOG, index=False, sep=";", encoding="utf-8-sig")

    total_ok = int((df_log["Status"] == "OK").sum())
    total_erro = int((df_log["Status"] == "Erro").sum())
    elapsed = time.time() - inicio

    print(f"\n✅ {total_ok} produto(s) atualizados com sucesso.")
    if total_erro:
        print(f"❌ {total_erro} produto(s) com erro — confira {ARQUIVO_LOG}.")
    print(f"📄 Log salvo: {ARQUIVO_LOG}")
    print(f"⏱ Tempo total: {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nExecução interrompida manualmente.")
        sys.exit(130)
    except Exception as e:
        print(f"Erro inesperado: {e}")
        sys.exit(1)
