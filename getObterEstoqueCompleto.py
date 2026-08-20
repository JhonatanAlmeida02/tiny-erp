"""
getObterEstoqueCompleto.py
-----------------------------
Carga COMPLETA e única do estoque de TODOS os produtos, usando o endpoint
produto.obter.estoque.php (1 chamada por produto), em vez do endpoint de
"atualizações" (lista.atualizacoes.estoque) que só enxerga mudanças dos
últimos ~29 dias e por isso NUNCA consegue trazer o catálogo inteiro sozinho.

Rode esse script só quando precisar reconstruir o CSV de estoque do zero
(ex: primeira vez, ou pra reconciliar depois de um tempo). No dia a dia,
o getObterEstoque.py (incremental, de hora em hora) já dá conta de manter
tudo atualizado sozinho a partir daqui.

Fonte da lista de produtos: TB_bases/Tb_BaseProdutos_Tiny.csv (já sincronizado
pelo getObterProdutos.py) — assim não duplicamos a paginação de IDs.

Ao final, atualiza o checkpoint de "estoque" pra AGORA, pra que a próxima
sincronização incremental continue exatamente de onde essa carga completa
parou (sem buraco, sem duplicar trabalho).
"""

import asyncio
import json
import os
import time
import unicodedata

import aiohttp
import pandas as pd

from checkpoint_utils import set_last_run

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")
API_URL = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
PASTA_DESTINO = "TB_bases"
INPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseProdutos_Tiny.csv")
OUTPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseEstoque_Tiny.csv")
JOB_NAME = "estoque"

LIMIT_PER_MINUTE = 95  # um pouco de folga em relação ao limite real de 100/min
MAX_CONCURRENT = 8
MAX_RETRIES = 3

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


# ================= RATE LIMITER (mesmo padrão usado no resto do projeto) =================
class RateLimiter:
    def __init__(self, max_per_minute=LIMIT_PER_MINUTE):
        self.max_per_minute = max_per_minute
        self.requests = []
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            self.requests = [t for t in self.requests if now - t < 60]
            if len(self.requests) >= self.max_per_minute:
                wait = 61 - (now - self.requests[0])
                print(f"⏸ Limite atingido, aguardando {wait:.1f}s...")
                await asyncio.sleep(wait)
                return await self.acquire()
            self.requests.append(now)


# ================= BUSCA DE ESTOQUE POR PRODUTO =================
async def obter_estoque_produto(session, produto_id, rate_limiter):
    for tentativa in range(MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            payload = {"token": TOKEN_API, "id": produto_id, "formato": "json"}
            async with session.post(API_URL, data=payload) as response:
                text = await response.text()
                if "API Bloqueada" in text:
                    print("⚠️ API bloqueada - aguardando 30s...")
                    await asyncio.sleep(30)
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    if tentativa < MAX_RETRIES:
                        await asyncio.sleep(5)
                        continue
                    return None

                retorno = data.get("retorno", {})
                if retorno.get("status") == "OK":
                    return retorno.get("produto")

                erro = retorno.get("erros", [{"erro": "desconhecido"}])[0].get("erro", "")
                if "bloque" in erro.lower() and tentativa < MAX_RETRIES:
                    await asyncio.sleep(10)
                    continue
                return None
        except Exception:
            if tentativa < MAX_RETRIES:
                await asyncio.sleep(5)
                continue
            return None
    return None


# ================= PROCESSAMENTO DE CADA PRODUTO (mesmo formato do CSV atual) =================
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


# ================= WORKER =================
async def worker(fila, session, rate_limiter, resultados, progresso, erros):
    while not fila.empty():
        try:
            produto_id = fila.get_nowait()
        except asyncio.QueueEmpty:
            break
        produto = await obter_estoque_produto(session, produto_id, rate_limiter)
        if produto:
            resultados.append(processar_produto(produto))
        else:
            erros.append(produto_id)
        progresso["feito"] += 1
        if progresso["feito"] % 100 == 0 or progresso["feito"] == progresso["total"]:
            print(f"[estoque completo] {progresso['feito']}/{progresso['total']} produtos processados")
        fila.task_done()


# ================= FUNÇÃO PRINCIPAL =================
async def main_async():
    if not TOKEN_API:
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        return

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Arquivo {INPUT_FILE} não encontrado. Rode getObterProdutos.py primeiro "
              f"(precisamos da lista de IDs de produtos).")
        return

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    df_produtos = pd.read_csv(INPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
    ids = df_produtos["id"].dropna().astype(str).tolist()
    total = len(ids)
    print(f"🚀 Carga completa de estoque: {total} produtos a processar (1 chamada por produto).")

    fila = asyncio.Queue()
    for pid in ids:
        await fila.put(pid)

    resultados = []
    erros = []
    progresso = {"feito": 0, "total": total}
    rate_limiter = RateLimiter()

    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    inicio = time.time()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(worker(fila, session, rate_limiter, resultados, progresso, erros))
                   for _ in range(MAX_CONCURRENT)]
        await fila.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    colunas_finais = ["ID Produto", "Código", "Nome", "Unidade", "Saldo Total", "Saldo Reservado"] + ordem_depositos
    df_final = pd.DataFrame(resultados).reindex(columns=colunas_finais, fill_value="")
    df_final.to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")

    # Marca o checkpoint como "agora" — a partir daqui, o getObterEstoque.py
    # (incremental, de hora em hora) continua sozinho de onde essa carga parou.
    set_last_run(JOB_NAME)

    elapsed = time.time() - inicio
    print(f"\n✅ Carga completa de estoque concluída! {len(resultados)}/{total} produtos salvos.")
    if erros:
        print(f"⚠️ {len(erros)} produtos deram erro e não vieram (IDs: {erros[:10]}{'...' if len(erros) > 10 else ''})")
    print(f"📄 CSV salvo: {OUTPUT_FILE}")
    print(f"⏱ Tempo total: {elapsed:.1f}s ({elapsed/60:.1f} min)")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
