import asyncio
import aiohttp
import pandas as pd
import os
import sys
import time
import json

# ================= CONFIGURAÇÕES =================
TOKEN = os.environ.get("TINY_TOKEN", "")
API_URL = "https://api.tiny.com.br/api2/produto.obter.tags.php"
OUTPUT_DIR = "TB_bases"
# Corrigido: era "Tb_BaseProdutos_tiny.csv" (t minúsculo) mas o getObterProdutos.py
# salva como "Tb_BaseProdutos_Tiny.csv" (T maiúsculo). No Windows isso não dava erro
# porque o sistema de arquivos ignora maiúscula/minúscula - no Linux (GitHub Actions) dá.
INPUT_FILE = os.path.join(OUTPUT_DIR, "Tb_BaseProdutos_Tiny.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Tb_ProdutosTag_Tiny.csv")
ERROS_FILE = os.path.join(OUTPUT_DIR, "TagsProdutos_Tiny_Erros.csv")

LIMIT_PER_MINUTE = 100
MAX_CONCURRENT = 8
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 5


# ================= RATE LIMITER =================
class FastRateLimiter:
    def __init__(self, max_per_minute=100):
        self.max_per_minute = max_per_minute
        self.requests = []
        self.lock = asyncio.Lock()
        self.min_interval = 60 / max_per_minute
        self.last_request = 0

    async def acquire(self):
        while True:
            wait_time = 0.0
            async with self.lock:
                now = time.time()
                time_since_last = now - self.last_request
                if time_since_last < self.min_interval:
                    await asyncio.sleep(self.min_interval - time_since_last)
                    now = time.time()

                self.requests = [req_time for req_time in self.requests if now - req_time < 60]

                if len(self.requests) < self.max_per_minute:
                    self.requests.append(now)
                    self.last_request = now
                    return

                oldest_request = self.requests[0]
                wait_time = 61 - (now - oldest_request)
            # Sleep FORA do "async with self.lock" — dentro dele, a nova
            # tentativa (próxima volta do while) travaria pra sempre tentando
            # readquirir um lock que ela mesma ainda estaria segurando
            # (asyncio.Lock não é reentrante).
            print(f"\n⏸ Limite atingido, aguardando {wait_time:.1f}s...")
            await asyncio.sleep(wait_time)


# ================= FUNÇÃO DE BUSCA =================
async def get_tags_produto(session, produto_id, rate_limiter):
    for attempt in range(MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            payload = {"token": TOKEN, "id": produto_id, "formato": "json"}
            async with session.post(API_URL, data=payload) as response:
                text = await response.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(DELAY_BETWEEN_RETRIES)
                        continue
                    return None, f"Resposta não pôde ser decodificada: {text[:100]}..."

                if data.get("retorno", {}).get("status") == "OK":
                    produto = data["retorno"]["produto"]
                    tags = produto.get("tags", [])
                    if not tags:  # Ignorar produtos sem tags
                        return None, None
                    # Retornar uma lista de linhas, cada tag em uma linha
                    linhas = []
                    for t in tags:
                        linhas.append({
                            "ID Produto": produto["id"],
                            "Código": produto.get("codigo", ""),
                            "Nome": produto.get("nome", ""),
                            "Tag": t["tag"]["nome_tag"]
                        })
                    return linhas, None

                erro = data.get("retorno", {}).get("erros", [{"erro": "Erro desconhecido"}])[0]["erro"]
                if "bloque" in erro.lower() and attempt < MAX_RETRIES:
                    await asyncio.sleep(DELAY_BETWEEN_RETRIES)
                    continue
                return None, erro

        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2)
                continue
            return None, str(e)
    return None, "Falha após várias tentativas"


# ================= WORKER =================
async def worker(queue, session, rate_limiter, results, erros, progress):
    while not queue.empty():
        try:
            produto_id = queue.get_nowait()
            linhas, erro = await get_tags_produto(session, produto_id, rate_limiter)
            if linhas:  # Adiciona apenas se houver tags
                results.extend(linhas)
            if erro:
                erros.append({"id": produto_id, "erro": erro})
                print(f"\n❌ Erro no produto ID {produto_id}: {erro}")
            progress['processed'] += 1
            queue.task_done()
        except asyncio.QueueEmpty:
            break
        except Exception as e:
            print(f"\nErro no worker: {e}")
            break


# ================= FUNÇÃO PRINCIPAL =================
async def main():
    print("🚀 Iniciando coleta de tags de produtos...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ---------- Leitura robusta do CSV ----------
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8-sig') as f:
            first_line = f.readline()
            sep = ';' if ';' in first_line else ','

        df_ids = pd.read_csv(INPUT_FILE, sep=sep, encoding='utf-8-sig', dtype=str, on_bad_lines='skip')

        # Remove BOM e padroniza nomes de colunas
        df_ids.columns = (
            df_ids.columns.str.replace('\ufeff', '', regex=True)
            .str.strip()
            .str.lower()
        )

        # Força o nome da primeira coluna a ser 'id' se ela contiver isso
        if 'id' not in df_ids.columns:
            for col in df_ids.columns:
                if 'id' in col:
                    df_ids.rename(columns={col: 'id'}, inplace=True)
                    break

        if 'id' not in df_ids.columns:
            print(f"❌ Colunas encontradas: {df_ids.columns.tolist()}")
            raise KeyError("A coluna 'id' não foi encontrada no CSV.")

        df_ids['id'] = df_ids['id'].astype(str).str.replace(',', '').str.strip()
        lista_ids = df_ids['id'].tolist()
        total = len(lista_ids)
        print(f"📋 Total de produtos encontrados: {total}")

    except Exception as e:
        print(f"❌ Erro ao ler CSV: {e}")
        sys.exit(1)

    # ---------- Execução assíncrona ----------
    rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    results = []
    erros = []
    progress = {'processed': 0}
    queue = asyncio.Queue()
    for pid in lista_ids:
        await queue.put(pid)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(worker(queue, session, rate_limiter, results, erros, progress))
                   for _ in range(MAX_CONCURRENT)]
        start_time = time.time()
        last_processed = 0

        while progress['processed'] < total:
            current_processed = progress['processed']
            if current_processed != last_processed:
                elapsed = time.time() - start_time
                rate = current_processed / elapsed * 60 if elapsed > 0 else 0
                eta = (total - current_processed) / (rate / 60) if rate > 0 else 0
                percent = (current_processed / total) * 100
                print(f"\r📦 {current_processed}/{total} ({percent:.2f}%) | "
                      f"Rate: {rate:.1f} req/min | ETA: {eta/60:.1f}min", end='', flush=True)
                last_processed = current_processed
            await asyncio.sleep(1)

        await queue.join()
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)

    # ---------- Gravação dos resultados ----------
    print("\n📊 Criando CSV de tags...")
    if results:
        df = pd.DataFrame(results)
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig", sep=";")
        print(f"✅ CSV salvo: {OUTPUT_FILE}")
    else:
        print("⚠️ Nenhum produto com tags foi encontrado.")

    if erros:
        df_erros = pd.DataFrame(erros)
        df_erros.to_csv(ERROS_FILE, index=False, encoding="utf-8-sig", sep=";")
        print(f"⚠️ Erros registrados em: {ERROS_FILE}")

    elapsed = time.time() - start_time
    avg_rate = len(results) / elapsed * 60 if elapsed > 0 else 0
    print(f"⏱ Tempo total: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"⚡ Taxa média: {avg_rate:.1f} requisições/minuto")


# ================= EXECUÇÃO =================
if __name__ == "__main__":
    asyncio.run(main())
