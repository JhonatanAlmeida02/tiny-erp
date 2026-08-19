import asyncio
import aiohttp
import pandas as pd
import requests
import os
import sys
import time
import json
from datetime import datetime

# ================= CONFIGURAÇÕES =================
TOKEN = os.environ.get("TINY_TOKEN", "")
URL_GRUPOS = "https://api.tiny.com.br/api2/grupo.tag.pesquisa.php"
URL_TAGS = "https://api.tiny.com.br/api2/tag.pesquisa.php"
OUTPUT_DIR = "TB_bases"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Tb_GrupoTag_Tiny.csv")

# Configurações baseadas no seu plano (100 req/min)
LIMIT_PER_MINUTE = 100
MAX_CONCURRENT = 25
MAX_RETRIES = 2

# ================= RATE LIMITER =================
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
                sleep_time = self.min_interval - time_since_last
                await asyncio.sleep(sleep_time)
                now = time.time()
            self.requests = [req_time for req_time in self.requests if now - req_time < 60]
            if len(self.requests) >= self.max_per_minute:
                oldest_request = self.requests[0]
                wait_time = 61 - (now - oldest_request)
                print(f"\n⏸ Limite atingido, aguardando {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)
                return await self.acquire()
            self.requests.append(now)
            self.last_request = now

# ================= FUNÇÕES DE BUSCA ASYNC =================
async def get_group_tags(session, grupo_id, grupo_nome, rate_limiter):
    tags = []
    pagina = 1
    while True:
        for attempt in range(MAX_RETRIES + 1):
            try:
                await rate_limiter.acquire()
                params = {
                    "token": TOKEN, "formato": "json", 
                    "idGrupo": grupo_id, "pagina": pagina
                }
                async with session.get(URL_TAGS, params=params) as response:
                    text = await response.text()  # sempre lê como texto
                    try:
                        data = json.loads(text)  # tenta decodificar manualmente
                    except json.JSONDecodeError:
                        if attempt < MAX_RETRIES:
                            print(f"\n⚠️ Grupo '{grupo_nome}' retornou resposta inválida, tentando novamente...")
                            await asyncio.sleep(2)
                            continue
                        return grupo_id, [], f"Resposta não pôde ser decodificada: {text[:100]}..."

                    if data.get("retorno", {}).get("status") == "OK":
                        registros = data.get("retorno", {}).get("registros", [])
                        for reg in registros:
                            t = reg.get("registro", {})
                            tags.append({
                                "id": t.get("id"),
                                "nome": t.get("nome"),
                                "id_grupo": t.get("id_grupo"),
                                "grupo": t.get("grupo")
                            })
                        
                        if pagina >= int(data.get("retorno", {}).get("numero_paginas", 1)):
                            return grupo_id, tags, None
                        pagina += 1
                    else:
                        erro = data.get("retorno", {}).get("erros", [{"erro": "Erro desconhecido"}])[0]["erro"]
                        if "limite" in erro.lower() and attempt < MAX_RETRIES:
                            await asyncio.sleep(2)
                            continue
                        return grupo_id, [], erro
            except Exception as e:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(1)
                    continue
                return grupo_id, [], str(e)

# ================= WORKER OTIMIZADO =================
async def worker(queue, session, rate_limiter, results, progress, erros):
    while not queue.empty():
        try:
            grupo = queue.get_nowait()
            grupo_id = grupo["id"]
            grupo_nome = grupo["nome"]
            _, tags, erro = await get_group_tags(session, grupo_id, grupo_nome, rate_limiter)
            results.extend(tags)
            if erro:
                erros.append({"id_grupo": grupo_id, "grupo": grupo_nome, "erro": erro})
                print(f"\n❌ Erro no grupo '{grupo_nome}': {erro}")
            progress['processed'] += 1
            queue.task_done()
        except asyncio.QueueEmpty:
            break
        except Exception as e:
            print(f"\nErro no worker: {e}")
            break

# ================= FUNÇÃO PRINCIPAL =================
async def main():
    print("🚀 Iniciando coleta de tags...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. BUSCAR GRUPOS (Síncrono)
    print("🔍 Buscando grupos de tags...")
    grupos = []
    pagina = 1
    while True:
        params = {"token": TOKEN, "formato": "json", "pesquisa": "", "pagina": pagina}
        r = requests.get(URL_GRUPOS, params=params)
        data = r.json().get("retorno", {})
        if data.get("status") != "OK":
            print("❌ Erro ao buscar grupos:", data.get("erros", "desconhecido"))
            sys.exit(1)
        registros = data.get("registros", [])
        for reg in registros:
            g = reg.get("registro", {})
            grupos.append({"id": g.get("id"), "nome": g.get("nome")})
        if pagina >= int(data.get("numero_paginas", 1)):
            break
        pagina += 1
    print(f"✅ {len(grupos)} grupos encontrados.")

    # 2. BUSCAR TAGS (Assíncrono)
    total = len(grupos)
    queue = asyncio.Queue()
    for g in grupos:
        await queue.put(g)
    
    results = []
    erros = []
    progress = {'processed': 0}
    rate_limiter = FastRateLimiter(LIMIT_PER_MINUTE)
    start_time = time.time()
    
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(limit=30, limit_per_host=30, ttl_dns_cache=300, use_dns_cache=True)
    
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [
            asyncio.create_task(worker(queue, session, rate_limiter, results, progress, erros))
            for _ in range(MAX_CONCURRENT)
        ]
        
        last_processed = 0
        while progress['processed'] < total:
            current_processed = progress['processed']
            if current_processed != last_processed:
                elapsed = time.time() - start_time
                if elapsed > 0:
                    real_rate = current_processed / elapsed * 60
                    eta = (total - current_processed) / (real_rate / 60) if real_rate > 0 else 0
                    percent = (current_processed / total) * 100
                    print(f"\r📦 {current_processed}/{total} ({percent:.1f}%) | "
                          f"Rate: {real_rate:.1f} req/min | ETA: {eta/60:.1f}min", end='', flush=True)
                last_processed = current_processed
            await asyncio.sleep(1)
        
        await queue.join()
        for worker_task in workers:
            worker_task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
    
    # 3. EXPORTAR PARA CSV
    print("\n📊 Processando dados e gerando CSV...")
    df_tags = pd.DataFrame(results)
    df_tags.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig", sep=";")
    
    if erros:
        df_erros = pd.DataFrame(erros)
        df_erros.to_csv(os.path.join(OUTPUT_DIR, "Tb_grupoTags_Erros.csv"), index=False, encoding="utf-8-sig", sep=";")
        print(f"⚠️ Erros registrados em: {os.path.join(OUTPUT_DIR, 'Tb_grupoTags_Erros.csv')}")

    end_time = time.time()
    elapsed = end_time - start_time
    avg_rate = len(results) / elapsed * 60 if elapsed > 0 else 0
    
    print(f"\n✅ Coleta de tags concluída!")
    print(f"📄 CSV salvo: {OUTPUT_FILE}")
    print(f"⏱ Tempo total: {elapsed:.1f}s ({elapsed/60:.1f}min)")
    print(f"⚡ Taxa média: {avg_rate:.1f} requisições/minuto")

# ================= EXECUÇÃO =================
if __name__ == "__main__":
    asyncio.run(main())
