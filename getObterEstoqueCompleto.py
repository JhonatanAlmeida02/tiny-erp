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

CONTROLE DE VAZÃO: a API da Tiny bloqueia o token por RAJADA, não só por
volume. Por isso o RateLimiter aqui espaça as chamadas em 60/LIMIT segundos
uma da outra, além de respeitar o teto por minuto. Quando mesmo assim vem
"API Bloqueada", a pausa é GLOBAL (todos os workers param juntos), com
backoff progressivo — senão os workers acordam em bando e renovam o bloqueio.
"""

import asyncio
import json
import os
import sys
import time
import unicodedata

import aiohttp
import pandas as pd

from checkpoint_utils import set_last_run

# Força a saída a aparecer ao vivo no log do GitHub Actions (sem isso, o
# Python "segura" a saída em buffer até acumular bastante texto, e como
# esse script só imprime de vez em quando, o log parece "travado" mesmo
# estando funcionando normalmente em segundo plano).
sys.stdout.reconfigure(line_buffering=True)

# ================= CONFIGURAÇÕES =================
TOKEN_API = os.environ.get("TINY_TOKEN", "")
API_URL = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
PASTA_DESTINO = "TB_bases"
INPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseProdutos_Tiny.csv")
OUTPUT_FILE = os.path.join(PASTA_DESTINO, "Tb_BaseEstoque_Tiny.csv")
JOB_NAME = "estoque"

LIMIT_PER_MINUTE = 90   # folga em relação ao limite real de 100/min
MAX_CONCURRENT = 4      # com espaçamento de ~0,67s, mais que isso não acelera nada
MAX_RETRIES = 4

# Pausa global quando a API responde "API Bloqueada" (cresce a cada bloqueio).
PAUSA_BLOQUEIO_INICIAL = 60
PAUSA_BLOQUEIO_MAX = 300

# Acima disso, considera a carga falha e NÃO grava o CSV nem o checkpoint,
# pra não substituir a base boa por uma base capenga.
MAX_ERROS_TOLERADOS_PCT = 5.0

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

COLUNAS_BASE = ["ID Produto", "Código", "Nome", "Unidade", "Saldo Total", "Saldo Reservado"]
COLUNAS_FINAIS = COLUNAS_BASE + ordem_depositos


def normalizar_nome(nome) -> str:
    """Remove acentos, caixa e espaços das pontas. Aceita None sem estourar."""
    if not nome:
        return ""
    return (unicodedata.normalize("NFKD", str(nome))
            .encode("ASCII", "ignore")
            .decode("utf-8")
            .lower()
            .strip())


# ================= RATE LIMITER =================
class RateLimiter:
    """Reserva um horário para cada requisição, garantindo DUAS coisas:

    1. Espaçamento mínimo entre chamadas consecutivas (60/max_per_minute).
       Sem isso os workers disparam a janela inteira em rajada e a Tiny
       bloqueia o token — foi exatamente o que aconteceu antes.
    2. Teto por janela de 60s.

    A reserva é feita dentro do lock e o sleep acontece fora dele, então os
    workers não acordam todos no mesmo instante disputando o mesmo slot.
    """

    def __init__(self, max_per_minute=LIMIT_PER_MINUTE):
        self.max_per_minute = max_per_minute
        self.min_interval = 60.0 / max_per_minute
        self.requests = []
        self.last_request = 0.0
        self.blocked_until = 0.0
        self.pausa_atual = PAUSA_BLOQUEIO_INICIAL
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            agora = time.time()
            alvo = max(agora, self.blocked_until, self.last_request + self.min_interval)

            self.requests = [t for t in self.requests if alvo - t < 60]
            if len(self.requests) >= self.max_per_minute:
                alvo = max(alvo, self.requests[0] + 61)
                self.requests = [t for t in self.requests if alvo - t < 60]

            self.requests.append(alvo)
            self.last_request = alvo
            espera = alvo - agora

        if espera > 0:
            await asyncio.sleep(espera)

    async def sinalizar_bloqueio(self) -> float:
        """Pausa TODOS os workers de uma vez. Retorna quantos segundos."""
        async with self.lock:
            agora = time.time()
            if agora < self.blocked_until:
                # Já tem pausa em curso: outro worker avisou primeiro.
                return self.blocked_until - agora
            pausa = self.pausa_atual
            self.blocked_until = agora + pausa
            self.pausa_atual = min(self.pausa_atual * 2, PAUSA_BLOQUEIO_MAX)
            print(f"⚠️ API bloqueada - pausando TODOS os workers por {pausa:.0f}s...")
            return pausa


# ================= BUSCA DE ESTOQUE POR PRODUTO =================
async def obter_estoque_produto(session, produto_id, rate_limiter):
    for tentativa in range(MAX_RETRIES + 1):
        try:
            await rate_limiter.acquire()
            payload = {"token": TOKEN_API, "id": produto_id, "formato": "json"}
            async with session.post(API_URL, data=payload) as response:
                text = await response.text()

                if response.status == 429 or "API Bloqueada" in text:
                    pausa = await rate_limiter.sinalizar_bloqueio()
                    await asyncio.sleep(pausa)
                    continue

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    if tentativa < MAX_RETRIES:
                        await asyncio.sleep(5)
                        continue
                    return None, f"Resposta inválida: {text[:80]}"

                retorno = data.get("retorno", {})
                if retorno.get("status") == "OK":
                    produto = retorno.get("produto")
                    if produto is not None:
                        produto.setdefault("id", produto_id)
                    return produto, None

                erro = retorno.get("erros", [{"erro": "desconhecido"}])[0].get("erro", "")
                if "bloque" in erro.lower():
                    pausa = await rate_limiter.sinalizar_bloqueio()
                    await asyncio.sleep(pausa)
                    continue
                return None, erro
        except Exception as e:
            if tentativa < MAX_RETRIES:
                await asyncio.sleep(5)
                continue
            return None, str(e)
    return None, "Falha após várias tentativas (bloqueio persistente)"


# ================= PROCESSAMENTO DE CADA PRODUTO =================
def processar_produto(produto: dict, meta: dict = None) -> dict:
    """`meta` traz código/nome/unidade do CSV de produtos, usados quando o
    endpoint de estoque não devolve esses campos."""
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
        nome_norm = normalizar_nome(deposito_info.get("nome", ""))
        if nome_norm in mapa_depositos:
            depositos_dict[mapa_depositos[nome_norm]] = deposito_info.get("saldo", "")

    linha.update(depositos_dict)
    return linha


# ================= MERGE COM CSV EXISTENTE (UPSERT) =================
def mesclar_com_csv_existente(linhas_novas: list) -> pd.DataFrame:
    """Upsert por ID. Produtos que falharam nesta carga MANTÊM a linha antiga
    em vez de sumirem da base."""
    df_novo = pd.DataFrame(linhas_novas).reindex(columns=COLUNAS_FINAIS, fill_value="")
    df_novo["ID Produto"] = df_novo["ID Produto"].astype(str)

    if os.path.exists(OUTPUT_FILE):
        df_existente = pd.read_csv(OUTPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
        df_existente = df_existente.reindex(columns=COLUNAS_FINAIS, fill_value="")
        df_existente["ID Produto"] = df_existente["ID Produto"].astype(str)
        df_existente = df_existente[~df_existente["ID Produto"].isin(df_novo["ID Produto"])]
        df_final = pd.concat([df_existente, df_novo], ignore_index=True)
    else:
        df_final = df_novo

    return df_final.reindex(columns=COLUNAS_FINAIS, fill_value="")


# ================= WORKER =================
async def worker(fila, session, rate_limiter, resultados, progresso, erros, catalogo):
    while True:
        try:
            produto_id = fila.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            produto, erro = await obter_estoque_produto(session, produto_id, rate_limiter)
            if produto:
                resultados.append(processar_produto(produto, catalogo.get(str(produto_id), {})))
            else:
                erros.append({"id": produto_id, "erro": erro})

            progresso["feito"] += 1
            if progresso["feito"] % 25 == 0 or progresso["feito"] == progresso["total"]:
                decorrido = time.time() - progresso["inicio"]
                ritmo = progresso["feito"] / decorrido * 60 if decorrido else 0
                restante = (progresso["total"] - progresso["feito"]) / ritmo if ritmo else 0
                print(f"[estoque completo] {progresso['feito']}/{progresso['total']} produtos "
                      f"({ritmo:.0f}/min, ~{restante:.0f} min restantes, {len(erros)} erros)")
        finally:
            fila.task_done()


# ================= FUNÇÃO PRINCIPAL =================
async def main_async():
    if not TOKEN_API:
        print("⚠️ Configure o token (variável de ambiente TINY_TOKEN) antes de rodar.")
        sys.exit(1)

    if not os.path.exists(INPUT_FILE):
        print(f"❌ Arquivo {INPUT_FILE} não encontrado. Rode getObterProdutos.py primeiro "
              f"(precisamos da lista de IDs de produtos).")
        sys.exit(1)

    os.makedirs(PASTA_DESTINO, exist_ok=True)

    df_produtos = pd.read_csv(INPUT_FILE, sep=";", dtype=str, encoding="utf-8-sig")
    if "id" not in df_produtos.columns:
        print(f"❌ Coluna 'id' não encontrada em {INPUT_FILE}. "
              f"Colunas disponíveis: {list(df_produtos.columns)}")
        sys.exit(1)

    # Guarda código/nome/unidade: produto.obter.estoque.php não devolve esses campos.
    catalogo = {}
    for _, row in df_produtos.iterrows():
        pid = str(row.get("id", "") or "").strip()
        if pid and pid.lower() != "nan":
            catalogo[pid] = {
                "codigo": row.get("codigo", "") or "",
                "nome": row.get("nome", "") or "",
                "unidade": row.get("unidade", "") or "",
            }

    ids = list(catalogo.keys())
    total = len(ids)
    estimativa = total / LIMIT_PER_MINUTE
    print(f"🚀 Carga completa de estoque: {total} produtos a processar "
          f"(1 chamada por produto, ~{estimativa:.0f} min a {LIMIT_PER_MINUTE}/min).")

    fila = asyncio.Queue()
    for pid in ids:
        await fila.put(pid)

    resultados = []
    erros = []
    inicio = time.time()
    progresso = {"feito": 0, "total": total, "inicio": inicio}
    rate_limiter = RateLimiter()

    timeout = aiohttp.ClientTimeout(total=60, connect=15)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, limit_per_host=MAX_CONCURRENT)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        workers = [asyncio.create_task(
            worker(fila, session, rate_limiter, resultados, progresso, erros, catalogo))
            for _ in range(MAX_CONCURRENT)]
        await asyncio.gather(*workers)

    elapsed = time.time() - inicio
    pct_erro = (len(erros) / total * 100) if total else 0

    if pct_erro > MAX_ERROS_TOLERADOS_PCT:
        print(f"\n❌ Carga abortada: {len(erros)}/{total} produtos falharam ({pct_erro:.1f}%), "
              f"acima do limite de {MAX_ERROS_TOLERADOS_PCT}%.")
        print("   O CSV e o checkpoint NÃO foram alterados — a base antiga foi preservada.")
        for e in erros[:10]:
            print(f"   - ID {e['id']}: {e['erro']}")
        sys.exit(1)

    df_final = mesclar_com_csv_existente(resultados)
    df_final.to_csv(OUTPUT_FILE, index=False, sep=";", encoding="utf-8-sig")

    # Só marca o checkpoint DEPOIS que o CSV foi salvo com sucesso — a partir
    # daqui o getObterEstoque.py (incremental) continua de onde essa carga parou.
    set_last_run(JOB_NAME)

    print(f"\n✅ Carga completa concluída! {len(resultados)}/{total} produtos coletados.")
    if erros:
        print(f"⚠️ {len(erros)} produtos deram erro (linhas antigas preservadas): "
              f"{[e['id'] for e in erros[:10]]}{'...' if len(erros) > 10 else ''}")
    print(f"📄 CSV salvo: {OUTPUT_FILE} (total de {len(df_final)} produtos na base)")
    print(f"⏱ Tempo total: {elapsed:.1f}s ({elapsed / 60:.1f} min)")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
