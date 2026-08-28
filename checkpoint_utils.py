"""
checkpoint_utils.py
--------------------
Controla a "última execução com sucesso" de cada job de sincronização
(estoque, pedidos, produtos, tags...), pra permitir sincronização incremental
em vez de sempre puxar tudo de novo.

Uso básico:

    from checkpoint_utils import get_last_run, set_last_run

    ultima_execucao = get_last_run("estoque")   # None na primeira vez
    ...
    set_last_run("estoque")                     # salva "agora" ao final

Cada "job" (estoque, pedidos, produtos, tags) tem seu próprio checkpoint,
guardados no mesmo arquivo JSON.
"""

import json
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Pasta onde os checkpoints ficam salvos (mesma pasta TB_bases dos seus CSVs).
# Cada job tem seu PRÓPRIO arquivo (em vez de um único checkpoints.json
# compartilhado) justamente pra evitar que dois workflows rodando ao mesmo
# tempo (ex: estoque e produtos em paralelo) briguem pelo mesmo arquivo
# quando forem dar commit/push.
PASTA_DESTINO = "TB_bases"
PASTA_CHECKPOINTS = os.path.join(PASTA_DESTINO, "checkpoints")

# Formato de data usado pela API do Tiny
FORMATO_TINY = "%d/%m/%Y %H:%M:%S"

# Fuso de referência. O runner do GitHub Actions roda em UTC, então um
# datetime.now() "pelado" gravava o checkpoint 3h no futuro em relação ao
# horário que a API do Tiny entende — e a sincronização seguinte pulava
# tudo que tivesse mudado nessa janela de 3h. Sempre use agora() aqui.
FUSO_TINY = ZoneInfo("America/Sao_Paulo")

# Margem de segurança: subtrai alguns minutos do checkpoint salvo,
# pra não perder alterações que aconteceram bem no meio da execução anterior.
MARGEM_SEGURANCA_MINUTOS = 5


def agora() -> datetime:
    """Horário atual no fuso de Brasília, independente do fuso da máquina."""
    return datetime.now(FUSO_TINY)


def _arquivo_checkpoint(job_name: str) -> str:
    return os.path.join(PASTA_CHECKPOINTS, f"{job_name}.json")


def _carregar_checkpoint(job_name: str) -> str | None:
    caminho = _arquivo_checkpoint(job_name)
    if not os.path.exists(caminho):
        return None
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f).get("ultima_execucao")
    except (json.JSONDecodeError, OSError):
        # Arquivo corrompido ou ilegível: melhor recomeçar do zero
        # do que travar a execução do script principal.
        return None


def _salvar_checkpoint(job_name: str, valor: str) -> None:
    os.makedirs(PASTA_CHECKPOINTS, exist_ok=True)
    with open(_arquivo_checkpoint(job_name), "w", encoding="utf-8") as f:
        json.dump({"ultima_execucao": valor}, f, indent=2, ensure_ascii=False)


def get_last_run(job_name: str) -> str | None:
    """
    Retorna a data/hora da última execução bem-sucedida do job,
    já no formato que a API do Tiny espera (dd/mm/yyyy hh:mm:ss),
    com a margem de segurança já aplicada.

    Retorna None se o job nunca rodou (primeira execução = carga full).
    """
    valor = _carregar_checkpoint(job_name)
    if not valor:
        return None

    try:
        dt = datetime.strptime(valor, FORMATO_TINY)
    except ValueError:
        return None

    dt_com_margem = dt - timedelta(minutes=MARGEM_SEGURANCA_MINUTOS)
    return dt_com_margem.strftime(FORMATO_TINY)


def set_last_run(job_name: str, momento: datetime | None = None) -> None:
    """
    Salva o momento da execução atual como novo checkpoint do job.
    Se `momento` não for passado, usa o horário de Brasília (nunca o da
    máquina, que no GitHub Actions é UTC).

    Chame isso SÓ depois que o processamento terminar com sucesso,
    nunca no início do script.
    """
    momento = momento or agora()
    _salvar_checkpoint(job_name, momento.strftime(FORMATO_TINY))


def reset_job(job_name: str) -> None:
    """Remove o checkpoint de um job específico, forçando a próxima
    execução a ser uma carga full novamente. Útil depois de exclusões
    em massa no Tiny, ou pra reconciliar dados periodicamente."""
    caminho = _arquivo_checkpoint(job_name)
    if os.path.exists(caminho):
        os.remove(caminho)