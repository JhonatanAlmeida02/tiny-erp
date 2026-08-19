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

# Arquivo onde os checkpoints ficam salvos (mesma pasta TB_bases dos seus CSVs)
PASTA_DESTINO = "TB_bases"
ARQUIVO_CHECKPOINT = os.path.join(PASTA_DESTINO, "checkpoints.json")

# Formato de data usado pela API do Tiny
FORMATO_TINY = "%d/%m/%Y %H:%M:%S"

# Margem de segurança: subtrai alguns minutos do checkpoint salvo,
# pra não perder alterações que aconteceram bem no meio da execução anterior.
MARGEM_SEGURANCA_MINUTOS = 5


def _carregar_checkpoints() -> dict:
    if not os.path.exists(ARQUIVO_CHECKPOINT):
        return {}
    try:
        with open(ARQUIVO_CHECKPOINT, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        # Arquivo corrompido ou ilegível: melhor recomeçar do zero
        # do que travar a execução do script principal.
        return {}


def _salvar_checkpoints(dados: dict) -> None:
    os.makedirs(PASTA_DESTINO, exist_ok=True)
    with open(ARQUIVO_CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=2, ensure_ascii=False)


def get_last_run(job_name: str) -> str | None:
    """
    Retorna a data/hora da última execução bem-sucedida do job,
    já no formato que a API do Tiny espera (dd/mm/yyyy hh:mm:ss),
    com a margem de segurança já aplicada.

    Retorna None se o job nunca rodou (primeira execução = carga full).
    """
    dados = _carregar_checkpoints()
    valor = dados.get(job_name)
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
    Se `momento` não for passado, usa datetime.now().

    Chame isso SÓ depois que o processamento terminar com sucesso,
    nunca no início do script.
    """
    dados = _carregar_checkpoints()
    momento = momento or datetime.now()
    dados[job_name] = momento.strftime(FORMATO_TINY)
    _salvar_checkpoints(dados)


def reset_job(job_name: str) -> None:
    """Remove o checkpoint de um job específico, forçando a próxima
    execução a ser uma carga full novamente. Útil depois de exclusões
    em massa no Tiny, ou pra reconciliar dados periodicamente."""
    dados = _carregar_checkpoints()
    if job_name in dados:
        del dados[job_name]
        _salvar_checkpoints(dados)