"""
enviar_sharepoint.py
----------------------
Wrapper de linha de comando em cima do sharepoint_utils.py, pra usar como
um step separado nos workflows do GitHub Actions (assim, se o envio falhar,
fica claro nos logs qual etapa quebrou, sem misturar com a sincronização).

Uso:
    python enviar_sharepoint.py <caminho_local> <pasta_destino_no_sharepoint> [site]

site é opcional, "dpLogistica" por padrão. Outras opções: "prodigyHub"
(ver dicionário SITES em sharepoint_utils.py).

Exemplos:
    python enviar_sharepoint.py TB_bases/Tb_BaseEstoque_Tiny.csv tinyEstoque
    python enviar_sharepoint.py TB_bases/Tb_BaseEstoque_Tiny.csv TinyERP prodigyHub
"""

import sys

from sharepoint_utils import upload_to_sharepoint

if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        print("Uso: python enviar_sharepoint.py <arquivo_local> <pasta_no_sharepoint> [site]")
        sys.exit(1)

    caminho_local, pasta_destino = sys.argv[1], sys.argv[2]
    site = sys.argv[3] if len(sys.argv) == 4 else "dpLogistica"
    upload_to_sharepoint(caminho_local, pasta_destino, site=site)
