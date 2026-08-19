"""
enviar_sharepoint.py
----------------------
Wrapper de linha de comando em cima do sharepoint_utils.py, pra usar como
um step separado nos workflows do GitHub Actions (assim, se o envio falhar,
fica claro nos logs qual etapa quebrou, sem misturar com a sincronização).

Uso:
    python enviar_sharepoint.py <caminho_local> <pasta_destino_no_sharepoint>

Exemplo:
    python enviar_sharepoint.py TB_bases/Tb_BaseEstoque_Tiny.csv tinyEstoque
"""

import sys

from sharepoint_utils import upload_to_sharepoint

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python enviar_sharepoint.py <arquivo_local> <pasta_no_sharepoint>")
        sys.exit(1)

    caminho_local, pasta_destino = sys.argv[1], sys.argv[2]
    upload_to_sharepoint(caminho_local, pasta_destino)
