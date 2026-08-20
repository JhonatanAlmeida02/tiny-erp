"""
sharepoint_utils.py
---------------------
Envia arquivos para uma pasta de uma biblioteca de documentos do SharePoint,
via Microsoft Graph API, autenticando como o App Registration do Azure AD
("GitHub Actions - Tiny ERP Sync") — client credentials, sem usuário logado.

Requer 3 variáveis de ambiente (cadastradas como Secrets no GitHub):
    AZURE_TENANT_ID
    AZURE_CLIENT_ID
    AZURE_CLIENT_SECRET

Uso básico:
    from sharepoint_utils import upload_to_sharepoint
    upload_to_sharepoint("TB_bases/Tb_BaseEstoque_Tiny.csv", "tinyEstoque")

Reaproveitável pra qualquer outro arquivo/planilha futura — só chamar de
novo com outro caminho local e outra pasta de destino.
"""

import os

import requests

TENANT_ID = os.environ.get("AZURE_TENANT_ID", "")
CLIENT_ID = os.environ.get("AZURE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "")

# Não são segredos (é só o "endereço" dos sites, público dentro da empresa),
# por isso ficam fixos aqui em vez de precisar virar Secret no GitHub.
# Pra adicionar um novo site de destino no futuro, só acrescenta uma entrada
# neste dicionário.
SITES = {
    "dpLogistica": {
        "hostname": "mediprobrasil.sharepoint.com",
        "site_path": "/sites/dpLogistica",
    },
    "prodigyHub": {
        "hostname": "mediprobrasil.sharepoint.com",
        "site_path": "/sites/ProdigyHub",
    },
}

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def _obter_token() -> str:
    """Autentica como o App Registration (client credentials) e retorna um access token."""
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    dados = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
    }
    resp = requests.post(url, data=dados, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]


def _obter_site_id(token: str, hostname: str, site_path: str) -> str:
    """Resolve o ID interno do site do SharePoint a partir do hostname + caminho."""
    url = f"{GRAPH_BASE}/sites/{hostname}:{site_path}"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]


def upload_to_sharepoint(caminho_local: str, pasta_destino: str, nome_arquivo: str | None = None,
                          site: str = "dpLogistica") -> None:
    """
    Envia um arquivo local para uma pasta dentro da biblioteca "Documentos
    Compartilhados" do site do SharePoint configurado. Sobrescreve o arquivo
    se ele já existir lá (upload é sempre "tudo ou nada", não faz merge).

    pasta_destino: caminho da pasta dentro da biblioteca, ex: "tinyEstoque"
                   (sem barra no início/fim).
    site: chave do dicionário SITES (ex: "dpLogistica", "prodigyHub").
    """
    if site not in SITES:
        raise ValueError(f"Site '{site}' não configurado. Opções: {list(SITES)}")
    if not (TENANT_ID and CLIENT_ID and CLIENT_SECRET):
        raise RuntimeError(
            "Variáveis AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET "
            "não configuradas (cadastre como Secrets no GitHub)."
        )
    if not os.path.exists(caminho_local):
        raise FileNotFoundError(f"Arquivo local não encontrado: {caminho_local}")

    site_config = SITES[site]
    nome_arquivo = nome_arquivo or os.path.basename(caminho_local)
    token = _obter_token()
    site_id = _obter_site_id(token, site_config["hostname"], site_config["site_path"])

    url = f"{GRAPH_BASE}/sites/{site_id}/drive/root:/{pasta_destino}/{nome_arquivo}:/content"

    with open(caminho_local, "rb") as f:
        conteudo = f.read()

    resp = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "text/csv"},
        data=conteudo,
        timeout=60,
    )
    resp.raise_for_status()
    print(f"📤 Enviado para o SharePoint ({site}): {pasta_destino}/{nome_arquivo}")
