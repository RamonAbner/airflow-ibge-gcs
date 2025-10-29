# dags/ingestion/ibge.py
from typing import Any, Dict, List
import re
import requests
from requests.adapters import HTTPAdapter, Retry
import pandas as pd

def _sanitize_bq_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Garante nomes válidos p/ BigQuery: snake_case, sem pontos/acentos/espaços, não inicia com dígito."""
    def clean(name: str) -> str:
        name = name.strip().lower()
        name = name.replace(".", "_").replace(" ", "_").replace("-", "_")
        name = re.sub(r"[^a-z0-9_]", "_", name)     # só [a-z0-9_]
        if re.match(r"^[0-9]", name):               # não iniciar com dígito
            name = f"_{name}"
        return re.sub(r"_+", "_", name)             # colapsa múltiplos "_"
    df.columns = [clean(c) for c in df.columns]
    return df

class Ibge:
    def __init__(self, base_url: str = "https://servicodados.ibge.gov.br/api/v1"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": "ibge-ingestion/1.0"})

    def get_df(self, path: str) -> pd.DataFrame:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.get(url, timeout=(5, 30))
        resp.raise_for_status()
        data: List[Dict[str, Any]] | Dict[str, Any] = resp.json()

        if not data:
            return pd.DataFrame()
        if isinstance(data, dict):
            data = [data]

        # 🔑 Achata com "_" para evitar nomes com ponto (ex.: regiao_id)
        df = pd.json_normalize(data, sep="_")
        # 🔑 Sanitiza nomes para compatibilidade com BigQuery
        return _sanitize_bq_columns(df)

    def get_estados(self) -> pd.DataFrame:
        return self.get_df("localidades/estados")

    def get_municipios(self) -> pd.DataFrame:
        return self.get_df("localidades/municipios")
