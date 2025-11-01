from typing import Any, Dict, List
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import pandas as pd
from utils.utils import clean_columnnames


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

        
        df = pd.json_normalize(data, sep="_")

        
        return clean_columnnames(df)


    def get_estados(self) -> pd.DataFrame:
        return self.get_df("localidades/estados")

    def get_municipios(self) -> pd.DataFrame:
        return self.get_df("localidades/municipios")
