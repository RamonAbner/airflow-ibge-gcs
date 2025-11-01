from infrastructure.gcp import GCP
from infrastructure.ibge import Ibge
from config.settings import BUCKET_NAME, GCP_CONN_ID, SYSTEM, IBGE_BASE_URL


class IbgeToGcs:
    def __init__(self) -> None:
        self.system = SYSTEM
        self.ibge = Ibge(base_url=IBGE_BASE_URL)
        self.gcp = GCP(bucket_name=BUCKET_NAME, gcp_conn_id=GCP_CONN_ID)

    def run(self, table_path: str, dt: str | None = None) -> str:
        df = self.ibge.get_df(table_path)
        bad = [c for c in df.columns if "." in c]
        if bad:
            raise ValueError(f"Colunas inválidas com ponto: {bad}")

        table = table_path.split("/")[-1]
        return self.gcp.send_parquet(df, self.system, table, dt=dt)


def ibge_to_gcs(table_path: str, dt: str | None = None) -> str:
    pipeline = IbgeToGcs()
    return pipeline.run(table_path=table_path, dt=dt)
