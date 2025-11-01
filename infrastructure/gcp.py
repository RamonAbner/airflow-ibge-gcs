from __future__ import annotations
from datetime import date, datetime
from io import BytesIO
from typing import Optional
import pandas as pd
from google.cloud import storage
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

class GCP:

    def __init__(self, bucket_name: str, gcp_conn_id: str = "google_cloud_default"):
        hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = hook.get_credentials()
        project_id = hook.project_id

        self.client = storage.Client(project=project_id, credentials=credentials)
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name

    def send_parquet(self, df: pd.DataFrame, system: str, table: str, dt: Optional[str] = None) -> str:
        if df is None or df.empty:
            raise ValueError("DataFrame vazio: nada para enviar ao GCS")

        dt_str = dt or date.today().isoformat()
        yyyy, mm, dd = dt_str.split("-")

        
        now = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_path = f"{system}/{table}/year={yyyy}/month={mm}/day={dd}/part-{now}.parquet"

        buf = BytesIO()
        df.to_parquet(buf, index=False, compression="snappy", engine="pyarrow")
        buf.seek(0)

        blob = self.bucket.blob(blob_path)

        
        blob.upload_from_file(
            buf,
            content_type="application/vnd.apache.parquet",
            if_generation_match=0
        )

        return f"gs://{self.bucket_name}/{blob_path}"
