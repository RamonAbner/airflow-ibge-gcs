"""
Configurações globais do projeto de ingestão IBGE → GCS.
Centraliza variáveis de ambiente e parâmetros compartilhados.
"""

# 🔧 Configurações do Google Cloud
BUCKET_NAME = "rm_cm_landing"  # Nome do bucket no GCS
GCP_CONN_ID = "gcp_conn_id"     # ID da conexão configurada no Airflow

# 🧩 Configurações do sistema IBGE
SYSTEM = "ibge"
IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v1"

# 🕒 Padrão de timezone do Airflow
TIMEZONE = "America/Sao_Paulo"

# 📅 Padrão de agendamento (pode ser reutilizado em DAGs)
SCHEDULE = "@daily"

# 🚀 Diretórios padrão (caso queira gerar localmente antes do upload)
DATA_PATH = "/opt/airflow/data"
TMP_PATH = "/tmp"
