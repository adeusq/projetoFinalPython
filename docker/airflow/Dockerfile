FROM apache/airflow:2.9.3-python3.11

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11

COPY docker/airflow/requirements.txt /requirements.txt

# Usa o arquivo de constraints oficial do Airflow para impedir que o pip resolva/instale
# uma versão mais nova do apache-airflow como dependência transitiva das providers
# (isso já quebrou o entrypoint uma vez: pip puxou Airflow 3.x e sobrescreveu o 2.9.3
# da imagem base, removendo o executável `airflow` esperado pelos scripts do container).
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
