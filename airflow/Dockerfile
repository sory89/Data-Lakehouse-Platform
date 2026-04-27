FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-docker==3.8.0 \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-redis==3.5.0 \
    apache-airflow-providers-http==4.8.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0

COPY dags/ /opt/airflow/dags/
RUN mkdir -p /opt/airflow/plugins
