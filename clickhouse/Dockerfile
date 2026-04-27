FROM python:3.11-slim

RUN pip install --no-cache-dir \
    clickhouse-connect==0.7.0 \
    psycopg2-binary==2.9.9

COPY sync.py /app/sync.py
WORKDIR /app

CMD ["python", "sync.py"]
