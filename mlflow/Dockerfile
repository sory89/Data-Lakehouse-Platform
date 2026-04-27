FROM python:3.11-slim

RUN pip install --no-cache-dir \
    mlflow==2.10.0 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0 \
    scikit-learn==1.3.2

ENV MLFLOW_S3_ENDPOINT_URL=http://minio:9000
ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin

EXPOSE 5001

CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5001", \
     "--backend-store-uri", "postgresql://lakehouse:lakehouse@postgres:5432/lakehouse", \
     "--default-artifact-root", "s3://mlflow/artifacts", \
     "--serve-artifacts"]
