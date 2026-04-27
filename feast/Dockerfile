FROM python:3.11-slim

RUN pip install --no-cache-dir \
    feast==0.36.0 \
    redis==5.0.1 \
    psycopg2-binary==2.9.9 \
    boto3==1.34.0 \
    pandas==2.1.0 \
    pyarrow==14.0.2

WORKDIR /feast
COPY feature_store.yaml .
COPY features/ features/

ENV FEAST_USAGE=False

CMD ["feast", "serve", "--host", "0.0.0.0", "--port", "6566"]
