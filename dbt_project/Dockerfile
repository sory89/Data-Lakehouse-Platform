FROM python:3.11-slim
WORKDIR /dbt
# Pin protobuf to avoid MessageToJson bug in dbt 1.7.0
RUN pip install --no-cache-dir \
    dbt-postgres==1.7.0 \
    protobuf==4.25.3
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["dbt", "run", "--profiles-dir", ".", "--project-dir", "."]
