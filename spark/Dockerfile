FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9

# S3A JARs
RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Iceberg JARs
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
    curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar && \
    test -s /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar && \
    test -s /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar && \
    echo "✅ Iceberg JARs verified"

# spark-defaults.conf — loads Iceberg at Spark startup, before any Python code runs
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/spark-jobs
COPY jobs/ .
ENV PYTHONUNBUFFERED=1
