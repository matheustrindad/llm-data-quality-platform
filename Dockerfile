FROM apache/airflow:2.8.0-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download hadoop-aws JARs necessários para PySpark ler do MinIO via S3A
RUN mkdir -p /opt/spark-jars && \
    curl -L -o /opt/spark-jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

ENV SPARK_EXTRA_CLASSPATH=/opt/spark-jars/hadoop-aws-3.3.4.jar:/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt