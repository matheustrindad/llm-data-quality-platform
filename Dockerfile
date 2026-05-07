FROM apache/airflow:2.8.0-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --chown=airflow:root requirements.txt /opt/airflow/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt