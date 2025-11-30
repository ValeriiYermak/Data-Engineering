FROM apache/airflow:2.9.1

USER root

# Встановлюємо Java та системні пакети
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Встановлюємо Python пакети
RUN pip install --no-cache-dir \
    pyspark==3.4.1 \
    requests==2.31.0

# Встановлюємо змінні середовища
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3