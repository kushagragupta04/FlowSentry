# =============================================================
# Flink Processor Dockerfile
# Based on official PyFlink image with Redis client added.
# =============================================================
FROM flink:1.18-scala_2.12-java11

# Install Python 3.11 (Flink 1.18 ships with Python 3.x support)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf python3.11 /usr/bin/python3 \
    && ln -sf python3.11 /usr/bin/python

# Install PyFlink matching the Flink image version
WORKDIR /opt/flink
RUN pip3 install --no-cache-dir apache-flink==1.18.0

# Install job dependencies
WORKDIR /opt/flink/usrlib
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy job code
COPY . /opt/flink/usrlib/

# Flink Prometheus metrics reporter JAR (needed for metrics.reporters=prom)
RUN wget -q -O /opt/flink/lib/flink-metrics-prometheus-1.18.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/1.18.0/flink-metrics-prometheus-1.18.0.jar

# Kafka SQL connector for PyFlink DataStream API
RUN wget -q -O /opt/flink/lib/flink-sql-connector-kafka-1.18.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.18.0/flink-sql-connector-kafka-1.18.0.jar

ENV PYTHONPATH=/opt/flink/usrlib \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# The base image entrypoint handles jobmanager/taskmanager mode
# Job is submitted separately via: flink run -py /opt/flink/usrlib/job.py
