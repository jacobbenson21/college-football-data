# Custom Airflow image: base Airflow + Java (for PySpark) + an isolated venv that
# holds the project's dependencies (pyspark, dbt, boto3, snowflake) so they don't
# collide with Airflow's own pinned dependencies.
FROM apache/airflow:2.10.3-python3.12

USER root

# Java for PySpark; procps gives Spark the `ps` command it expects.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Project dependencies in a separate venv so Airflow's deps stay untouched.
COPY requirements.txt /tmp/requirements.txt
RUN python -m venv /opt/app-venv \
    && /opt/app-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/app-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

USER airflow
