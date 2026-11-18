FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless git \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java

USER airflow

# Airflow providers, pinned by the official constraints file
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
      apache-airflow-providers-databricks apache-airflow-providers-amazon

# The lakehouse package (Spark + Delta + Snowflake connector + Great Expectations)
COPY --chown=airflow:root pyproject.toml README.md /opt/airflow/gril/
COPY --chown=airflow:root src /opt/airflow/gril/src
RUN pip install --no-cache-dir "/opt/airflow/gril[spark,snowflake,quality]"

# dbt lives in its own venv to avoid dependency clashes with Airflow
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-snowflake>=1.8,<1.10"
