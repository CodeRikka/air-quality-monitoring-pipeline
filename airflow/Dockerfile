FROM apache/airflow:3.1.8-python3.11

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
USER airflow
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.11.txt" \
    -r /tmp/requirements.txt
