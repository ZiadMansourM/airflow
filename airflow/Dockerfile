FROM apache/airflow:2.10.5

RUN pip install --no-cache-dir 'apache-airflow-providers-cncf-kubernetes==7.6.0'

COPY dags/ /opt/airflow/dags/
