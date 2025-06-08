# from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

default_args = {
    "owner": "ziad",
    "retries": 0,
    # "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iot_pipeline",
    default_args=default_args,
    # start_date=days_ago(1),
    start_date=datetime(2025, 1, 1),
    # schedule_interval="@hourly",
    schedule_interval=None,
    catchup=False,
    tags=["batch", "iot"],
) as dag:

    process_sales = KubernetesPodOperator(
        task_id="process_iot",
        name="process-iot",
        namespace="airflow",             # ⬅ same namespace Helm chart uses
        image_pull_policy="Always",  # always pull latest image
        image="ziadmmhassanin/process-iot:v0.1.0",
        cmds=["python", "process_iot.py"],    # inside the container’s /app
        # ENV VARS—override creds/per-run settings without rebuilding image
        env_vars={
            "S3_ENDPOINT_URL": "http://minio.minio.svc.cluster.local:9000",
            # "MINIO_ROOT_USER": "{{ var.value.MINIO_USER }}",
            # "MINIO_ROOT_PASSWORD": "{{ var.value.MINIO_PASS }}",
            "MINIO_ROOT_USER": "x62hAFEb4wkRNRaR",
            "MINIO_ROOT_PASSWORD": "IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW",
        },
        # secrets=[Secret(
        #     deploy_type="env",
        #     deploy_target="AWS_SECRET_ACCESS_KEY",
        #     secret="minio-cred",
        #     key="password",
        # )],
        get_logs=True,
        is_delete_operator_pod=True,     # clean up after finish
        in_cluster=True,                 # talk to K8s API from inside cluster
        # resources={"request_cpu":"200m","request_memory":"256Mi"}  # optional
    )