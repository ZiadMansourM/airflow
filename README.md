```bash
Push to sales/ ─────────────────────────────▶ build-sales.yaml
                                               ├─ Build + push ziadmmhassanin/process-sales:<commit>
                                               └─ PR: bump image tag in dag_sales.py
                                                                 ▼
                                                     Merge PR to main
                                                                 ▼

Push to iot/ ───────────────────────────────▶ build-iot.yaml
                                               ├─ Build + push ziadmmhassanin/process-iot:<commit>
                                               └─ PR: bump image tag in dag_iot.py
                                                                 ▼
                                                     Merge PR to main
                                                                 ▼

On any DAG tag bump merge ────────────────▶ build-airflow.yaml
                                             ├─ Build + push ziadmmhassanin/airflow:<commit>
                                             └─ Patch image tag in helm/airflow-values.yaml
                                                                ▼
                                                    Git commit to main branch
                                                                ▼
                                                        Argo CD auto-sync
                                                                ▼
                                                    Airflow redeployed in cluster
```

```bash
ziadh@Ziads-MacBook-Air demo % tree -I '*.jsonl|*.csv|*.log' -I 'data'
.
├── airflow
│   ├── dags
│   │   ├── dag_iot.py
│   │   └── dag_sales.py
│   └── Dockerfile
├── iot
│   ├── Dockerfile
│   ├── process_iot.py
│   └── requirements.txt
├── README.md
├── sales
│   ├── Dockerfile
│   ├── process_sales.py
│   └── requirements.txt
└── values.yaml

4 directories, 11 files
ziadh@Ziads-MacBook-Air demo % 
```

```bash
minikube start --memory max --cpus max

minikube stop
minikube delete

docker build -t ziadmmhassanin/airflow:2.10.6 .
docker push ziadmmhassanin/airflow:2.10.6

docker build -t ziadmmhassanin/process-sales:v0.1.0 .
docker push ziadmmhassanin/process-sales:v0.1.0

docker build -t ziadmmhassanin/process-iot:v0.1.0 .
docker push ziadmmhassanin/process-iot:v0.1.0

helm repo add minio https://charts.min.io/
helm search repo minio

# x62hAFEb4wkRNRaR 
# IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW
# "S3_ENDPOINT_URL": "http://minio.minio.svc.cluster.local:9000"

helm install minio minio/minio --version 5.4.0 --set mode=standalone --namespace minio --create-namespace --set rootUser=x62hAFEb4wkRNRaR --set rootPassword=IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW --set 'buckets[0].name=raw' --set 'buckets[0].policy=public' --set 'buckets[1].name=processed' --set 'buckets[1].policy=public' --set resources.requests.memory=512Mi --set resources.requests.cpu=250m

helm upgrade --install airflow airflow/airflow --set airflowPodAnnotations.random=r$(uuidgen) --version 1.16.0 --namespace airflow --create-namespace -f values.yaml

```

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

kubectl port-forward svc/minio 9000:9000 --namespace minio

kubectl port-forward svc/minio-console 9001:9001 --namespace minio
```

# Go:
    - Arrive: Thuresday, June 12 Friday, June 13
    - Before Saturday: 14 June
# Return:
    - Arrive: Wednesday June 18 @ 03 PM


```bash
ziadh@Ziads-MacBook-Air airflow % helm install minio minio/minio --version 5.4.0 --set mode=standalone --namespace minio --create-namespace --set rootUser=x62hAFEb4wkRNRaR --set rootPassword=IF1r6ZtELYWbKmBFOamMpj0XjK2W96sW --set 'buckets[0].name=raw' --set 'buckets[0].policy=public' --set 'buckets[1].name=processed' --set 'buckets[1].policy=public' --set resources.requests.memory=512Mi --set resources.requests.cpu=250m
NAME: minio
LAST DEPLOYED: Sat Jun  7 19:32:04 2025
NAMESPACE: minio
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
MinIO can be accessed via port 9000 on the following DNS name from within your cluster:
minio.minio.cluster.local

To access MinIO from localhost, run the below commands:

  1. export POD_NAME=$(kubectl get pods --namespace minio -l "release=minio" -o jsonpath="{.items[0].metadata.name}")

  2. kubectl port-forward $POD_NAME 9000 --namespace minio

Read more about port forwarding here: http://kubernetes.io/docs/user-guide/kubectl/kubectl_port-forward/

You can now access MinIO server on http://localhost:9000. Follow the below steps to connect to MinIO server with mc client:

  1. Download the MinIO mc client - https://min.io/docs/minio/linux/reference/minio-mc.html#quickstart

  2. export MC_HOST_minio-local=http://$(kubectl get secret --namespace minio minio -o jsonpath="{.data.rootUser}" | base64 --decode):$(kubectl get secret --namespace minio minio -o jsonpath="{.data.rootPassword}" | base64 --decode)@localhost:9000

  3. mc ls minio-local
ziadh@Ziads-MacBook-Air airflow % 
```

```bash
ziadh@Ziads-MacBook-Air demo % helm upgrade --install airflow airflow/airflow --set airflowPodAnnotations.random=r$(uuidgen) --version 1.16.0 --namespace airflow --create-namespace -f values.yaml
Release "airflow" has been upgraded. Happy Helming!
NAME: airflow
LAST DEPLOYED: Sat Jun  7 19:59:37 2025
NAMESPACE: airflow
STATUS: deployed
REVISION: 2
TEST SUITE: None
NOTES:
Thank you for installing Apache Airflow 2.10.5!

Your release is named airflow.
You can now access your dashboard(s) by executing the following command(s) and visiting the corresponding port at localhost in your browser:

Airflow Webserver:     kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow
Default Webserver (Airflow UI) Login credentials:
    username: admin
    password: admin
Default Postgres connection credentials:
    username: postgres
    password: postgres
    port: 5432

You can get Fernet Key value by running the following:

    echo Fernet Key: $(kubectl get secret --namespace airflow airflow-fernet-key -o jsonpath="{.data.fernet-key}" | base64 --decode)

WARNING:
    Kubernetes workers task logs may not persist unless you configure log persistence or remote logging!
    Logging options can be found at: https://airflow.apache.org/docs/helm-chart/stable/manage-logs.html
    (This warning can be ignored if logging is configured with environment variables or secrets backend)
ziadh@Ziads-MacBook-Air demo % 
```

```bash
process-sales-daihrbdp                  0/1     Terminating         0          2s    airflow_kpo_in_cluster=True,airflow_version=2.10.5,already_checked=True,dag_id=sales_pipeline,kubernetes_pod_operator=True,run_id=manual__2025-06-07T181009.7387870000-f2f1db2a9,task_id=process_sales,try_number=1

dag_id=sales_pipeline
kubectl get pods --namespace airflow -l dag_id=sales_pipeline

kubectl -n airflow logs -l dag_id=sales_pipeline 
```