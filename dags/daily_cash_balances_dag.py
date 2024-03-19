import datetime as dt

from kubernetes.client import models as k8s

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.secret import Secret

configmaps = [
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="api-config")),
    k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="security-config"))
]

secrets = [
    Secret("env", None, "security-secret")
]

with DAG(
        dag_id="daily_cash_balances_dag",
        description="Invoke debezium to import Cash Balances",
        start_date=dt.datetime(2024, 3, 15),
        schedule_interval="@hourly",
        default_args={"depends_on_past": True},
) as dag:
    insert_cash_balances_signal = KubernetesPodOperator(
        task_id="insert_cash_balances_signal",
        image="kind-registry:5000/cash-balances-importer:latest",
        cmds=["/app/bin/cash-balances-importer"],
        namespace="infrastructure",
        env_from=configmaps,
        secrets=secrets,
        name="cash-balances-importer",
        in_cluster=True,
        image_pull_policy="Always",
        on_finish_action="delete_succeeded_pod"
    )
