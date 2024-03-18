import datetime as dt

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

with DAG(
        dag_id="daily_cash_balances_dag.py",
        description="Invoke debezium to import Cash Balances",
        start_date=dt.datetime(2024, 3, 15),
        schedule_interval="@hourly",
        default_args={"depends_on_past": True},
) as dag:
    insert_cash_balances_signal = KubernetesPodOperator(
        task_id="insert_cash_balances_signal",
        image="kind-registry:5000/cash-balances-importer:latest",
        cmds=["su", "-", "appuser", "-c"],
        params=["/app/bin/cash-balances-importer"],
        namespace="infrastructure",
        name="cash-balances-importer",
        in_cluster=True,
        image_pull_policy="Always",
        on_finish_action="delete_succeeded_pod",
    )
