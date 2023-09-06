import json
from datetime import datetime

from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from plugins.sparketl.helper import ssh_set_env
from operators.parentDAG import BaseDAG


dag_id = "cdc"
ds = '{{ ds }}'
spark_class_path = '/tmp/spark_pipelines/cdc'
dag_config = json.loads(Variable.get(dag_id + '_config', deserialize_json=False), strict=False, encoding='utf-8')
tables = dag_config['tables']

with BaseDAG(custom_args=args, schedule_interval='0 * * * *', dag_id=dag_id) as dag:
    env_params = {"ds": ds}

    def create_ssh_cdp_tasks(table_data):
        table_name = table_data.get("table_name")
        table_data["ds"] = ds

        def _load_history_or_not(**kwargs):
            return f"history_{table_name}_load" if kwargs["execution_date"].hour == 0 else f'daily_only_{table_name}'

        raw_load = SSHOperator(
            task_id=f"raw_{table_name}_stream",
            ssh_conn_id="ssh_default",
            dag=dag,
            command=ssh_set_env(table_data, f"python3 {spark_class_path}/raw_load.py")
            )

        daily_load = SSHOperator(
            task_id=f"daily_{table_name}_load",
            ssh_conn_id="ssh_default",
            dag=dag,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            command=ssh_set_env(table_data, f"python3 {spark_class_path}/daily_load.py")
            )

        daily_or_historical_load = BranchPythonOperator(
            task_id=f'check_load_history_for_{table_name}_or_not',
            python_callable=_load_history_or_not,
            provide_context=True,
            dag=dag
            )

        daily_only = DummyOperator(task_id=f'daily_only_{table_name}', dag=dag)

        history_load = SSHOperator(
            task_id=f"history_{table_name}_load",
            ssh_conn_id="ssh_default",
            dag=dag,
            command=ssh_set_env(table_data, f"python3 {spark_class_path}/history_load.py --mode airflow")
            )

        return raw_load >> daily_load >> daily_or_historical_load >> [history_load, daily_only]

    def create_tasks_dynamically(tables):
        return [create_ssh_cdp_tasks(t) for t in tables]

tasks = create_tasks_dynamically(tables)
