from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator



with DAG(
    'clean_all_xcom',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'max_active_tasks': 7,
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        #'on_success_callback': cleanup_xcom,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='',
    schedule_interval=None,
    start_date=datetime(2022, 2, 22),
    catchup=False
) as dag:

    delete_xcom = PostgresOperator(
        task_id='delete_xcom',
        postgres_conn_id='postgre_connection',
        sql="delete from xcom"
    )

    delete_xcom