import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",       # 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],   # 필요없으면 지워도 됨
    # params={"example_key": "example_value"},
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )
    
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOMENAME",
    )
    
    bash_t1 >> bash_t2