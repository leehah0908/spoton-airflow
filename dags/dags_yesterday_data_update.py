from operators.yesterday_game_data_operator import YesterdayGameDataOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_yesterday_data_update',
    schedule='1 0 * * *',
    start_date=pendulum.datetime(2025, 1, 8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    yesterday_game_data_update = YesterdayGameDataOperator(
        task_id='yesterday_game_data_update'
    )

    yesterday_game_data_update