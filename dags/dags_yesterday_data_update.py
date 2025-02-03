from operators.yesterday_game_data_operator import YesterdayGameDataOperator
from airflow import DAG
import pendulum

# 어제 경기 결과 재수집
with DAG(
    dag_id='dags_yesterday_data_update',
    # 매일 00:00 실행
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2025, 1, 8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    yesterday_game_data_update = YesterdayGameDataOperator(
        task_id='yesterday_game_data_update'
    )

    yesterday_game_data_update