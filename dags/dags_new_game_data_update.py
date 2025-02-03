from operators.new_game_data_operator import NewGameDataOperator
from airflow import DAG
import pendulum

# 새로운 경기 일정 업데이트
with DAG(
    dag_id='dags_new_game_data_update',
    # 매일 12:00 실행
    schedule='0 12 * * *',
    start_date=pendulum.datetime(2025, 1, 8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    new_game_data_update = NewGameDataOperator(
        task_id='new_game_data_update'
    )

    new_game_data_update