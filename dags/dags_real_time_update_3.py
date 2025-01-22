from operators.today_game_update_operator import TodayGameRealTimeUpdate
from airflow import DAG
import pendulum

# 실시간 경기 정보 업데이트
with DAG(
    dag_id='dags_real_time_update_3',
    schedule='2-59/5 * * * *',
    start_date=pendulum.datetime(2025, 1, 8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    today_game_real_time_update = TodayGameRealTimeUpdate(
        task_id='today_game_real_time_update',
    )

    today_game_real_time_update