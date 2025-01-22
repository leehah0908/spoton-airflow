from operators.today_game_check_operator import TodayGameCheck
from airflow import DAG
import pendulum

# 오늘 경기 리스트 업데이트
with DAG(
    dag_id='dags_today_game_check',
    # 매일 00:00 실행
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2025, 1, 8, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    today_game_check = TodayGameCheck(
        task_id='today_game_check'
    )

    today_game_check