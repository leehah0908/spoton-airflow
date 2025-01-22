from airflow.models.baseoperator import BaseOperator
from hooks.custom_mysql_hook import MySQLAPIHook
import redis

class TodayGameCheck(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = 'conn-db-mysql-custom'

        # Redis 클라이언트 설정
        self.redis_client = redis.StrictRedis(
            host="spoton-redis",
            port=6379,
            db=4,
            decode_responses=True
        )

    # 오퍼레이터 실행
    def execute(self, context):
        from datetime import datetime
        import pendulum
        import json

        custom_mysql_hook = MySQLAPIHook(self.mysql_conn_id)

        # 오늘 날짜 계산
        today_start = (datetime.now(pendulum.timezone("Asia/Seoul"))).strftime("%Y-%m-%d 00:00:00")
        today_end = (datetime.now(pendulum.timezone("Asia/Seoul"))).strftime("%Y-%m-%d 23:59:59")

        # 범위 조건으로 쿼리 작성 (index 활용)
        query = f"SELECT * FROM game_data WHERE gameDate BETWEEN '{today_start}' AND '{today_end}'"

        df = custom_mysql_hook.select_query(query)

        # 어제 경기 gameId 불러와서 리스트에 저장
        sports_list = df[(df['league'] != 'epl') & (df['league'] != 'lck')]['gameId'].to_list()
        epl_list = df[(df['league'] == 'epl')]['gameId'].to_list()
        lck_list = df[(df['league'] == 'lck')]['gameId'].to_list()

        today_game_dic = {
            "sports_list": sports_list,
            "epl_list": epl_list,
            "lck_list:": lck_list
        }

        self.redis_client.set("today_game_list", json.dumps(today_game_dic), ex=88200)