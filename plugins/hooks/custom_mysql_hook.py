from airflow.hooks.base import BaseHook
import pymysql
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class MySQLAPIHook(BaseHook):

    def __init__(self, mysql_conn_id):
        self.mysql_conn_id = mysql_conn_id
        self.engine = None
        self._init_engine()
    
    def _init_engine(self):
        airflow_conn = BaseHook.get_connection(self.mysql_conn_id)

        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.schema = airflow_conn.schema

        db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.schema}"
        self.engine = create_engine(db_connection_str)
        self.log.info("MySQL engine 생성")

    # select 쿼리 실행
    def select_query(self, query):
        self.log.info(f"***** Running query: {query} *****")

        result = pd.read_sql(query, con=self.engine)
        return result
    
    # update 쿼리 실행
    def update_query(self, temp_data):
        game_table = Table('game_data', MetaData(), autoload_with=self.engine)

        qr = game_table.update().where(game_table.c.gameId == temp_data.get('gameId')
                                       ).values(gameBoard = temp_data.get('gameBoard'),
                                                gameDetail = temp_data.get('gameDetail'))
        
        with self.engine.begin() as conn:
            conn.execute(qr)
        
    # update 쿼리 실행
    def lck_update_query(self, temp_data):
        game_table = Table('game_data', MetaData(), autoload_with=self.engine)

        qr = game_table.update().where(game_table.c.gameId == temp_data.get('gameId')
                                       ).values(gameBoard = temp_data.get('gameBoard'))
        
        with self.engine.begin() as conn:
            conn.execute(qr)


    # 새로운 경기 데이터 append
    def append_data(self, save_df):
        save_df.to_sql(name = "game_data", con = self.engine, if_exists = "append", index = False)