from airflow.hooks.base import BaseHook
import pymysql
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table

class MySQLAPIHook(BaseHook):

    def __init__(self, mysql_conn_id):
        self.mysql_conn_id = mysql_conn_id
        
    # 첫 연결
    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.mysql_conn_id)

        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        # self.port = airflow_conn.port
        self.schema = airflow_conn.schema
        
        self.mysql_conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.schema,
            # port=self.port,
            charset='utf8'
            )
        
        self.log.info("MySQL connection successful!")

    # select 쿼리 실행
    def select_query(self, query):
        self.log.info(f"***** Running query: {query} *****")
        self.get_conn()

        db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/spoton"

        engine = create_engine(db_connection_str)
        result = pd.read_sql(query, con=engine)
        return result
    
    # update 쿼리 실행
    def update_query(self, temp_data):
        self.get_conn()
        db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/spoton"

        engine = create_engine(db_connection_str)
        game_table = Table('game_data', MetaData(), autoload=True, autoload_with=engine)

        qr = game_table.update().where(game_table.c.gameId == temp_data.get('gameId')
                                       ).values(gameBoard = temp_data.get('gameBoard'),
                                                gameDetail = temp_data.get('gameDetail'))
        # 쿼리 실행
        with engine.connect() as connection:
            result = connection.execute(qr)
            self.log.info(f"업데이트 컬럼 수: {result.rowcount}")
        
        # engine.execute(qr)

    # update 쿼리 실행 (lck)
    def lck_update_query(self, temp_data):
        self.get_conn()
        db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/spoton"

        engine = create_engine(db_connection_str)
        game_table = Table('game_data', MetaData(), autoload=True, autoload_with=engine)

        qr = game_table.update().where(game_table.c.gameId == temp_data.get('gameId')
                                       ).values(gameBoard = temp_data.get('gameBoard'))
        
        # 쿼리 실행
        with engine.connect() as connection:
            result = connection.execute(qr)
            self.log.info(f"업데이트 컬럼 수: {result.rowcount}")

        # engine.execute(qr)

    # 새로운 경기 데이터 append
    def append_data(self, save_df):
        self.get_conn()
        db_connection_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/spoton"

        engine = create_engine(db_connection_str)
        save_df.to_sql(name = "game_data", con = engine, if_exists = "append", index = False)