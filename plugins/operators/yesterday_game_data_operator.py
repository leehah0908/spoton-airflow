from airflow.models.baseoperator import BaseOperator
from hooks.custom_mysql_hook import MySQLAPIHook

class YesterdayGameDataOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = 'conn-db-mysql-custom'

    # 일반 종목 경기 데이터 요청
    def sports_update(self, sports_list):
        import requests
        import json

        data_list = []

        for temp_game_id in sports_list:
            try:
                game_board_url = f"https://api-gw.sports.naver.com/schedule/games/{temp_game_id}"
                game_board_response = requests.get(game_board_url).json()
                game_board_data = game_board_response.get('result').get('game')
                game_board_to_json = json.dumps(game_board_data)

                game_detail_url = f"https://api-gw.sports.naver.com/schedule/games/{temp_game_id}/record"
                game_detail_response = requests.get(game_detail_url).json()
                game_detail_data = game_detail_response.get('result').get('recordData')
                game_detail_to_json = json.dumps(game_detail_data)

                data_list.append({
                    "gameId":temp_game_id,
                    "gameBoard": game_board_to_json,
                    "gameDetail": None if game_detail_to_json == 'null' else game_detail_to_json,
                })
            except:
                self.log.info(f"실패 ID : {temp_game_id}")
                continue

        return data_list

    # epl 경기 데이터 요청
    def epl_update(self, epl_list):
        import requests
        import json

        data_list = []

        for temp_game_id in epl_list:
            try:
                away_teams = []
                home_teams = []
                away_players = []
                home_players = []
                
                game_board_url = f"https://api-gw.sports.naver.com/schedule/games/{temp_game_id}"
                game_board_response = requests.get(game_board_url).json()
                game_board_data = game_board_response.get('result').get('game')
                game_board_to_json = json.dumps(game_board_data)

                away_team_code = game_board_data.get('awayTeamCode')
                home_team_code = game_board_data.get('homeTeamCode')

                game_team_url =   f"https://api-gw.sports.naver.com/stats/categories/epl/games/{temp_game_id}/teams"
                game_team_response = requests.get(game_team_url).json()

                game_player_url = f"https://api-gw.sports.naver.com/stats/categories/epl/games/{temp_game_id}/players"
                game_player_response = requests.get(game_player_url).json()

                game_team_data = game_team_response.get('result').get('teams')
                game_player_data = game_player_response.get('result').get('players')

                for temp_team in game_team_data:
                    if temp_team.get('teamId') == away_team_code:
                        away_teams.append(temp_team)
                    elif temp_team.get('teamId') == home_team_code:
                        home_teams.append(temp_team)

                for temp_player in game_player_data:
                    if temp_player.get('teamId') == away_team_code:
                        away_players.append(temp_player)
                    elif temp_player.get('teamId') == home_team_code:
                        home_players.append(temp_player)

                game_detail = {
                    'away_teams': away_teams,
                    'home_teams': home_teams,
                    'away_players': away_players,
                    'home_players': home_players
                    }
                game_detail_to_json = json.dumps(game_detail)

                data_list.append({
                    "gameId":temp_game_id,
                    "gameBoard": game_board_to_json,
                    "gameDetail": None if game_detail_to_json == 'null' else game_detail_to_json,
                })
            except:
                self.log.info(f"실패 ID : {temp_game_id}")
                continue

        return data_list
    
    # lck 경기 데이터 요청
    def lck_update(self, lck_list):
        import requests
        import json

        data_list = []

        for temp_game_id in lck_list:
            try:
                game_board_url = f"https://esports-api.game.naver.com/service/v1/match/gameId/{temp_game_id}"
                game_board_response = requests.get(game_board_url).json()

                gameBoard = {
                    'homeTeamScore' : game_board_response.get('content').get('homeScore'),
                    'awayTeamScore' : game_board_response.get('content').get('awayScore'),
                    'statusCode' : game_board_response.get('content').get('matchStatus'),
                    'statusInfo' : game_board_response.get('content').get('currentMatchSet'),
                    'stadium' : game_board_response.get('content').get('stadium')
                    }
                game_board_to_json = json.dumps(gameBoard)

                data_list.append({
                    'gameId':temp_game_id,
                    'gameBoard': game_board_to_json
                    })
            except:
                self.log.info(f"실패 ID : {temp_game_id}")
                continue
        return data_list

    # 오퍼레이터 실행
    def execute(self, context):
        from datetime import datetime, timedelta
        import pendulum
        
        check_update = 0

        custom_mysql_hook = MySQLAPIHook(self.mysql_conn_id)

        # 어제의 날짜 계산
        yesterday_start = (datetime.now(pendulum.timezone("Asia/Seoul")) - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        yesterday_end = (datetime.now(pendulum.timezone("Asia/Seoul")) - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")

        # 범위 조건으로 쿼리 작성
        query = f"SELECT * FROM game_data WHERE gameDate BETWEEN '{yesterday_start}' AND '{yesterday_end}'"

        df = custom_mysql_hook.select_query(query)
        self.log.info(f"업데이트할 경기수 : {len(df)}")

        # 어제 경기 gameId 불러와서 리스트에 저장
        sports_list = df[(df['league'] != 'epl') & (df['league'] != 'lck')]['gameId'].to_list()
        epl_list = df[(df['league'] == 'epl')]['gameId'].to_list()
        lck_list = df[(df['league'] == 'lck')]['gameId'].to_list()

        sports_data = self.sports_update(sports_list)
        epl_data = self.epl_update(epl_list)
        lck_data = self.lck_update(lck_list)
        
        # sports update
        try:
            for temp_data in sports_data:
                custom_mysql_hook.update_query(temp_data)
                check_update += 1
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))

        # epl update
        try:
            for temp_data in epl_data:
                custom_mysql_hook.update_query(temp_data)
                check_update += 1
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))

        # lck update
        try:
            for temp_data in lck_data:
                custom_mysql_hook.lck_update_query(temp_data)
                check_update += 1
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))

        self.log.info(f"업데이트한 경기수 : {check_update}")