from airflow.models.baseoperator import BaseOperator
import redis

class TodayGameRealTimeUpdate(BaseOperator):

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

        self.redis_client_db3 = redis.StrictRedis(
            host="spoton-redis",
            port=6379,
            db=3,
            decode_responses=True
        )

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
                    "gameId" : temp_game_id,
                    'gameDate' : game_board_data.get('gameDateTime'),
                    'sports' : game_board_data.get('superCategoryId'),
                    'league' : game_board_data.get('categoryId'),
                    'homeTeam' : game_board_data.get('homeTeamName'),
                    'awayTeam' : game_board_data.get('awayTeamName'),
                    "gameBoard": game_board_data,
                    "gameDetail": game_detail_data,
                    'cancel' : game_board_data.get('cancel'),
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
                    'gameDate' : game_board_data.get('gameDateTime'),
                    'sports' : game_board_data.get('superCategoryId'),
                    'league' : game_board_data.get('categoryId'),
                    'homeTeam' : game_board_data.get('homeTeamName'),
                    'awayTeam' : game_board_data.get('awayTeamName'),
                    "gameBoard": game_board_data,
                    "gameDetail": game_detail,
                    'cancel' : game_board_data.get('cancel'),
                })
            except:
                self.log.info(f"실패 ID : {temp_game_id}")
                continue

        return data_list
    
    # lck 경기 데이터 요청
    def lck_update(self, lck_list):
        from datetime import datetime
        import requests
        import json

        data_list = []

        for temp_game_id in lck_list:
            try:
                game_board_url = f"https://esports-api.game.naver.com/service/v1/match/gameId/{temp_game_id}"
                game_board_response = requests.get(game_board_url).json().get('content')

                gameBoard = {
                    'homeTeamScore' : game_board_response.get('homeScore'),
                    'awayTeamScore' : game_board_response.get('awayScore'),
                    'statusCode' : game_board_response.get('matchStatus'),
                    'statusInfo' : game_board_response.get('currentMatchSet'),
                    'stadium' : game_board_response.get('stadium')
                    }
                game_board_to_json = json.dumps(gameBoard)

                data_list.append({
                    'gameId':temp_game_id,
                    'gameDate' : datetime.fromtimestamp(game_board_response.get('startDate')/1000),
                    'sports' : 'esports',
                    'league' : game_board_response.get('topLeagueId'),
                    'homeTeam' : game_board_response.get('homeTeam').get('name'),
                    'awayTeam' : game_board_response.get('awayTeam').get('name'),
                    'gameBoard': gameBoard,
                    "gameDetail": None,
                    'cancel' : False,
                    })
            except:
                self.log.info(f"실패 ID : {temp_game_id}")
                continue
        return data_list

    # 오퍼레이터 실행
    def execute(self, context):
        import json

        data = self.redis_client.get("today_game_list")
        today_game_list = json.loads(data)

        sports_data = self.sports_update(today_game_list.get('sports_list'))
        epl_data = self.epl_update(today_game_list.get('epl_list'))
        lck_data = self.lck_update(today_game_list.get('lck_list'))

        all_list = sports_data + epl_data + lck_data
        
        for temp_data in all_list:
            try:
                save_data = json.dumps(temp_data, ensure_ascii=False)
                self.log.info(save_data)
                self.redis_client_db3.set(temp_data.get("gameId"), save_data, ex=70)
            except Exception as e:
                self.log.info(e)
                self.log.info(temp_data.get('gameId'))
