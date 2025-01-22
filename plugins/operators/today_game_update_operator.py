from airflow.models.baseoperator import BaseOperator
import redis

class TodayGameRealTimeUpdate(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Redis 클라이언트 설정
        self.redis_client = redis.StrictRedis(
            host="spoton-redis",
            port=6379,
            db=4,
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
        import json

        data = self.redis_client.get("today_game_list")
        today_game_list = json.loads(data)

        sports_data = self.sports_update(today_game_list.get('sports_list'))
        epl_data = self.epl_update(today_game_list.get('epl_list'))
        lck_data = self.lck_update(today_game_list.get('lck_list'))
        
        # sports update
        try:
            for temp_data in sports_data:
                self.redis_client.set(temp_data.get('gameId'), json.dumps(temp_data), ex=70)
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))

        # epl update
        try:
            for temp_data in epl_data:
                self.redis_client.set(temp_data.get('gameId'), json.dumps(temp_data), ex=70)
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))

        # lck update
        try:
            for temp_data in lck_data:
                self.redis_client.set(temp_data.get('gameId'), json.dumps(temp_data), ex=70)
        except Exception as e:
            self.log.info(e)
            self.log.info(temp_data.get('gameId'))