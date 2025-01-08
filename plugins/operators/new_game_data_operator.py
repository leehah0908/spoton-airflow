from airflow.models.baseoperator import BaseOperator
from hooks.custom_mysql_hook import MySQLAPIHook

class NewGameDataOperator(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mysql_conn_id = 'conn-db-mysql-custom'

    # 오퍼레이터 실행
    def execute(self, context):
        import pandas as pd
        import requests
        from datetime import timedelta, datetime
        from dateutil.relativedelta import relativedelta
        import calendar
        import json

        # 기본 변수 세팅
        api_urls = {
            'kbo' :     f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cbaseball&upperCategoryId=kbaseball&categoryId=kbo&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'mlb' :     f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cbaseball&upperCategoryId=wbaseball&categoryId=mlb&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'kleague' : f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2CmatchRound%2CroundTournamentInfo%2CphaseCode%2CgroupName%2Cleg%2ChasPtSore%2ChomePtScore%2CawayPtScore%2Cleague%2CleagueName%2CaggregateWinner%2CneutralGround%2Cpostponed&upperCategoryId=kfootball&categoryId=kleague&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'epl' :     f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2CmatchRound%2CroundTournamentInfo%2CphaseCode%2CgroupName%2Cleg%2ChasPtSore%2ChomePtScore%2CawayPtScore%2Cleague%2CleagueName%2CaggregateWinner%2CneutralGround%2Cpostponed&upperCategoryId=wfootball&categoryId=epl&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'kbl' :     f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cconference&superCategoryId=basketball&categoryId=kbl&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'nba' :     f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cconference&superCategoryId=basketball&categoryId=nba&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'kovo' :    f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cround%2CgroupName&superCategoryId=volleyball&categoryId=kovo&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'wkovo' :   f"https://api-gw.sports.naver.com/schedule/games?fields=basic%2Cschedule%2Cround%2CgroupName&superCategoryId=volleyball&categoryId=wkovo&fromDate=start_date&toDate=end_date&roundCodes=&size=1000",
            'lck' :     f"https://esports-api.game.naver.com/service/v2/schedule/month?month=start_date&topLeagueId=lck&relay=false",
        }

        custom_mysql_hook = MySQLAPIHook(self.mysql_conn_id)
        query = f"SELECT * FROM game_data"
        df = custom_mysql_hook.select_query(query)

        data_list = []

        for league in api_urls.keys():
            tmep_game_df = df[df['league'] == league]
            df_url = api_urls.get(league)

            # 가장 마지막 경기 일정의 날짜와 이후 1년의 날짜 세팅
            start = tmep_game_df['gameDate'].dt.date.max() + timedelta(days = 1)
            end = start + timedelta(days = 365)
            
            # sports
            if league != "lck":
                # 새로운 데이터가 있는지 확인
                check_url = df_url.replace("start_date", str(start)).replace("end_date", str(end))
                response = requests.get(check_url).json()

                # 최신 데이터 없음
                if response.get('result').get('gameTotalCount') == 0:
                    self.log.info(f"***** {league} 최신 데이터 없음 *****")
                    continue
                # 최신 데이터 있음 -> 수집
                else:
                    current_date = start

                    # 마지막으로 수집된 경기 이후의 1년 동안의 새로운 데이터 수집
                    while current_date <= end:
                        last_day = calendar.monthrange(current_date.year, current_date.month)[1]

                        temp_start_date = f"{current_date.year}-{current_date.month:02d}-01"
                        temp_end_date = f"{current_date.year}-{current_date.month:02d}-{last_day}"

                        url = df_url.replace("start_date", temp_start_date).replace("end_date", temp_end_date)
                        response = requests.get(url).json()

                        if response.get('result').get('gameTotalCount') == 0:
                            current_date += relativedelta(months = 1)
                            continue
                        elif response.get('result').get('gameTotalCount') > 0:
                            game_data = response.get('result').get('games')
                            
                            for i in game_data:
                                try:
                                    game_board_url = f"https://api-gw.sports.naver.com/schedule/games/{i.get('gameId')}"
                                    game_board_response = requests.get(game_board_url).json()
                                    game_board_data = game_board_response.get('result').get('game')
                                    game_board_to_json = json.dumps(game_board_data)

                                    data_list.append({
                                        'gameId' : i.get('gameId'),
                                        'gameDate' : i.get('gameDateTime'),
                                        'sports' : i.get('superCategoryId'),
                                        'league' : i.get('categoryId'),
                                        'homeTeam' : i.get('homeTeamName'),
                                        'awayTeam' : i.get('awayTeamName'),
                                        'gameBoard': game_board_to_json,
                                        'cancel' : i.get('cancel'),
                                    })
                                except Exception as e:
                                    self.log.info(e)
                                    self.log.info(i.get('gameId'))
                                    continue
                            current_date += relativedelta(months = 1)
                    
                    self.log.info(f"***** {league} 추가 수집 완료 *****")

            # esports
            elif league == "lck":
                # 새로운 데이터가 있는지 확인
                check_url = df_url.replace("start_date", str(start.strftime("%Y-%m")))
                response = requests.get(check_url).json()

                if len(response.get('content').get('matches')) == 0:
                    self.log.info(f"***** {league} 최신 데이터 없음 *****")
                    continue
                else:
                    current_date = start

                    # 마지막으로 수집된 경기 이후의 1년 동안의 새로운 데이터 수집
                    while current_date <= end:
                        temp_start_date = f"{current_date.year}-{current_date.month:02d}"

                        url = df_url.replace("start_date", temp_start_date)
                        response = requests.get(url).json()

                        if len(response.get('content').get('matches')) == 0:
                            current_date += relativedelta(months = 1)
                            continue
                        elif len(response.get('content').get('matches')) > 0:
                            game_data = response.get('content').get('matches')
                            
                            for i in game_data:
                                try:
                                    gameBoard = {
                                        'homeTeamScore' : i.get('homeScore'),
                                        'awayTeamScore' : i.get('awayScore'),
                                        'statusCode' : i.get('matchStatus'),
                                        'statusInfo' : i.get('currentMatchSet'),
                                        'stadium' : i.get('stadium')
                                        }
                                    game_board_to_json = json.dumps(gameBoard)

                                    data_list.append({
                                        'gameId' : i.get('gameId'),
                                        'gameDate' : datetime.fromtimestamp(i.get('startDate')/1000),
                                        'sports' : 'esports',
                                        'league' : i.get('topLeagueId'),
                                        'homeTeam' : i.get('homeTeam').get('name'),
                                        'awayTeam' : i.get('awayTeam').get('name'),
                                        'gameBoard': game_board_to_json,
                                        'cancel' : False,
                                    })
                                except Exception as e:
                                    self.log.info(e)
                                    self.log.info(i.get('gameId'))
                                    continue
                            current_date += relativedelta(months = 1)
                    
                    # LOL은 월별로 데이터를 수집하기 때문에 0건 수집 로직이 작동할 수 있음
                    self.log.info(f"***** {league} 추가 수집 완료 *****")

        new_data = pd.DataFrame(data_list)
        new_data['gameDate'] = pd.to_datetime(new_data['gameDate'], errors='coerce')
        save_df = new_data[~new_data['gameId'].isin(df['gameId'])] # append를 위해서 중복 제거

        self.log.info(f"{len(save_df)}건 추가 수집 완료")

        if len(save_df) != 0:
            try:
                custom_mysql_hook.append_data(save_df)
                self.log.info(f"{len(save_df)}건 insert 완료")
            except Exception as e:
                self.log.info(e)
                self.log.info("insert 실패")

        else:
            self.log.info("추가 수집 없음")