"""Load team game logs data."""
import time
import polars as pl
from nba_api.stats.endpoints import teamgamelog
from nba_api.stats.static import teams
from loaders.base import BaseLoader
from config import START_SEASON, END_SEASON, VERBOSE, COOLDOWN_TIME

class TeamGameLogsLoader(BaseLoader):
    """Loader for team game logs."""
    
    def __init__(self, start_season=None, end_season=None):
        super().__init__()
        self.table_name = "raw_team_game_logs"
        self.start_season = start_season or START_SEASON
        self.end_season = end_season or END_SEASON
    
    def fetch_data(self) -> pl.DataFrame:
        """Fetch team game logs for all teams."""
        all_teams = teams.get_teams()
        seasons = [
            f"{year}-{str(year + 1)[-2:]}" 
            for year in range(self.start_season, self.end_season)
        ]
        
        all_games = []
        
        for team in all_teams:
            team_id = team['id']
            team_name = team['full_name']
            
            if VERBOSE:
                print(f"  Fetching {team_name}...")
            
            for season in seasons:
                try:
                    gamelog = self.api_call(
                        teamgamelog.TeamGameLog,
                        team_id=team_id,
                        season=season,
                        timeout=30
                    )
                    
                    if gamelog is None:
                        continue
                    
                    df_pandas = gamelog.get_data_frames()[0]
                    
                    if not df_pandas.empty:
                        df_polars = pl.from_pandas(df_pandas)
                        all_games.append(df_polars)
                        
                        if VERBOSE:
                            print(f"    ✓ {season}: {len(df_polars)} games")
                
                except Exception as e:
                    if VERBOSE:
                        print(f"    ⚠️  {season}: Failed ({e})")
            
            # Cooldown between teams
            time.sleep(COOLDOWN_TIME)
        
        if not all_games:
            return pl.DataFrame()
        
        return pl.concat(all_games)
    
    def get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            Team_ID INT,
            Game_ID VARCHAR(15),
            GAME_DATE VARCHAR(20),
            MATCHUP VARCHAR(20),
            WL VARCHAR(1),
            W INT,
            L INT,
            W_PCT FLOAT,
            MIN INT,
            FGM INT,
            FGA INT,
            FG_PCT FLOAT,
            FG3M INT,
            FG3A INT,
            FG3_PCT FLOAT,
            FTM INT,
            FTA INT,
            FT_PCT FLOAT,
            OREB INT,
            DREB INT,
            REB INT,
            AST INT,
            STL INT,
            BLK INT,
            TOV INT,
            PF INT,
            PTS INT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (Game_ID, Team_ID),
            INDEX idx_team (Team_ID),
            INDEX idx_date (GAME_DATE)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

def load_team_game_logs(start_season=None, end_season=None):
    """Entry point for loading team game logs."""
    loader = TeamGameLogsLoader(start_season, end_season)
    loader.run()