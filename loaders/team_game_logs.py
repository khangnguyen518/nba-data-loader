"""Load team game logs data."""
import polars as pl
from nba_api.stats.endpoints import leaguegamefinder
from loaders.base import BaseLoader
from config import START_SEASON, END_SEASON, VERBOSE

class TeamGameLogsLoader(BaseLoader):
    """Loader for team game logs."""
    
    def __init__(self, start_season=None, end_season=None):
        super().__init__()
        self.table_name = "raw_team_game_logs"
        self.start_season = start_season or START_SEASON
        self.end_season = end_season or END_SEASON
    
    def fetch_data(self) -> pl.DataFrame:
        """Fetch team game logs."""
        seasons = [
            f"{year}-{str(year + 1)[-2:]}" 
            for year in range(self.start_season, self.end_season)
        ]
        
        all_games = []
        
        for season in seasons:
            if VERBOSE:
                print(f"  Fetching {season}...")
            
            try:
                gamefinder = self.api_call(
                    leaguegamefinder.LeagueGameFinder,
                    season_nullable=season,
                    league_id_nullable='00',
                    season_type_nullable='Regular Season'
                )
                
                df_pandas = gamefinder.get_data_frames()[0]
                
                if not df_pandas.empty:
                    df_polars = pl.from_pandas(df_pandas)
                    all_games.append(df_polars)
                    
                    if VERBOSE:
                        print(f"    ✓ {len(df_polars)} games")
            
            except Exception as e:
                if VERBOSE:
                    print(f"    ⚠️  Failed: {e}")
        
        if not all_games:
            return pl.DataFrame()
        
        return pl.concat(all_games)
    
    def get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            SEASON_ID VARCHAR(10),
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR(10),
            TEAM_NAME VARCHAR(100),
            GAME_ID VARCHAR(15),
            GAME_DATE VARCHAR(20),
            MATCHUP VARCHAR(20),
            WL VARCHAR(1),
            MIN VARCHAR(10),
            PTS INT,
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
            PLUS_MINUS INT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (GAME_ID, TEAM_ID),
            INDEX idx_team (TEAM_ID),
            INDEX idx_season (SEASON_ID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

def load_team_game_logs(start_season=None, end_season=None):
    """Entry point for loading team game logs."""
    loader = TeamGameLogsLoader(start_season, end_season)
    loader.run()