"""Load NBA teams data."""
import polars as pl
from nba_api.stats.static import teams
from loaders.base import BaseLoader

class TeamsLoader(BaseLoader):
    """Loader for NBA teams."""
    
    def __init__(self):
        super().__init__()
        self.table_name = "raw_teams"
    
    def fetch_data(self) -> pl.DataFrame:
        """Fetch all NBA teams from the API."""
        data = self.api_call(teams.get_teams)
        return pl.DataFrame(data)
    
    def get_create_table_sql(self) -> str:
        """Create raw teams table."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT PRIMARY KEY,
            full_name VARCHAR(255),
            abbreviation VARCHAR(10),
            nickname VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(50),
            year_founded INT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_abbreviation (abbreviation)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

def load_teams():
    """Entry point for loading teams."""
    loader = TeamsLoader()
    loader.run()