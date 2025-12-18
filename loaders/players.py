"""Load NBA players data."""
import polars as pl
from nba_api.stats.static import players
from loaders.base import BaseLoader

class PlayersLoader(BaseLoader):
    """Loader for NBA players."""
    
    def __init__(self):
        super().__init__()
        self.table_name = "raw_players"
    
    def fetch_data(self) -> pl.DataFrame:
        """Fetch all NBA players from the API."""
        data = self.api_call(players.get_players)
        return pl.DataFrame(data)
    
    def get_create_table_sql(self) -> str:
        """Create raw players table."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT PRIMARY KEY,
            full_name VARCHAR(255),
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            is_active BOOLEAN,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_last_name (last_name),
            INDEX idx_active (is_active)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

def load_players():
    """Entry point for loading players."""
    loader = PlayersLoader()
    loader.run()