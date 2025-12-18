"""Load player common info data."""
import time
import polars as pl
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.static import players
from loaders.base import BaseLoader
from db import get_db
from config import VERBOSE, COOLDOWN_INTERVAL, COOLDOWN_TIME


class PlayerInfoLoader(BaseLoader):
    """Loader for player common info (biographical/roster data)."""

    def __init__(self, limit_players=None, cooldown_interval=None, resume=False, active_only=False):
        """
        Initialize player info loader.

        Args:
            limit_players: Limit to N players (for testing)
            cooldown_interval: Take a break after every N players
            resume: Skip players already in database
            active_only: Only fetch for active players
        """
        super().__init__()
        self.table_name = "raw_player_common_info"
        self.limit_players = limit_players
        self.cooldown_interval = cooldown_interval or COOLDOWN_INTERVAL
        self.resume = resume
        self.active_only = active_only
        self._loaded_player_ids = set()
        self._active_player_ids = set()

    def _get_loaded_player_ids(self) -> set:
        """Get player IDs already in the database."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT PERSON_ID FROM {self.table_name}")
                rows = cursor.fetchall()
                loaded_ids = {row[0] for row in rows}
                print(f"✓ Found {len(loaded_ids)} players already in database")
                return loaded_ids
        except Exception as e:
            print(f"⚠️  Could not check existing players: {e}")
            return set()

    def _get_active_player_ids(self) -> set:
        """Get active player IDs from raw_players table."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT id FROM raw_players WHERE is_active = 1")
                rows = cursor.fetchall()
                active_ids = {row[0] for row in rows}
                print(f"✓ Found {len(active_ids)} active players in database")
                return active_ids
        except Exception as e:
            print(f"⚠️  Could not get active players: {e}")
            return set()

    def _fetch_player_info(self, player_id: int, player_name: str) -> pl.DataFrame | None:
        """
        Fetch common info for a single player.

        Args:
            player_id: NBA player ID
            player_name: Player's full name (for logging)

        Returns:
            Polars DataFrame with player info, or None if no data
        """
        if self.check_shutdown():
            return None

        try:
            player_info = self.api_call(
                commonplayerinfo.CommonPlayerInfo,
                player_id=player_id,
                timeout=15
            )

            if player_info is None:
                if VERBOSE:
                    print(f"    ⚠️  {player_name}: No data (API call failed)")
                return None

            df_pandas = player_info.common_player_info.get_data_frame()

            if not df_pandas.empty:
                # Convert to polars
                df_polars = pl.from_pandas(df_pandas)

                # Store partial data for graceful shutdown
                self._partial_data.append(df_polars)

                return df_polars

        except Exception as e:
            if VERBOSE:
                print(f"    ⚠️  {player_name}: Failed ({e})")

        return None

    def fetch_data(self) -> pl.DataFrame:
        """Fetch common info for all players."""
        # Get all players
        all_players = players.get_players()

        # If resuming, get already loaded player IDs
        if self.resume:
            self._loaded_player_ids = self._get_loaded_player_ids()

        # If active only, get active player IDs
        if self.active_only:
            self._active_player_ids = self._get_active_player_ids()
            if not self._active_player_ids:
                print("⚠️  No active players found. Run players loader first.")

        # Limit for testing
        if self.limit_players:
            all_players = all_players[:self.limit_players]
            print(f"ℹ️  Limited to {self.limit_players} players for testing")

        print(f"ℹ️  Fetching common info for {len(all_players)} players")
        if self.resume:
            print(f"   Resume mode: Will skip players already in database")
        if self.active_only:
            print(f"   Active only: Will only fetch for active players ({len(self._active_player_ids)} players)")
        print(f"   Cool-down: Every {self.cooldown_interval} players")

        all_info = []
        player_count = 0
        skipped_count = 0
        skipped_inactive = 0
        fetched_count = 0

        for player in all_players:
            # Check for shutdown
            if self.check_shutdown():
                print(f"\n⚠️  Shutdown requested - stopping at player {player_count}/{len(all_players)}")
                break

            player_id = player['id']
            player_name = player['full_name']
            player_count += 1

            # Skip if active_only and player is not active
            if self.active_only and player_id not in self._active_player_ids:
                skipped_inactive += 1
                continue

            # Skip if resuming and player already loaded
            if self.resume and player_id in self._loaded_player_ids:
                if VERBOSE:
                    print(f"  [{player_count}/{len(all_players)}] {player_name} - skipped (already loaded)")
                skipped_count += 1
                continue

            if VERBOSE:
                print(f"  [{player_count}/{len(all_players)}] {player_name}...")

            # Cool down period (based on fetched count, not total count)
            fetched_count += 1
            if fetched_count > 0 and fetched_count % self.cooldown_interval == 0:
                if VERBOSE:
                    print(f"   💤 Taking a {COOLDOWN_TIME}s cool-down break after {fetched_count} API calls...")
                time.sleep(COOLDOWN_TIME)

            # Fetch player info
            df = self._fetch_player_info(player_id, player_name)
            if df is not None:
                all_info.append(df)

        # Summary
        print(f"\n📊 Summary:")
        print(f"   Total players: {len(all_players)}")
        if self.active_only:
            print(f"   Skipped (inactive): {skipped_inactive}")
        print(f"   Skipped (already loaded): {skipped_count}")
        print(f"   Fetched this run: {len(all_info)}")

        # Combine all info
        if not all_info:
            print("⚠️  No new player info to load")
            return pl.DataFrame()

        combined = pl.concat(all_info)
        print(f"✓ Combined info for {len(combined)} players")

        return combined

    def get_create_table_sql(self) -> str:
        """Create raw player common info table."""
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            PERSON_ID INT PRIMARY KEY,
            FIRST_NAME VARCHAR(255),
            LAST_NAME VARCHAR(255),
            DISPLAY_FIRST_LAST VARCHAR(255),
            DISPLAY_LAST_COMMA_FIRST VARCHAR(255),
            DISPLAY_FI_LAST VARCHAR(255),
            PLAYER_SLUG VARCHAR(255),
            BIRTHDATE VARCHAR(255),
            SCHOOL VARCHAR(255),
            COUNTRY VARCHAR(255),
            LAST_AFFILIATION VARCHAR(255),
            HEIGHT VARCHAR(255),
            WEIGHT VARCHAR(255),
            SEASON_EXP INT,
            JERSEY VARCHAR(255),
            POSITION VARCHAR(255),
            ROSTERSTATUS VARCHAR(255),
            GAMES_PLAYED_CURRENT_SEASON_FLAG VARCHAR(255),
            TEAM_ID INT,
            TEAM_NAME VARCHAR(255),
            TEAM_ABBREVIATION VARCHAR(255),
            TEAM_CODE VARCHAR(255),
            TEAM_CITY VARCHAR(255),
            PLAYERCODE VARCHAR(255),
            FROM_YEAR INT,
            TO_YEAR INT,
            DLEAGUE_FLAG VARCHAR(255),
            NBA_FLAG VARCHAR(255),
            GAMES_PLAYED_FLAG VARCHAR(255),
            DRAFT_YEAR VARCHAR(255),
            DRAFT_ROUND VARCHAR(255),
            DRAFT_NUMBER VARCHAR(255),
            GREATEST_75_FLAG VARCHAR(255),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_team (TEAM_ID),
            INDEX idx_country (COUNTRY),
            INDEX idx_position (POSITION),
            INDEX idx_draft_year (DRAFT_YEAR)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """


def load_player_info(limit_players=None, cooldown_interval=None, resume=False, active_only=False):
    """
    Entry point for loading player common info.

    Args:
        limit_players: Limit to N players (for testing)
        cooldown_interval: Take a break after every N players
        resume: Skip players already in database
        active_only: Only fetch for active players

    Returns:
        PlayerInfoLoader instance (for retry logic)
    """
    loader = PlayerInfoLoader(
        limit_players=limit_players,
        cooldown_interval=cooldown_interval,
        resume=resume,
        active_only=active_only
    )
    loader.run()
    return loader
