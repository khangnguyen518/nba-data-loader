"""Load player career stats data."""
import time
import math
import polars as pl
from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
from loaders.base import BaseLoader
from db import get_db
from config import VERBOSE, COOLDOWN_INTERVAL, COOLDOWN_TIME


class PlayerCareerLoader(BaseLoader):
    """Loader for player career stats (season-by-season totals)."""

    def __init__(self, limit_players=None, cooldown_interval=None, resume=False, active_only=False):
        super().__init__()
        self.table_name = "raw_player_career_stats"
        self.limit_players = limit_players
        self.cooldown_interval = cooldown_interval or COOLDOWN_INTERVAL
        self.resume = resume
        self.active_only = active_only
        self._loaded_player_ids = set()
        self._active_player_ids = set()
        self._existing_gp = {}

    def _get_existing_gp(self) -> dict:
        """Get existing GP values from database. Returns {(player_id, season_id, team_id): gp}"""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT PLAYER_ID, SEASON_ID, TEAM_ID, GP FROM {self.table_name}")
                rows = cursor.fetchall()
                gp_dict = {(row[0], row[1], row[2]): row[3] for row in rows}
                print(f"✓ Loaded {len(gp_dict)} existing career stat records")
                return gp_dict
        except Exception as e:
            print(f"⚠️  Could not load existing GP values: {e}")
            return {}

    def _get_loaded_player_ids(self) -> set:
        """Get player IDs already in the database."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SELECT DISTINCT PLAYER_ID FROM {self.table_name}")
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

    def _normalize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize DataFrame columns to consistent types."""
        schema = {
            'PLAYER_ID': pl.Int64, 'SEASON_ID': pl.String, 'LEAGUE_ID': pl.String,
            'TEAM_ID': pl.Int64, 'TEAM_ABBREVIATION': pl.String, 'PLAYER_AGE': pl.Float64,
            'GP': pl.Int64, 'GS': pl.Int64, 'MIN': pl.Float64,
            'FGM': pl.Float64, 'FGA': pl.Float64, 'FG_PCT': pl.Float64,
            'FG3M': pl.Float64, 'FG3A': pl.Float64, 'FG3_PCT': pl.Float64,
            'FTM': pl.Float64, 'FTA': pl.Float64, 'FT_PCT': pl.Float64,
            'OREB': pl.Float64, 'DREB': pl.Float64, 'REB': pl.Float64,
            'AST': pl.Float64, 'STL': pl.Float64, 'BLK': pl.Float64,
            'TOV': pl.Float64, 'PF': pl.Float64, 'PTS': pl.Float64,
        }

        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df = df.with_columns(pl.col(col).cast(dtype))
                except Exception:
                    df = df.with_columns(pl.col(col).cast(pl.String))

        return df

    def _clean_row_for_mysql(self, row: list) -> list:
        """Clean a row for MySQL insertion by converting NaN/inf to None."""
        cleaned = []
        for val in row:
            if val is None:
                cleaned.append(None)
            elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                cleaned.append(None)
            else:
                cleaned.append(val)
        return cleaned

    def _fetch_player_career(self, player_id: int, player_name: str) -> pl.DataFrame | None:
        """Fetch career stats for a single player."""
        if self.check_shutdown():
            return None

        try:
            career = self.api_call(
                playercareerstats.PlayerCareerStats,
                player_id=player_id,
                timeout=15
            )

            if career is None:
                if VERBOSE:
                    print(f"    ⚠️  {player_name}: No data (API call failed)")
                return None

            try:
                df_pandas = career.get_data_frames()[0]
            except (KeyError, IndexError):
                if VERBOSE:
                    print(f"    ⚠️  {player_name}: No career stats available")
                return None

            if not df_pandas.empty:
                df_polars = pl.from_pandas(df_pandas)
                df_polars = self._normalize_dataframe(df_polars)
                self._partial_data.append(df_polars)

                if VERBOSE:
                    print(f"    ✓ {len(df_polars)} seasons")

                return df_polars
            else:
                if VERBOSE:
                    print(f"    ⚠️  {player_name}: Empty career stats")
                return None

        except Exception as e:
            if 'resultSet' in str(e).lower():
                if VERBOSE:
                    print(f"    ⚠️  {player_name}: No NBA career stats")
                return None
            if VERBOSE:
                print(f"    ⚠️  {player_name}: Failed ({e})")

        return None

    def _filter_changed_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """Filter to only rows where GP changed or new records."""
        if df.is_empty() or not self._existing_gp:
            return df

        rows_to_keep = []
        
        for row in df.iter_rows(named=True):
            player_id = row.get('PLAYER_ID')
            season_id = row.get('SEASON_ID')
            team_id = row.get('TEAM_ID')
            gp = row.get('GP')
            
            key = (player_id, season_id, team_id)
            
            if key not in self._existing_gp:
                rows_to_keep.append(True)
            elif self._existing_gp[key] != gp:
                rows_to_keep.append(True)
            else:
                rows_to_keep.append(False)
        
        filtered = df.filter(pl.Series(rows_to_keep))
        
        skipped = len(df) - len(filtered)
        if skipped > 0:
            print(f"✓ Skipped {skipped} unchanged records, keeping {len(filtered)} new/updated")
        
        return filtered

    def fetch_data(self) -> pl.DataFrame:
        """Fetch career stats for all players."""
        all_players = players.get_players()

        if self.resume:
            self._loaded_player_ids = self._get_loaded_player_ids()

        if self.active_only:
            self._active_player_ids = self._get_active_player_ids()
            if not self._active_player_ids:
                print("⚠️  No active players found. Run players loader first.")

        self._existing_gp = self._get_existing_gp()

        if self.limit_players:
            all_players = all_players[:self.limit_players]
            print(f"ℹ️  Limited to {self.limit_players} players for testing")

        print(f"ℹ️  Fetching career stats for {len(all_players)} players")
        if self.resume:
            print(f"   Resume mode: Will skip players already in database")
        if self.active_only:
            print(f"   Active only: Will only fetch for active players ({len(self._active_player_ids)} players)")
        print(f"   Cool-down: Every {self.cooldown_interval} players")

        all_careers = []
        player_count = 0
        skipped_count = 0
        skipped_inactive = 0
        fetched_count = 0

        for player in all_players:
            if self.check_shutdown():
                print(f"\n⚠️  Shutdown requested - stopping at player {player_count}/{len(all_players)}")
                break

            player_id = player['id']
            player_name = player['full_name']
            player_count += 1

            if self.active_only and player_id not in self._active_player_ids:
                skipped_inactive += 1
                continue

            if self.resume and player_id in self._loaded_player_ids:
                if VERBOSE:
                    print(f"  [{player_count}/{len(all_players)}] {player_name} - skipped (already loaded)")
                skipped_count += 1
                continue

            if VERBOSE:
                print(f"  [{player_count}/{len(all_players)}] {player_name}...")

            fetched_count += 1
            if fetched_count > 0 and fetched_count % self.cooldown_interval == 0:
                if VERBOSE:
                    print(f"   💤 Taking a {COOLDOWN_TIME}s cool-down break after {fetched_count} API calls...")
                time.sleep(COOLDOWN_TIME)

            df = self._fetch_player_career(player_id, player_name)
            if df is not None:
                all_careers.append(df)

        print(f"\n📊 Summary:")
        print(f"   Total players: {len(all_players)}")
        if self.active_only:
            print(f"   Skipped (inactive): {skipped_inactive}")
        print(f"   Skipped (already loaded): {skipped_count}")
        print(f"   Fetched this run: {len(all_careers)}")

        if not all_careers:
            print("⚠️  No new player career stats to load")
            return pl.DataFrame()

        try:
            combined = pl.concat(all_careers, how="diagonal")
        except Exception:
            combined = pl.concat(all_careers, how="diagonal_relaxed")

        print(f"✓ Combined career stats: {len(combined)} player-seasons")

        combined = self._filter_changed_rows(combined)

        return combined

    def insert_data(self, df: pl.DataFrame):
        """Insert DataFrame into database, handling NaN values."""
        from db import get_db
        from config import BATCH_SIZE

        if df.is_empty():
            print(f"⚠️  No data to insert into {self.table_name}")
            return

        columns = df.columns
        placeholders = ", ".join(["%s"] * len(columns))
        cols_str = ", ".join(columns)

        sql = f"REPLACE INTO {self.table_name} ({cols_str}) VALUES ({placeholders})"
        rows = [self._clean_row_for_mysql(row) for row in df.to_numpy().tolist()]

        total_inserted = 0

        with get_db() as conn:
            cursor = conn.cursor()

            for i in range(0, len(rows), BATCH_SIZE):
                if self._shutdown_requested and not self._is_cleaning_up:
                    print(f"\n⚠️  Shutdown requested - stopping insert")
                    break

                batch = rows[i:i + BATCH_SIZE]

                try:
                    cursor.executemany(sql, batch)
                    total_inserted += len(batch)
                    if VERBOSE:
                        print(f"  ✓ Inserted batch: {len(batch)} rows (total: {total_inserted}/{len(rows)})")
                except Exception as e:
                    print(f"❌ Batch insert failed: {e}")
                    for row in batch:
                        if self._shutdown_requested and not self._is_cleaning_up:
                            break
                        try:
                            cursor.execute(sql, row)
                            total_inserted += 1
                        except Exception as row_error:
                            print(f"⚠️  Failed to insert row: {row_error}")

        print(f"✓ Inserted {total_inserted} rows into {self.table_name}")

    def get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            PLAYER_ID INT,
            SEASON_ID VARCHAR(255),
            LEAGUE_ID VARCHAR(255),
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR(255),
            PLAYER_AGE FLOAT,
            GP INT,
            GS INT,
            MIN FLOAT,
            FGM FLOAT,
            FGA FLOAT,
            FG_PCT FLOAT,
            FG3M FLOAT,
            FG3A FLOAT,
            FG3_PCT FLOAT,
            FTM FLOAT,
            FTA FLOAT,
            FT_PCT FLOAT,
            OREB FLOAT,
            DREB FLOAT,
            REB FLOAT,
            AST FLOAT,
            STL FLOAT,
            BLK FLOAT,
            TOV FLOAT,
            PF FLOAT,
            PTS FLOAT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (PLAYER_ID, SEASON_ID, TEAM_ID),
            INDEX idx_player (PLAYER_ID),
            INDEX idx_season (SEASON_ID),
            INDEX idx_team (TEAM_ID)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """


def load_player_career(limit_players=None, cooldown_interval=None, resume=False, active_only=False):
    """Entry point for loading player career stats."""
    loader = PlayerCareerLoader(
        limit_players=limit_players,
        cooldown_interval=cooldown_interval,
        resume=resume,
        active_only=active_only
    )
    loader.run()
    return loader