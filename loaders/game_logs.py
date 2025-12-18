"""Load player game logs data."""
import time
import math
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players
from loaders.base import BaseLoader
from db import get_db
from config import START_SEASON, END_SEASON, VERBOSE, COOLDOWN_TIME


class GameLogsLoader(BaseLoader):
    """Loader for player game logs."""

    def __init__(
        self,
        start_season=None,
        end_season=None,
        limit_players=None,
        cooldown_interval=10,
        max_workers=3,
        use_career_stats=True,
        resume=False,
        active_only=False,
        start_player=0
    ):
        super().__init__()
        self.table_name = "raw_player_game_logs"
        self.start_season = start_season or START_SEASON
        self.end_season = end_season or END_SEASON
        self.limit_players = limit_players
        self.cooldown_interval = cooldown_interval
        self.max_workers = max_workers
        self.use_career_stats = use_career_stats
        self.resume = resume
        self.active_only = active_only
        self.start_player = start_player
        self._lock = Lock()
        self._player_seasons = {}
        self._loaded_player_seasons = {}
        self._active_player_ids = set()

    def _get_loaded_player_seasons(self) -> dict:
        """Get player IDs and their seasons already in the game logs database."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT Player_ID, SEASON_ID 
                    FROM {self.table_name}
                    GROUP BY Player_ID, SEASON_ID
                """)
                rows = cursor.fetchall()

                loaded = {}
                for player_id, season_id in rows:
                    if player_id not in loaded:
                        loaded[player_id] = set()
                    loaded[player_id].add(season_id)

                total_combinations = sum(len(seasons) for seasons in loaded.values())
                print(f"✓ Found {len(loaded)} players with {total_combinations} player-season combinations in game logs")
                return loaded
        except Exception as e:
            print(f"⚠️  Could not check existing game logs: {e}")
            return {}

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

    def _load_player_seasons_from_career_stats(self) -> dict:
        """Load seasons for each player from raw_player_career_stats."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT PLAYER_ID, SEASON_ID 
                    FROM raw_player_career_stats
                    WHERE LEAGUE_ID = '00'
                    GROUP BY PLAYER_ID, SEASON_ID
                """)
                rows = cursor.fetchall()

                player_seasons = {}
                for player_id, season_id in rows:
                    if player_id not in player_seasons:
                        player_seasons[player_id] = set()
                    player_seasons[player_id].add(season_id)

                total_seasons = sum(len(seasons) for seasons in player_seasons.values())
                print(f"✓ Loaded {total_seasons} player-season combinations from raw_player_career_stats")
                print(f"   ({len(player_seasons)} unique players)")
                return player_seasons

        except Exception as e:
            print(f"⚠️  Could not load player seasons from career stats: {e}")
            print("   Make sure to run player_career loader first!")
            return {}

    def _get_seasons_for_player(self, player_id: int) -> list[str]:
        """Get the list of seasons to fetch for a specific player."""
        min_season_year = self.start_season
        max_season_year = self.end_season - 1

        if self.use_career_stats and player_id in self._player_seasons:
            career_seasons = self._player_seasons[player_id]

            filtered_seasons = []
            for season in career_seasons:
                try:
                    season_year = int(season.split('-')[0])
                    if min_season_year <= season_year <= max_season_year:
                        filtered_seasons.append(season)
                except (ValueError, IndexError):
                    continue

            seasons = sorted(filtered_seasons)
        else:
            seasons = [
                f"{year}-{str(year + 1)[-2:]}"
                for year in range(self.start_season, self.end_season)
            ]

        if self.resume and player_id in self._loaded_player_seasons:
            loaded_season_ids = self._loaded_player_seasons[player_id]
            seasons_to_fetch = []
            for season in seasons:
                year = int(season.split('-')[0])
                season_id = f"2{year}"
                if season_id not in loaded_season_ids:
                    seasons_to_fetch.append(season)
            return seasons_to_fetch

        return seasons

    def _normalize_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize DataFrame columns to consistent types."""
        schema = {
            'SEASON_ID': pl.String, 'Player_ID': pl.Int64, 'Game_ID': pl.String,
            'GAME_DATE': pl.String, 'MATCHUP': pl.String, 'WL': pl.String, 'MIN': pl.String,
            'FGM': pl.Float64, 'FGA': pl.Float64, 'FG_PCT': pl.Float64,
            'FG3M': pl.Float64, 'FG3A': pl.Float64, 'FG3_PCT': pl.Float64,
            'FTM': pl.Float64, 'FTA': pl.Float64, 'FT_PCT': pl.Float64,
            'OREB': pl.Float64, 'DREB': pl.Float64, 'REB': pl.Float64,
            'AST': pl.Float64, 'STL': pl.Float64, 'BLK': pl.Float64,
            'TOV': pl.Float64, 'PF': pl.Float64, 'PTS': pl.Float64,
            'PLUS_MINUS': pl.Float64, 'VIDEO_AVAILABLE': pl.Float64,
        }

        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df = df.with_columns(pl.col(col).cast(dtype))
                except Exception:
                    df = df.with_columns(pl.col(col).cast(pl.String))

        return df

    def _clean_row_for_mysql(self, row: list) -> list:
        """Clean a row for MySQL insertion."""
        cleaned = []
        for val in row:
            if val is None:
                cleaned.append(None)
            elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                cleaned.append(None)
            elif isinstance(val, str) and val.lower() == 'nan':
                cleaned.append(None)
            else:
                cleaned.append(val)
        return cleaned

    def _fetch_season_for_player(self, player_id: int, player_name: str, season: str) -> pl.DataFrame | None:
        """Fetch game logs for a single player-season combination."""
        if self.check_shutdown():
            return None

        try:
            gamelog = self.api_call(
                playergamelog.PlayerGameLog,
                player_id=player_id,
                season=season,
                timeout=15
            )

            if gamelog is None:
                return None

            df_pandas = gamelog.get_data_frames()[0]

            if not df_pandas.empty:
                df_pandas = df_pandas.infer_objects(copy=False)
                df_polars = pl.from_pandas(df_pandas)
                df_polars = self._normalize_dataframe(df_polars)

                with self._lock:
                    self._partial_data.append(df_polars)

                if VERBOSE:
                    with self._lock:
                        print(f"    ✓ {season}: {len(df_polars)} games")

                return df_polars

        except Exception as e:
            if VERBOSE:
                with self._lock:
                    print(f"    ⚠️  {season}: No data ({e})")

        return None

    def _fetch_seasons_parallel(self, player_id: int, player_name: str, seasons: list[str]) -> list[pl.DataFrame]:
        """Fetch multiple seasons for a player in parallel."""
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_season = {
                executor.submit(self._fetch_season_for_player, player_id, player_name, season): season
                for season in seasons
            }

            for future in as_completed(future_to_season):
                if self.check_shutdown():
                    for f in future_to_season:
                        f.cancel()
                    break

                try:
                    df = future.result()
                    if df is not None:
                        results.append(df)
                except Exception as e:
                    if VERBOSE:
                        with self._lock:
                            print(f"    ❌ Exception in parallel fetch ({e})")

        return results

    def fetch_data(self) -> pl.DataFrame:
        """Fetch game logs for all players."""
        if self.use_career_stats:
            self._player_seasons = self._load_player_seasons_from_career_stats()
            if not self._player_seasons:
                print("⚠️  No career stats found. Run player_career loader first, or use --no-career-stats flag")

        if self.resume:
            self._loaded_player_seasons = self._get_loaded_player_seasons()

        if self.active_only:
            self._active_player_ids = self._get_active_player_ids()
            if not self._active_player_ids:
                print("⚠️  No active players found. Run players loader first.")

        all_players = players.get_players()
        total_players = len(all_players)

        if self.start_player > 0:
            all_players = all_players[self.start_player:]
            print(f"ℹ️  Starting from player {self.start_player} (skipped first {self.start_player})")

        if self.limit_players:
            all_players = all_players[:self.limit_players]
            print(f"ℹ️  Limited to {self.limit_players} players for testing")

        print(f"ℹ️  Season bounds: {self.start_season} to {self.end_season}")
        if self.use_career_stats:
            print(f"   (Using actual seasons from raw_player_career_stats)")
        if self.resume:
            print(f"   Resume mode: Will skip player-seasons already in database")
        if self.active_only:
            print(f"   Active only: Will only fetch for active players ({len(self._active_player_ids)} players)")

        print(f"ℹ️  Fetching data for {len(all_players)} players")
        print(f"   Parallel workers: {self.max_workers}")
        print(f"   Cool-down: Every {self.cooldown_interval} players")

        all_logs = []
        player_count = self.start_player
        skipped_count = 0
        skipped_inactive = 0
        skipped_no_career = 0
        fetched_count = 0

        for player in all_players:
            if self.check_shutdown():
                print(f"\n⚠️  Shutdown requested - stopping at player {player_count}/{total_players}")
                break

            player_id = player['id']
            player_name = player['full_name']
            player_count += 1

            if self.active_only and player_id not in self._active_player_ids:
                skipped_inactive += 1
                continue

            if self.use_career_stats and player_id not in self._player_seasons:
                skipped_no_career += 1
                continue

            seasons = self._get_seasons_for_player(player_id)

            if not seasons:
                skipped_count += 1
                continue

            if VERBOSE:
                if len(seasons) == 1:
                    print(f"  [{player_count}/{total_players}] {player_name} ({seasons[0]})...")
                else:
                    print(f"  [{player_count}/{total_players}] {player_name} ({len(seasons)} seasons)...")

            fetched_count += 1
            if fetched_count > 0 and fetched_count % self.cooldown_interval == 0:
                if VERBOSE:
                    print(f"   💤 Taking a {COOLDOWN_TIME}s cool-down break...")
                time.sleep(COOLDOWN_TIME)

            player_logs = self._fetch_seasons_parallel(player_id, player_name, seasons)
            all_logs.extend(player_logs)

        print(f"\n📊 Summary:")
        print(f"   Total players: {total_players}")
        if self.start_player > 0:
            print(f"   Started from: {self.start_player}")
        if self.active_only:
            print(f"   Skipped (inactive): {skipped_inactive}")
        if self.use_career_stats:
            print(f"   Skipped (no career stats): {skipped_no_career}")
        print(f"   Skipped (no seasons in range): {skipped_count}")
        print(f"   Players fetched: {fetched_count}")

        if not all_logs:
            print("⚠️  No new game logs to load")
            return pl.DataFrame()

        try:
            combined = pl.concat(all_logs, how="diagonal")
        except Exception:
            combined = pl.concat(all_logs, how="diagonal_relaxed")

        return combined

    def insert_data(self, df: pl.DataFrame):
        """Insert DataFrame into database."""
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
                        try:
                            cursor.execute(sql, row)
                            total_inserted += 1
                        except Exception:
                            pass

        print(f"✓ Inserted {total_inserted} rows into {self.table_name}")

    def get_create_table_sql(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            SEASON_ID VARCHAR(10),
            Player_ID INT,
            Game_ID VARCHAR(15),
            GAME_DATE VARCHAR(20),
            MATCHUP VARCHAR(20),
            WL VARCHAR(1),
            MIN VARCHAR(10),
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
            PLUS_MINUS INT,
            VIDEO_AVAILABLE INT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (Game_ID, Player_ID),
            INDEX idx_player (Player_ID),
            INDEX idx_season (SEASON_ID),
            INDEX idx_date (GAME_DATE)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """


def load_game_logs(
    start_season=None,
    end_season=None,
    limit_players=None,
    cooldown_interval=None,
    max_workers=3,
    use_career_stats=True,
    resume=False,
    active_only=False,
    start_player=0
):
    """Entry point for loading game logs."""
    from config import COOLDOWN_INTERVAL

    loader = GameLogsLoader(
        start_season=start_season,
        end_season=end_season,
        limit_players=limit_players,
        cooldown_interval=cooldown_interval if cooldown_interval is not None else COOLDOWN_INTERVAL,
        max_workers=max_workers,
        use_career_stats=use_career_stats,
        resume=resume,
        active_only=active_only,
        start_player=start_player
    )
    loader.run()
    return loader