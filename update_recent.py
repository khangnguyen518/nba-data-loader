"""
Incremental update script for NBA data.

This script:
1. Updates career stats for active players (to get any new seasons)
2. Loads game logs for the current season

Designed to run automatically without timing out:
- Processes players in small batches
- Long delays between API calls
- Auto-saves progress for resume
- Detects consecutive timeouts and pauses to recover
"""
import os
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

from db import test_connection, get_db
from config import VERBOSE


# Progress file for auto-resume
PROGRESS_FILE = Path(__file__).parent / ".update_progress.json"

# Conservative API settings to avoid timeouts
BATCH_SIZE = 20              # Players per batch
DELAY_BETWEEN_PLAYERS = 3    # Seconds between each player
DELAY_BETWEEN_BATCHES = 60   # Seconds between batches (1 minute break)

# Timeout recovery settings
MAX_CONSECUTIVE_TIMEOUTS = 3  # After this many timeouts in a row, pause
TIMEOUT_RECOVERY_DELAY = 120  # Seconds to wait before retrying (2 minutes)


def get_current_season() -> tuple[int, str]:
    now = datetime.now()
    if now.month < 10:
        season_year = now.year - 1
    else:
        season_year = now.year
    season_string = f"{season_year}-{str(season_year + 1)[-2:]}"
    return season_year, season_string


def load_progress() -> dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_progress(data: dict):
    """Save progress to file."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(data, f)


def clear_progress():
    """Clear progress file."""
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


def get_active_player_ids() -> list:
    """Get all active player IDs from database, sorted for consistent ordering."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM raw_players WHERE is_active = 1 ORDER BY id")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Could not get active players: {e}")
        return []


def is_timeout_error(error: Exception) -> bool:
    """Check if an exception is a timeout-related error."""
    error_str = str(error).lower()
    return any(word in error_str for word in ['timeout', 'timed out', 'connection', 'read operation'])


def fetch_player_game_logs(player_id: int, season: str) -> tuple[int, bool]:
    """
    Fetch game logs for a single player.
    Returns (number of games loaded, was_timeout).
    """
    from nba_api.stats.endpoints import playergamelog
    
    try:
        time.sleep(DELAY_BETWEEN_PLAYERS)
        
        gamelog = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            timeout=30
        )
        
        df = gamelog.get_data_frames()[0]
        
        if df.empty:
            return 0, False
        
        # Insert into database
        with get_db() as conn:
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                sql = """
                    REPLACE INTO raw_player_game_logs 
                    (SEASON_ID, Player_ID, Game_ID, GAME_DATE, MATCHUP, WL, MIN,
                     FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
                     OREB, DREB, REB, AST, STL, BLK, TOV, PF, PTS, PLUS_MINUS, VIDEO_AVAILABLE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    row.get('SEASON_ID'), row.get('Player_ID'), row.get('Game_ID'),
                    row.get('GAME_DATE'), row.get('MATCHUP'), row.get('WL'), row.get('MIN'),
                    row.get('FGM'), row.get('FGA'), row.get('FG_PCT'),
                    row.get('FG3M'), row.get('FG3A'), row.get('FG3_PCT'),
                    row.get('FTM'), row.get('FTA'), row.get('FT_PCT'),
                    row.get('OREB'), row.get('DREB'), row.get('REB'),
                    row.get('AST'), row.get('STL'), row.get('BLK'),
                    row.get('TOV'), row.get('PF'), row.get('PTS'),
                    row.get('PLUS_MINUS'), row.get('VIDEO_AVAILABLE')
                ))
        
        return len(df), False
        
    except Exception as e:
        if VERBOSE:
            print(f"⚠️  Error: {e}")
        return 0, is_timeout_error(e)


def fetch_player_career(player_id: int, season: str) -> tuple[int, bool]:
    """
    Fetch career stats for a single player for current season only.
    Returns (number of records loaded, was_timeout).
    """
    from nba_api.stats.endpoints import playercareerstats
    
    try:
        time.sleep(DELAY_BETWEEN_PLAYERS)
        
        career = playercareerstats.PlayerCareerStats(
            player_id=player_id,
            timeout=30
        )
        
        df = career.get_data_frames()[0]
        
        if df.empty:
            return 0, False
        
        # Filter to current season only
        season_id = f"2{season.split('-')[0]}"  # e.g., "2024-25" -> "22024"
        df = df[df['SEASON_ID'] == season_id]
        
        if df.empty:
            return 0, False
        
        # Insert into database
        with get_db() as conn:
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                sql = """
                    REPLACE INTO raw_player_career_stats
                    (PLAYER_ID, SEASON_ID, LEAGUE_ID, TEAM_ID, TEAM_ABBREVIATION, PLAYER_AGE,
                     GP, GS, MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
                     OREB, DREB, REB, AST, STL, BLK, TOV, PF, PTS)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    row.get('PLAYER_ID'), row.get('SEASON_ID'), row.get('LEAGUE_ID'),
                    row.get('TEAM_ID'), row.get('TEAM_ABBREVIATION'), row.get('PLAYER_AGE'),
                    row.get('GP'), row.get('GS'), row.get('MIN'),
                    row.get('FGM'), row.get('FGA'), row.get('FG_PCT'),
                    row.get('FG3M'), row.get('FG3A'), row.get('FG3_PCT'),
                    row.get('FTM'), row.get('FTA'), row.get('FT_PCT'),
                    row.get('OREB'), row.get('DREB'), row.get('REB'),
                    row.get('AST'), row.get('STL'), row.get('BLK'),
                    row.get('TOV'), row.get('PF'), row.get('PTS')
                ))
        
        return len(df), False
        
    except Exception as e:
        if VERBOSE:
            print(f"⚠️  Error: {e}")
        return 0, is_timeout_error(e)


def get_team_ids() -> list:
    """Get all team IDs from database."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM raw_teams ORDER BY id")
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"❌ Could not get teams: {e}")
        return []


def fetch_team_game_logs(team_id: int, season: str) -> tuple[int, bool]:
    """
    Fetch game logs for a single team.
    Returns (number of games loaded, was_timeout).
    """
    from nba_api.stats.endpoints import teamgamelog
    
    try:
        time.sleep(DELAY_BETWEEN_PLAYERS)
        
        gamelog = teamgamelog.TeamGameLog(
            team_id=team_id,
            season=season,
            timeout=30
        )
        
        df = gamelog.get_data_frames()[0]
        
        if df.empty:
            return 0, False
        
        # Insert into database
        with get_db() as conn:
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                sql = """
                    REPLACE INTO raw_team_game_logs 
                    (Team_ID, Game_ID, GAME_DATE, MATCHUP, WL, W, L, W_PCT, MIN,
                     FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, FT_PCT,
                     OREB, DREB, REB, AST, STL, BLK, TOV, PF, PTS)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    row['Team_ID'], row['Game_ID'], row['GAME_DATE'],
                    row['MATCHUP'], row['WL'], row['W'], row['L'],
                    row['W_PCT'], row['MIN'],
                    row['FGM'], row['FGA'], row['FG_PCT'],
                    row['FG3M'], row['FG3A'], row['FG3_PCT'],
                    row['FTM'], row['FTA'], row['FT_PCT'],
                    row['OREB'], row['DREB'], row['REB'],
                    row['AST'], row['STL'], row['BLK'],
                    row['TOV'], row['PF'], row['PTS']
                ))
        
        return len(df), False
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        if VERBOSE:
            print(f"⚠️  Error: {e}")
        return 0, is_timeout_error(e)


def main(skip_career=False, resume=True):
    season_year, season_string = get_current_season()
    
    print(f"\n{'='*70}")
    print(f"  NBA INCREMENTAL UPDATE")
    print(f"  Current Season: {season_string}")
    print(f"  Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Test connection
    print("Testing database connection...")
    if not test_connection():
        print("❌ Cannot connect to database. Exiting.")
        return
    
    # Get active players
    player_ids = get_active_player_ids()
    if not player_ids:
        print("❌ No active players found. Run full load first.")
        return
    
    print(f"📋 Found {len(player_ids)} active players")
    
    # Load progress
    progress = load_progress() if resume else {}
    
    # Check if this is a continuation
    if progress.get('season') == season_string:
        start_idx = progress.get('last_completed_idx', -1) + 1
        print(f"📂 Resuming from player {start_idx + 1}/{len(player_ids)}")
    else:
        start_idx = 0
        progress = {'season': season_string, 'last_completed_idx': -1}
    
    print(f"⚙️  Settings: {BATCH_SIZE} players/batch, {DELAY_BETWEEN_PLAYERS}s delay, {TIMEOUT_RECOVERY_DELAY}s recovery")
    print()
    
    # Process players
    total_games = 0
    total_careers = 0
    consecutive_timeouts = 0
    first_timeout_idx = None  # Track where timeouts started
    
    i = start_idx
    while i < len(player_ids):
        player_id = player_ids[i]
        batch_num = i // BATCH_SIZE + 1
        
        # Batch break
        if i > start_idx and i % BATCH_SIZE == 0:
            print(f"\n💤 Batch {batch_num - 1} complete. Taking {DELAY_BETWEEN_BATCHES}s break...\n")
            time.sleep(DELAY_BETWEEN_BATCHES)
            consecutive_timeouts = 0  # Reset after batch break
        
        print(f"[{i + 1}/{len(player_ids)}] Player {player_id}...", end=" ", flush=True)
        
        try:
            had_timeout = False
            
            # Fetch career stats (current season only)
            if not skip_career:
                careers, career_timeout = fetch_player_career(player_id, season_string)
                total_careers += careers
                had_timeout = had_timeout or career_timeout
            
            # Fetch game logs
            games, games_timeout = fetch_player_game_logs(player_id, season_string)
            total_games += games
            had_timeout = had_timeout or games_timeout
            
            # Handle timeout tracking
            if had_timeout:
                if consecutive_timeouts == 0:
                    first_timeout_idx = i  # Remember where timeouts started
                consecutive_timeouts += 1
                print(f"⚠️  timeout ({consecutive_timeouts}/{MAX_CONSECUTIVE_TIMEOUTS})")
                
                if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS:
                    print(f"\n🛑 {MAX_CONSECUTIVE_TIMEOUTS} consecutive timeouts detected!")
                    print(f"   Pausing for {TIMEOUT_RECOVERY_DELAY} seconds to recover...")
                    print(f"   Progress saved. Will resume from player {first_timeout_idx + 1} (first timeout).\n")
                    
                    # Save progress at the FIRST timeout, not current
                    progress['last_completed_idx'] = first_timeout_idx - 1
                    save_progress(progress)
                    time.sleep(TIMEOUT_RECOVERY_DELAY)
                    
                    # Reset and go back to first timeout
                    consecutive_timeouts = 0
                    first_timeout_idx = None
                    i = progress['last_completed_idx'] + 1  # Resume from first timeout
                    print("🔄 Resuming...\n")
                    continue
                else:
                    i += 1  # Move to next player, still counting timeouts
                    continue
            else:
                consecutive_timeouts = 0  # Reset on success
                first_timeout_idx = None
                print(f"✓ {games} games" + (f", {careers} seasons" if not skip_career else ""))
            
            # Save progress after each successful player
            progress['last_completed_idx'] = i
            save_progress(progress)
            i += 1  # Move to next player
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupted! Progress saved. Run again to resume.")
            save_progress(progress)
            return
        except Exception as e:
            print(f"❌ Failed: {e}")
            # Save progress and move to next player
            progress['last_completed_idx'] = i
            save_progress(progress)
            i += 1
    
    # Complete!
    clear_progress()
    
    # Update team game logs (only 30 teams, quick)
    print(f"\n{'='*70}")
    print(f"  UPDATING TEAM GAME LOGS")
    print(f"{'='*70}\n")
    
    team_ids = get_team_ids()
    total_team_games = 0
    
    for team_id in team_ids:
        print(f"  Team {team_id}...", end=" ", flush=True)
        games, was_timeout = fetch_team_game_logs(team_id, season_string)
        total_team_games += games
        if was_timeout:
            print("⚠️  timeout")
        else:
            print(f"✓ {games} games")
    
    print(f"\n{'='*70}")
    print(f"  UPDATE COMPLETE")
    print(f"  Player games loaded: {total_games}")
    if not skip_career:
        print(f"  Career records loaded: {total_careers}")
    print(f"  Team games loaded: {total_team_games}")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Incremental NBA data update")
    parser.add_argument('--skip-career', action='store_true', help='Skip career stats update')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh, ignore saved progress')
    
    args = parser.parse_args()
    
    main(skip_career=args.skip_career, resume=not args.no_resume)