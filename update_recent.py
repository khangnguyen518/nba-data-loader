"""
Incremental update script for NBA data.

This script:
1. Updates career stats for active players (to get any new seasons)
2. Loads game logs for the current season

Run this daily/weekly to keep your database current.
"""
from datetime import datetime
from db import test_connection, get_db
from loaders import load_player_career, load_game_logs


def get_current_season() -> tuple[int, str]:
    now = datetime.now()
    
    if now.month < 10:
        season_year = now.year - 1
    else:
        season_year = now.year
    
    season_string = f"{season_year}-{str(season_year + 1)[-2:]}"
    return season_year, season_string


def check_career_stats_exist() -> bool:
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM raw_player_career_stats")
            count = cursor.fetchone()[0]
            return count > 0
    except Exception:
        return False


def check_players_exist() -> bool:
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM raw_players")
            count = cursor.fetchone()[0]
            return count > 0
    except Exception:
        return False


def main(skip_career=False, start_player=0):
    season_year, season_string = get_current_season()
    
    print(f"\n{'='*70}")
    print(f"  NBA INCREMENTAL UPDATE")
    print(f"  Current Season: {season_string}")
    print(f"  Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    print("Testing database connection...")
    if not test_connection():
        print("❌ Cannot connect to database. Exiting.")
        return
    
    if not check_players_exist():
        print("❌ No players in database. Run full load first:")
        print("   python main.py --setup")
        return
    
    print()
    
    if not skip_career:
        print(f"{'='*60}")
        print("STEP 1: Updating career stats for active players")
        print(f"{'='*60}\n")
        
        try:
            load_player_career(
                resume=False,
                active_only=True,
                cooldown_interval=20
            )
        except Exception as e:
            print(f"❌ Failed to update career stats: {e}")
        
        print()
    
    step_num = "1" if skip_career else "2"
    print(f"{'='*60}")
    print(f"STEP {step_num}: Loading game logs for {season_string} season")
    print(f"{'='*60}\n")
    
    try:
        load_game_logs(
            start_season=season_year,
            end_season=season_year + 1,
            use_career_stats=False,
            resume=False,
            active_only=True,
            max_workers=2,
            cooldown_interval=15,
            start_player=start_player
        )
    except Exception as e:
        print(f"❌ Failed to load game logs: {e}")
    
    print(f"\n{'='*70}")
    print(f"  UPDATE COMPLETE")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")


def update_full_history(start_year: int = 2020, start_player: int = 0):
    season_year, season_string = get_current_season()
    
    print(f"\n{'='*70}")
    print(f"  NBA FULL HISTORY UPDATE")
    print(f"  Seasons: {start_year}-{str(start_year+1)[-2:]} through {season_string}")
    print(f"  Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    if not test_connection():
        print("❌ Cannot connect to database. Exiting.")
        return
    
    if not check_career_stats_exist():
        print("❌ No career stats in database. Run career stats loader first:")
        print("   python main.py --skip-teams --skip-players --skip-player-info --skip-game-logs --skip-team-logs")
        return
    
    print()
    
    try:
        load_game_logs(
            start_season=start_year,
            end_season=season_year + 1,
            use_career_stats=False,
            resume=False,
            active_only=False,
            max_workers=3,
            cooldown_interval=20,
            start_player=start_player
        )
    except Exception as e:
        print(f"❌ Failed to load game logs: {e}")
    
    print(f"\n{'='*70}")
    print(f"  FULL HISTORY UPDATE COMPLETE")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    import argparse
    
    default_year, _ = get_current_season()
    
    parser = argparse.ArgumentParser(description="Incremental NBA data update")
    parser.add_argument('--full', action='store_true', help='Update full history instead of just current season')
    parser.add_argument('--start-year', type=int, default=default_year, help=f'Start year for full history update (default: {default_year})')
    parser.add_argument('--skip-career', action='store_true', help='Skip career stats update')
    parser.add_argument('--start-player', type=int, default=0, help='Skip first N players (for resuming)')
    
    args = parser.parse_args()
    
    if args.full:
        update_full_history(args.start_year, args.start_player)
    else:
        main(skip_career=args.skip_career, start_player=args.start_player)