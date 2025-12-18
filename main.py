"""NBA Raw Data Loader - Main entry point."""
import argparse
import sys
from db import create_database, test_connection
from loaders import (
    load_teams, load_players, load_game_logs,
    load_team_game_logs, load_player_info, load_player_career
)
from config import START_SEASON, END_SEASON


def print_header():
    print("\n" + "="*70)
    print(" "*20 + "NBA RAW DATA LOADER")
    print(" "*15 + "Loads data into raw tables for dbt")
    print("="*70 + "\n")


def print_footer(has_failures=False):
    print("\n" + "="*70)
    if has_failures:
        print("⚠️  Raw data loading complete with some failures!")
    else:
        print("✓ Raw data loading complete!")
    print("="*70 + "\n")


def main(args):
    print_header()

    if args.setup:
        print("Setting up database...")
        try:
            create_database()
        except Exception as e:
            print(f"❌ Failed to create database: {e}")
            sys.exit(1)

    print("Testing database connection...")
    if not test_connection():
        print("❌ Cannot connect to database.")
        sys.exit(1)

    if args.resume:
        print("\n🔄 RESUME MODE: Will skip already loaded data\n")

    loaders_with_failures = []

    if not args.skip_teams:
        try:
            load_teams()
        except Exception as e:
            print(f"❌ Failed to load teams: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    if not args.skip_players:
        try:
            load_players()
        except Exception as e:
            print(f"❌ Failed to load players: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    if not args.skip_player_info:
        try:
            loader = load_player_info(
                limit_players=args.limit_players,
                resume=args.resume,
                active_only=args.active_only
            )
            if hasattr(loader, 'failed_attempts') and loader.failed_attempts:
                loaders_with_failures.append(('player_info', loader))
        except Exception as e:
            print(f"❌ Failed to load player info: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    if not args.skip_player_career:
        try:
            loader = load_player_career(
                limit_players=args.limit_players,
                resume=args.resume,
                active_only=args.active_only
            )
            if hasattr(loader, 'failed_attempts') and loader.failed_attempts:
                loaders_with_failures.append(('player_career', loader))
        except Exception as e:
            print(f"❌ Failed to load player career stats: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    if not args.skip_game_logs:
        try:
            loader = load_game_logs(
                start_season=args.start_season,
                end_season=args.end_season,
                limit_players=args.limit_players,
                use_career_stats=not args.no_career_stats,
                resume=args.resume,
                active_only=args.active_only,
                start_player=args.start_player
            )
            if hasattr(loader, 'failed_attempts') and loader.failed_attempts:
                loaders_with_failures.append(('game_logs', loader))
        except Exception as e:
            print(f"❌ Failed to load game logs: {e}")
            if not args.continue_on_error:
                sys.exit(1)

    if not args.skip_team_logs:
        try:
            load_team_game_logs(
                start_season=args.start_season,
                end_season=args.end_season
            )
        except Exception as e:
            print(f"❌ Failed to load team game logs: {e}")

    if args.retry_failures:
        for loader_name, loader in loaders_with_failures:
            print(f"\nRetrying failed {loader_name} attempts...")
            loader.retry_failed_attempts()

    print_footer(len(loaders_with_failures) > 0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load NBA raw data into MySQL")

    parser.add_argument('--setup', action='store_true', help='Create database (first run)')
    parser.add_argument('--resume', action='store_true', help='Skip already-loaded data')
    parser.add_argument('--start-season', type=int, default=START_SEASON)
    parser.add_argument('--end-season', type=int, default=END_SEASON)
    parser.add_argument('--limit-players', type=int, help='Limit to N players (testing)')
    parser.add_argument('--start-player', type=int, default=0, help='Skip first N players (for resuming)')
    parser.add_argument('--active-only', action='store_true', help='Only active players')
    parser.add_argument('--skip-teams', action='store_true')
    parser.add_argument('--skip-players', action='store_true')
    parser.add_argument('--skip-player-info', action='store_true')
    parser.add_argument('--skip-player-career', action='store_true')
    parser.add_argument('--skip-game-logs', action='store_true')
    parser.add_argument('--skip-team-logs', action='store_true')
    parser.add_argument('--no-career-stats', action='store_true', help='Do not use career stats optimization')
    parser.add_argument('--continue-on-error', action='store_true')
    parser.add_argument('--retry-failures', action='store_true')

    args = parser.parse_args()
    main(args)