"""Loaders package for NBA data."""
from loaders.teams import load_teams
from loaders.players import load_players
from loaders.game_logs import load_game_logs
from loaders.team_game_logs import load_team_game_logs
from loaders.player_info import load_player_info
from loaders.player_career import load_player_career

__all__ = [
    'load_teams',
    'load_players',
    'load_game_logs',
    'load_team_game_logs',
    'load_player_info',
    'load_player_career'
]
