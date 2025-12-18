"""Single configuration file for NBA data loader."""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "nba_raw")
}

# API Settings
API_RATE_LIMIT = 3    # seconds between calls (increased from lower value)
API_TIMEOUT = 90      # request timeout in seconds
API_MAX_RETRIES = 5   # number of retry attempts
COOLDOWN_INTERVAL = 20  # Take a break after every N players
COOLDOWN_TIME = 15    # How long to wait during cool-down (seconds)

# Data Loading Settings
START_SEASON = 2023   # Default start season
END_SEASON = 2025     # Default end season
BATCH_SIZE = 500      # Rows per batch insert

# Logging
VERBOSE = True        # Print detailed logs