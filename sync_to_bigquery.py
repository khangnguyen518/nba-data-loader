"""
Sync MySQL tables to BigQuery.

Run this after update_recent.py to keep BigQuery in sync with MySQL.
"""
import os
from datetime import datetime, date
from decimal import Decimal
from google.cloud import bigquery
from google.oauth2 import service_account
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

# MySQL config
MYSQL_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "nba_raw")
}

# BigQuery config
BQ_PROJECT = os.getenv("BQ_PROJECT", "nba-project-481607")
BQ_DATASET = os.getenv("BQ_DATASET", "nba_raw")
BQ_KEYFILE = os.getenv("BQ_KEYFILE", os.path.expanduser("~/.dbt/bigquery-keyfile.json"))

# Tables to sync
TABLES = [
    "raw_players",
    "raw_teams",
    "raw_player_career_stats",
    "raw_player_game_logs",
    "raw_team_game_logs",
    "raw_player_common_info"
]


def get_mysql_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def get_bq_client():
    credentials = service_account.Credentials.from_service_account_file(BQ_KEYFILE)
    return bigquery.Client(credentials=credentials, project=BQ_PROJECT)


def serialize_row(row):
    """Convert datetime and other non-JSON types to strings."""
    serialized = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            serialized[key] = value.isoformat()
        elif isinstance(value, Decimal):
            serialized[key] = float(value)
        elif isinstance(value, bytes):
            serialized[key] = value.decode('utf-8', errors='ignore')
        else:
            serialized[key] = value
    return serialized


def get_table_data(table_name):
    """Fetch all data from a MySQL table."""
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [serialize_row(row) for row in rows]


def sync_table(bq_client, table_name):
    """Sync a single table from MySQL to BigQuery."""
    print(f"  Syncing {table_name}...")
    
    # Get data from MySQL
    rows = get_table_data(table_name)
    
    if not rows:
        print(f"    ⚠️  No data in {table_name}")
        return 0
    
    # Define BigQuery table reference
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    
    # Configure load job (replace entire table)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    
    # Load data
    job = bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete
    
    print(f"    ✓ Loaded {len(rows)} rows")
    return len(rows)


def sync_all():
    """Sync all tables from MySQL to BigQuery."""
    print(f"\n{'='*60}")
    print("  MYSQL → BIGQUERY SYNC")
    print(f"{'='*60}\n")
    
    print(f"Project: {BQ_PROJECT}")
    print(f"Dataset: {BQ_DATASET}")
    print()
    
    bq_client = get_bq_client()
    total_rows = 0
    
    for table in TABLES:
        try:
            rows = sync_table(bq_client, table)
            total_rows += rows
        except Exception as e:
            print(f"    ❌ Failed: {e}")
    
    print(f"\n{'='*60}")
    print(f"  SYNC COMPLETE — {total_rows} total rows")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    sync_all()