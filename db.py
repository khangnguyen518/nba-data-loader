"""Database connection and utilities."""
import mysql.connector
from contextlib import contextmanager
from config import DB_CONFIG, VERBOSE

@contextmanager
def get_db():
    """
    Context manager for database connections.
    Automatically commits on success, rolls back on error.
    
    Usage:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM teams")
    """
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"❌ Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def execute_sql(sql):
    """Execute a single SQL statement."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        if VERBOSE:
            print(f"✓ Executed SQL")

def create_database():
    """Create the database if it doesn't exist."""
    config = DB_CONFIG.copy()
    db_name = config.pop("database")
    
    try:
        # Connect without specifying database
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"✓ Database '{db_name}' is ready")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Failed to create database: {e}")
        raise

def test_connection():
    """Test database connection."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result[0] == 1:
                print("✓ Database connection successful")
                return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False