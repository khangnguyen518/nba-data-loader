"""Base loader class with common functionality."""
import time
import random
import signal
import sys
import math
from abc import ABC, abstractmethod
import polars as pl
from db import get_db
from config import API_RATE_LIMIT, API_MAX_RETRIES, API_TIMEOUT, BATCH_SIZE, VERBOSE


class BaseLoader(ABC):
    """
    Abstract base class for all data loaders.

    Subclasses must implement:
    - fetch_data(): Fetch data from NBA API
    - get_create_table_sql(): Return CREATE TABLE statement
    - Set self.table_name
    """

    def __init__(self):
        self.table_name = None  # Must be set in subclass
        self._shutdown_requested = False
        self._partial_data = []  # Store partial results during fetch
        self._is_cleaning_up = False  # Flag to allow insert during cleanup
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        signal_name = 'SIGINT' if signum == signal.SIGINT else 'SIGTERM'
        
        # If already cleaning up, force exit
        if self._is_cleaning_up:
            print("\n❌ Force shutdown during cleanup - some data may be lost")
            sys.exit(1)
        
        print(f"\n⚠️  Received {signal_name} - initiating graceful shutdown...")
        print("   Completing current operation, please wait...")
        print("   (Press Ctrl+C again to force quit - may lose partial data)")

        self._shutdown_requested = True

        # If interrupted twice before cleanup, force exit
        if hasattr(self, '_first_interrupt') and self._first_interrupt:
            print("\n❌ Force shutdown requested - exiting immediately")
            sys.exit(1)

        self._first_interrupt = True

    def _cleanup(self):
        """
        Clean up resources and save partial progress.
        Called during shutdown or at the end of execution.
        """
        if not self._partial_data:
            return
            
        self._is_cleaning_up = True  # Allow insert to complete
        
        print(f"\n{'='*60}")
        print("Saving partial progress...")
        print(f"{'='*60}")

        try:
            # Combine partial data into DataFrame
            if len(self._partial_data) > 1:
                try:
                    df = pl.concat(self._partial_data, how="diagonal")
                except:
                    df = pl.concat(self._partial_data, how="diagonal_relaxed")
            else:
                df = self._partial_data[0]

            if not df.is_empty():
                print(f"✓ Saving {len(df)} rows collected before shutdown...")
                self._force_insert_data(df)  # Use force insert
                print("✓ Partial data saved successfully")
        except Exception as e:
            print(f"❌ Failed to save partial data: {e}")
        finally:
            self._is_cleaning_up = False

        # Clear partial data
        self._partial_data = []

    def _force_insert_data(self, df: pl.DataFrame):
        """
        Force insert data without checking shutdown flag.
        Used during cleanup to ensure partial data is saved.
        
        Args:
            df: Polars DataFrame to insert
        """
        if df.is_empty():
            print(f"⚠️  No data to insert into {self.table_name}")
            return

        # Prepare SQL
        columns = df.columns
        placeholders = ", ".join(["%s"] * len(columns))
        cols_str = ", ".join(columns)

        sql = f"REPLACE INTO {self.table_name} ({cols_str}) VALUES ({placeholders})"
        rows = [self._clean_row_for_mysql(row) for row in df.to_numpy().tolist()]

        total_inserted = 0

        with get_db() as conn:
            cursor = conn.cursor()

            # Insert in batches - NO shutdown check here
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]

                try:
                    cursor.executemany(sql, batch)
                    total_inserted += len(batch)

                    if VERBOSE:
                        print(f"  ✓ Inserted batch: {len(batch)} rows (total: {total_inserted}/{len(rows)})")

                except Exception as e:
                    print(f"❌ Batch insert failed: {e}")
                    # Try inserting rows individually
                    for row in batch:
                        try:
                            cursor.execute(sql, row)
                            total_inserted += 1
                        except Exception as row_error:
                            print(f"⚠️  Failed to insert row: {row_error}")

        print(f"✓ Inserted {total_inserted} rows into {self.table_name}")

    def check_shutdown(self):
        """
        Check if shutdown has been requested.

        Returns:
            bool: True if shutdown requested, False otherwise
        """
        return self._shutdown_requested

    @abstractmethod
    def fetch_data(self) -> pl.DataFrame:
        """
        Fetch data from NBA API.
        
        Returns:
            Polars DataFrame with raw data
        """
        pass
    
    @abstractmethod
    def get_create_table_sql(self) -> str:
        """
        Return CREATE TABLE SQL statement.
        
        Returns:
            SQL string for creating the raw table
        """
        pass
    
    def api_call(self, func, *args, **kwargs):
        """
        Wrapper for API calls with retry logic and rate limiting.

        Args:
            func: Function to call
            *args, **kwargs: Arguments to pass to function

        Returns:
            Result from function call, or None if all attempts fail
        """
        # Check for shutdown before making API call
        if self._shutdown_requested:
            if VERBOSE:
                print("⚠️  Skipping API call due to shutdown request")
            return None

        # Ensure timeout is set if not provided
        if 'timeout' not in kwargs:
            kwargs['timeout'] = API_TIMEOUT

        # Rate limiting with jitter to make timing less predictable
        jitter = random.uniform(1, 3)  # Random 1-3 second
        time.sleep(API_RATE_LIMIT + jitter)

        for attempt in range(API_MAX_RETRIES):
            # Check for shutdown during retries
            if self._shutdown_requested:
                if VERBOSE:
                    print("⚠️  Aborting API retry due to shutdown request")
                return None

            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_str = str(e).lower()
                
                # Don't retry on "resultSet" errors - these are players with no data
                # This is not a network issue, just missing data
                if 'resultset' in error_str or "'resultset'" in error_str:
                    if VERBOSE:
                        print(f"    ⚠️  No data available for this player")
                    return None
                
                if attempt == API_MAX_RETRIES - 1:
                    if VERBOSE:
                        print(f"❌ API call failed after {API_MAX_RETRIES} attempts: {e}")
                    # Store failed attempt for retry later
                    if not hasattr(self, 'failed_attempts'):
                        self.failed_attempts = []
                    self.failed_attempts.append({
                        'func': func,
                        'args': args,
                        'kwargs': kwargs,
                        'error': str(e)
                    })
                    return None  # Return None instead of raising

                wait_time = 2 ** attempt  # Exponential backoff
                if VERBOSE:
                    print(f"⚠️  API call failed (attempt {attempt + 1}/{API_MAX_RETRIES}): {e}")
                    print(f"   Retrying in {wait_time}s...")
                time.sleep(wait_time)

    def retry_failed_attempts(self):
        """
        Retry all failed API calls that were collected during execution.
        
        Returns:
            List of results from successful retries
        """
        if not hasattr(self, 'failed_attempts') or not self.failed_attempts:
            print("✓ No failed attempts to retry")
            return []
        
        print(f"\n{'='*60}")
        print(f"Retrying {len(self.failed_attempts)} failed attempts...")
        print(f"{'='*60}\n")
        
        results = []
        remaining_failures = []
        
        for i, attempt in enumerate(self.failed_attempts, 1):
            print(f"[{i}/{len(self.failed_attempts)}] Retrying failed call...")
            try:
                result = self.api_call(
                    attempt['func'],
                    *attempt['args'],
                    **attempt['kwargs']
                )
                if result is not None:
                    results.append(result)
                    print("   ✓ Retry successful!")
                else:
                    remaining_failures.append(attempt)
                    print("   ❌ Retry failed")
            except Exception as e:
                remaining_failures.append(attempt)
                print(f"   ❌ Retry failed: {e}")
        
        # Update failed_attempts with any remaining failures
        self.failed_attempts = remaining_failures
        
        print(f"\n{'='*60}")
        print(f"Retry Summary: {len(results)} succeeded, {len(remaining_failures)} still failed")
        print(f"{'='*60}\n")
        
        return results
        
    def create_table(self):
        """Create the raw table in database."""
        from db import execute_sql
        
        sql = self.get_create_table_sql()
        execute_sql(sql)
        
        if VERBOSE:
            print(f"✓ Table '{self.table_name}' is ready")
    
    def _clean_row_for_mysql(self, row: list) -> list:
        """
        Clean a row for MySQL insertion by converting NaN/inf to None.
        
        Args:
            row: List of values
            
        Returns:
            Cleaned list with NaN/inf replaced by None
        """
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

    def insert_data(self, df: pl.DataFrame):
        """
        Insert DataFrame into database using batch inserts.

        Args:
            df: Polars DataFrame to insert
        """
        if df.is_empty():
            print(f"⚠️  No data to insert into {self.table_name}")
            return

        # Prepare SQL
        columns = df.columns
        placeholders = ", ".join(["%s"] * len(columns))
        cols_str = ", ".join(columns)

        # REPLACE INTO handles duplicates automatically
        sql = f"REPLACE INTO {self.table_name} ({cols_str}) VALUES ({placeholders})"

        # Convert DataFrame to list of tuples and clean NaN values
        rows = [self._clean_row_for_mysql(row) for row in df.to_numpy().tolist()]

        total_inserted = 0

        with get_db() as conn:
            cursor = conn.cursor()

            # Insert in batches
            for i in range(0, len(rows), BATCH_SIZE):
                # Check for shutdown between batches (but not during cleanup)
                if self._shutdown_requested and not self._is_cleaning_up:
                    print(f"\n⚠️  Shutdown requested - stopping insert (saved {total_inserted}/{len(rows)} rows)")
                    break

                batch = rows[i:i + BATCH_SIZE]

                try:
                    cursor.executemany(sql, batch)
                    total_inserted += len(batch)

                    if VERBOSE:
                        print(f"  ✓ Inserted batch: {len(batch)} rows (total: {total_inserted}/{len(rows)})")

                except Exception as e:
                    print(f"❌ Batch insert failed: {e}")
                    # Try inserting rows individually
                    for row in batch:
                        if self._shutdown_requested and not self._is_cleaning_up:
                            break
                        try:
                            cursor.execute(sql, row)
                            total_inserted += 1
                        except Exception as row_error:
                            print(f"⚠️  Failed to insert row: {row_error}")

        print(f"✓ Inserted {total_inserted} rows into {self.table_name}")
    
    def run(self):
        """
        Main execution flow:
        1. Create table
        2. Fetch data
        3. Insert data
        4. Cleanup
        """
        try:
            print(f"\n{'='*60}")
            print(f"Loading: {self.table_name}")
            print(f"{'='*60}")

            # Step 1: Create table
            if self._shutdown_requested:
                print("⚠️  Shutdown requested before table creation")
                return

            self.create_table()

            # Step 2: Fetch data
            if self._shutdown_requested:
                print("⚠️  Shutdown requested before data fetch")
                return

            print("Fetching data from NBA API...")
            df = self.fetch_data()

            if self._shutdown_requested:
                print("⚠️  Shutdown requested during data fetch")
                self._cleanup()
                return

            print(f"✓ Fetched {len(df)} rows")

            # Step 3: Insert data
            if self._shutdown_requested:
                print("⚠️  Shutdown requested before data insert")
                self._cleanup()
                return

            print("Inserting data into database...")
            self.insert_data(df)

            if not self._shutdown_requested:
                print(f"{'='*60}")
                print(f"✓ {self.table_name} loading complete!\n")
            else:
                print(f"{'='*60}")
                print(f"⚠️  {self.table_name} loading interrupted\n")

        except KeyboardInterrupt:
            print("\n⚠️  KeyboardInterrupt caught - cleaning up...")
            self._cleanup()
            raise
        except Exception as e:
            print(f"\n❌ Error during execution: {e}")
            self._cleanup()
            raise
        finally:
            # Always attempt cleanup if there's partial data
            if self._partial_data:
                self._cleanup()
