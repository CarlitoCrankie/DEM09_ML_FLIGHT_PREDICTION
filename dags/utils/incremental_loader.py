"""
Incremental Data Loader for Flight Price Pipeline
Handles change detection and incremental loading to PostgreSQL

Purpose: Optimize data transfer by only processing changed records
"""

import pandas as pd
import hashlib
from datetime import datetime
from typing import Dict, Tuple
import logging
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

logger = logging.getLogger("airflow.task")


class IncrementalDataLoader:
    """
    Handles incremental data loading with MD5-based change detection.
    
    Features:
    - Hash-based change detection (MD5)
    - Soft deletes (is_active flag)
    - Automatic full vs incremental decision
    - Detailed load statistics and metadata tracking
    """
    
    def __init__(self, postgres_conn_id='postgres_analytics', mysql_conn_id='mysql_staging'):
        """
        Initialize the incremental loader.
        
        Args:
            postgres_conn_id: Airflow connection ID for PostgreSQL
            mysql_conn_id: Airflow connection ID for MySQL
        """
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.mysql_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
        self.load_stats = {
            'new': 0,
            'updated': 0,
            'deleted': 0,
            'unchanged': 0
        }
        
    def calculate_record_hash(self, row: pd.Series) -> str:
        """
        Calculate MD5 hash of record for change detection.
        
        Uses key business fields that define record uniqueness:
        - airline, source, destination, date, time
        - fare information
        
        Args:
            row: Pandas Series representing a single record
            
        Returns:
            32-character MD5 hash string
        """
        # Use business key fields for hash calculation
        key_fields = [
            str(row.get('airline', '')),
            str(row.get('source_code', '')),
            str(row.get('destination_code', '')),
            str(row.get('departure_datetime', '')),
            str(row.get('base_fare_bdt', '')),
            str(row.get('total_fare_bdt', '')),
            str(row.get('travel_class', '')),
            str(row.get('booking_source', ''))
        ]
        
        record_string = '|'.join(key_fields)
        return hashlib.md5(record_string.encode()).hexdigest()
    
    def load_new_data_from_mysql(self) -> pd.DataFrame:
        """
        Load newly validated data from MySQL.
        
        Returns:
            DataFrame containing all validated records from MySQL
        """
        logger.info("Loading validated data from MySQL...")
        
        query = """
            SELECT 
                v.*,
                r.loaded_at as mysql_loaded_at
            FROM validated_flight_data v
            JOIN raw_flight_data r ON v.raw_id = r.id
            WHERE v.is_valid = 1
        """
        
        df = self.mysql_hook.get_pandas_df(query)
        logger.info(f" Loaded {len(df):,} valid records from MySQL staging")
        
        return df
    
    def load_existing_data_from_postgres(self) -> pd.DataFrame:
        """
        Load current active data from PostgreSQL Bronze layer.
        
        Returns:
            DataFrame with all active records and their hashes
        """
        logger.info("Loading existing active records from PostgreSQL...")
        
        query = """
        SELECT 
            airline, source_code, destination_code, departure_datetime,
            base_fare_bdt, total_fare_bdt, travel_class, booking_source,
            record_hash
        FROM bronze.validated_flights
        WHERE is_active = TRUE
        """
        
        try:
            df = self.postgres_hook.get_pandas_df(query)
            logger.info(f" Loaded {len(df):,} existing active records from PostgreSQL")
            return df
        except Exception as e:
            logger.warning(f" No existing data found or error occurred: {e}")
            return pd.DataFrame()
    
    def detect_changes(self, new_df: pd.DataFrame) -> Dict:
        """
        Detect new, unchanged, and deleted records using hash comparison.
        
        Process:
        1. Calculate hashes for new data
        2. Compare with existing hashes
        3. Identify new records (in new but not in existing)
        4. Identify deleted records (in existing but not in new)
        5. Identify unchanged records (in both)
        
        Args:
            new_df: DataFrame with new data from MySQL
            
        Returns:
            Dictionary containing:
            - new_records: DataFrame of records to insert
            - deleted_records: DataFrame of records to soft-delete
            - unchanged_records: DataFrame of records that haven't changed
            - change_percentage: Percentage of data that changed
        """
        logger.info(" Starting change detection process...")
        start_time = datetime.now()
        
        # Calculate hashes for new data
        logger.info("Calculating record hashes for new data...")
        new_df['record_hash'] = new_df.apply(self.calculate_record_hash, axis=1)
        
        # Get existing data
        existing_df = self.load_existing_data_from_postgres()
        
        # Handle first load scenario
        if existing_df.empty:
            logger.info(" No existing data found. All records are NEW.")
            return {
                'new_records': new_df,
                'deleted_records': pd.DataFrame(),
                'unchanged_records': pd.DataFrame(),
                'change_percentage': 100.0
            }
        
        # Detect changes using set operations on hashes
        existing_hashes = set(existing_df['record_hash'])
        new_hashes = set(new_df['record_hash'])
        
        # New records: in new data but not in existing
        new_record_hashes = new_hashes - existing_hashes
        new_records = new_df[new_df['record_hash'].isin(new_record_hashes)].copy()
        
        # Deleted records: in existing but not in new data
        deleted_record_hashes = existing_hashes - new_hashes
        deleted_records = existing_df[existing_df['record_hash'].isin(deleted_record_hashes)].copy()
        
        # Unchanged records: in both existing and new
        unchanged_hashes = existing_hashes & new_hashes
        unchanged_records = new_df[new_df['record_hash'].isin(unchanged_hashes)].copy()
        
        # Calculate change percentage
        total_changes = len(new_records) + len(deleted_records)
        change_percentage = (total_changes / len(new_df) * 100) if len(new_df) > 0 else 0
        
        # Update internal statistics
        self.load_stats['new'] = len(new_records)
        self.load_stats['deleted'] = len(deleted_records)
        self.load_stats['unchanged'] = len(unchanged_records)
        
        detection_time = (datetime.now() - start_time).total_seconds()
        
        # Log summary
        logger.info("=" * 70)
        logger.info(" CHANGE DETECTION SUMMARY")
        logger.info("=" * 70)
        logger.info(f"    New records:       {len(new_records):,}")
        logger.info(f"     Deleted records:   {len(deleted_records):,}")
        logger.info(f"     Unchanged records: {len(unchanged_records):,}")
        logger.info(f"    Change percentage: {change_percentage:.2f}%")
        logger.info(f"   ⏱  Detection time:    {detection_time:.2f}s")
        logger.info("=" * 70)
        
        return {
            'new_records': new_records,
            'deleted_records': deleted_records,
            'unchanged_records': unchanged_records,
            'change_percentage': change_percentage
        }
    
    def _safe_value(self, val):
        """
        Safely convert a value for database insertion.
        Handles NaN, None, and pandas NA values.
        
        Args:
            val: Value to convert
            
        Returns:
            None for NaN/NA values, otherwise the original value
        """
        # Check if it's a pandas/numpy NaN or None
        if val is None:
            return None
        if isinstance(val, (float, np.floating)) and np.isnan(val):
            return None
        if pd.isna(val):
            return None
        return val
    
    def apply_incremental_load(self, changes: Dict, load_type: str = 'INCREMENTAL') -> None:
        """
        Apply incremental changes to Bronze layer.
        
        Process:
        1. Insert new records
        2. Soft-delete removed records (set is_active = FALSE)
        3. Log metadata
        
        Args:
            changes: Output from detect_changes()
            load_type: 'INCREMENTAL' or 'FULL'
        """
        logger.info(f" Applying {load_type} load...")
        start_time = datetime.now()
        
        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Start transaction
            cursor.execute("BEGIN")
            
            # ====================================
            # Step 1: Insert new records
            # ====================================
            if not changes['new_records'].empty:
                logger.info(f" Inserting {len(changes['new_records']):,} new records...")
                
                # Add tracking columns
                new_data = changes['new_records'].copy()
                new_data['ingestion_timestamp'] = datetime.now()
                new_data['is_active'] = True
                
                # Convert boolean columns properly
                if 'is_valid' in new_data.columns:
                    new_data['is_valid'] = new_data['is_valid'].astype(bool)
                
                # Define expected columns
                columns_list = [
                    'airline', 'source_code', 'source_name', 'destination_code', 'destination_name',
                    'departure_datetime', 'arrival_datetime', 'duration_hrs', 'stopovers', 'aircraft_type',
                    'travel_class', 'booking_source', 'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt',
                    'seasonality', 'days_before_departure', 'is_valid', 'validation_errors', 
                    'mysql_raw_id', 'mysql_validated_id', 'mysql_loaded_at',
                    'record_hash', 'ingestion_timestamp', 'is_active'
                ]
                
                # Insert in chunks
                insert_chunk_size = 500
                total_inserted = 0
                
                for i in range(0, len(new_data), insert_chunk_size):
                    chunk = new_data.iloc[i:i + insert_chunk_size]
                    
                    # Generate placeholders for this chunk
                    placeholders = ', '.join(f"({', '.join(['%s']*len(columns_list))})" for _ in range(len(chunk)))
                    
                    insert_sql = f"""
                        INSERT INTO bronze.validated_flights 
                        ({', '.join(columns_list)})
                        VALUES {placeholders}
                    """
                    
                    # Flatten values for insertion
                    values = []
                    for _, row in chunk.iterrows():
                        for col in columns_list:
                            val = row.get(col)
                            values.append(self._safe_value(val))
                    
                    cursor.execute(insert_sql, values)
                    total_inserted += len(chunk)
                    logger.info(f"    Inserted chunk ({total_inserted:,} / {len(new_data):,})")
                
                logger.info(f"    Inserted {len(changes['new_records']):,} new records")
            
            # ====================================
            # Step 2: Soft delete removed records
            # ====================================
            if not changes['deleted_records'].empty:
                logger.info(f"  Marking {len(changes['deleted_records']):,} records as inactive...")
                
                deleted_hashes = changes['deleted_records']['record_hash'].tolist()
                
                # Build parameterized query
                placeholders = ','.join(['%s'] * len(deleted_hashes))
                update_query = f"""
                    UPDATE bronze.validated_flights
                    SET is_active = FALSE,
                        ingestion_timestamp = %s
                    WHERE record_hash IN ({placeholders})
                    AND is_active = TRUE
                """
                
                cursor.execute(update_query, [datetime.now()] + deleted_hashes)
                
                logger.info(f"    Marked {len(changes['deleted_records']):,} records as inactive")
            
            # ====================================
            # Step 3: Log metadata
            # ====================================
            execution_time = int((datetime.now() - start_time).total_seconds())
            
            try:
                cursor.execute("""
                    INSERT INTO bronze.data_load_metadata 
                    (records_inserted, records_updated, records_deleted, records_unchanged,
                     load_type, change_percentage, execution_time_seconds)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    self.load_stats['new'],
                    self.load_stats['updated'],
                    self.load_stats['deleted'],
                    self.load_stats['unchanged'],
                    load_type,
                    changes['change_percentage'],
                    execution_time
                ))
            except Exception as e:
                logger.warning(f" Could not log metadata: {e}")
            
            # Commit transaction
            conn.commit()
            
            logger.info("=" * 70)
            logger.info(f" {load_type} LOAD COMPLETED SUCCESSFULLY")
            logger.info(f"     Execution time: {execution_time}s")
            logger.info("=" * 70)
            
        except Exception as e:
            conn.rollback()
            logger.error(f" {load_type} load failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def apply_full_load(self, df: pd.DataFrame) -> None:
        """
        Perform full truncate and reload.
        
        Used when change percentage exceeds threshold or on first run.
        
        Args:
            df: Complete dataset to load
        """
        logger.info(" Applying FULL load (truncate and reload)...")
        start_time = datetime.now()
        
        conn = self.postgres_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Start transaction
            cursor.execute("BEGIN")
            
            # ====================================
            # Step 1: Truncate table
            # ====================================
            logger.info("  Truncating bronze.validated_flights...")
            cursor.execute("TRUNCATE TABLE bronze.validated_flights RESTART IDENTITY")
            
            # ====================================
            # Step 2: Prepare data
            # ====================================
            df_copy = df.copy()
            
            # Add tracking columns
            df_copy['record_hash'] = df_copy.apply(self.calculate_record_hash, axis=1)
            df_copy['ingestion_timestamp'] = datetime.now()
            df_copy['is_active'] = True
            
            # Convert boolean columns
            if 'is_valid' in df_copy.columns:
                df_copy['is_valid'] = df_copy['is_valid'].astype(bool)
            
            # ====================================
            # Step 3: Insert all data
            # ====================================
            logger.info(f"Inserting {len(df_copy):,} records...")
            
            # Define column order
            columns_list = [
                'airline', 'source_code', 'source_name', 'destination_code', 'destination_name',
                'departure_datetime', 'arrival_datetime', 'duration_hrs', 'stopovers', 'aircraft_type',
                'travel_class', 'booking_source', 'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt',
                'seasonality', 'days_before_departure', 'is_valid', 'validation_errors', 
                'mysql_raw_id', 'mysql_validated_id', 'mysql_loaded_at',
                'record_hash', 'ingestion_timestamp', 'is_active'
            ]
            
            # Insert in chunks
            insert_chunk_size = 500
            total_inserted = 0
            
            for i in range(0, len(df_copy), insert_chunk_size):
                chunk = df_copy.iloc[i:i + insert_chunk_size]
                
                # Generate placeholders for this chunk
                placeholders = ', '.join(f"({', '.join(['%s']*len(columns_list))})" for _ in range(len(chunk)))
                
                insert_sql = f"""
                    INSERT INTO bronze.validated_flights 
                    ({', '.join(columns_list)})
                    VALUES {placeholders}
                """
                
                # Flatten values for insertion using safe conversion
                values = []
                for _, row in chunk.iterrows():
                    for col in columns_list:
                        val = row.get(col)
                        values.append(self._safe_value(val))
                
                try:
                    cursor.execute(insert_sql, values)
                    total_inserted += len(chunk)
                    if total_inserted % 5000 == 0 or total_inserted == len(df_copy):
                        logger.info(f"  Inserted {total_inserted:,} / {len(df_copy):,} records")
                except Exception as e:
                    logger.error(f"   Error inserting chunk at offset {i}: {e}")
                    raise
            
            # ====================================
            # Step 4: Log metadata
            # ====================================
            execution_time = int((datetime.now() - start_time).total_seconds())
            
            try:
                cursor.execute("""
                    INSERT INTO bronze.data_load_metadata 
                    (records_inserted, load_type, change_percentage, execution_time_seconds)
                    VALUES (%s, %s, %s, %s)
                """, (len(df_copy), 'FULL', 100.0, execution_time))
            except Exception as e:
                logger.warning(f" Could not log metadata: {e}")
            
            # Commit transaction
            conn.commit()
            
            logger.info("=" * 70)
            logger.info(" FULL LOAD COMPLETED SUCCESSFULLY")
            logger.info(f"    Records loaded: {len(df_copy):,}")
            logger.info(f"     Execution time: {execution_time}s")
            logger.info("=" * 70)
            
        except Exception as e:
            conn.rollback()
            logger.error(f" FULL load failed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def get_load_statistics(self, days: int = 30) -> pd.DataFrame:
        """
        Get load statistics for monitoring and analysis.
        
        Args:
            days: Number of days of history to retrieve
            
        Returns:
            DataFrame with recent load statistics
        """
        query = f"""
        SELECT 
            load_timestamp,
            load_type,
            records_inserted,
            records_deleted,
            records_unchanged,
            change_percentage,
            execution_time_seconds
        FROM bronze.data_load_metadata
        WHERE load_timestamp >= NOW() - INTERVAL '{days} days'
        ORDER BY load_timestamp DESC
        LIMIT 10
        """
        
        try:
            return self.postgres_hook.get_pandas_df(query)
        except Exception as e:
            logger.warning(f"Could not retrieve load statistics: {e}")
            return pd.DataFrame()