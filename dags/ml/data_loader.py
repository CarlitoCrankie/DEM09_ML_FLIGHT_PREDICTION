"""
ML Data Loader - Loads data from PostgreSQL for model training
Matches actual Silver layer schema
"""

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)

class MLDataLoader:
    """Load data from PostgreSQL for ML training"""
    
    def __init__(self, postgres_conn_id='postgres_analytics'):
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        self.engine = None
    
    def connect_to_database(self):
        """Create database connection"""
        try:
            self.engine = self.postgres_hook.get_sqlalchemy_engine()
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def get_available_tables(self):
        """Check what tables are available in pipeline"""
        if not self.engine:
            if not self.connect_to_database():
                return None
        
        try:
            query = """
            SELECT schemaname, tablename, 
                   pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                   (SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = schemaname AND table_name = tablename) as column_count
            FROM pg_tables 
            WHERE schemaname IN ('bronze', 'silver', 'gold')
            ORDER BY schemaname, tablename;
            """
            
            tables_df = pd.read_sql(query, self.engine)
            logger.info("📋 Available tables in pipeline:")
            logger.info(f"\n{tables_df.to_string(index=False)}")
            return tables_df
            
        except Exception as e:
            logger.error(f"❌ Failed to get table info: {e}")
            return None
    
    def load_silver_data(self, limit=None):
        """
        Load cleaned data from Silver layer
        Matches actual schema: source_code, destination_code, total_fare_bdt
        """
        logger.info("📊 Loading data from Silver layer...")
        
        query = """
        SELECT 
            id,
            airline,
            source_code,
            destination_code,
            total_fare_bdt,
            travel_class,
            seasonality,
            is_peak_season,
            season_category,
            route_type
        FROM silver.silver_cleaned_flights
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        df = self.postgres_hook.get_pandas_df(query)
        
        # Create route column from source_code and destination_code
        df['route'] = df['source_code'] + '_to_' + df['destination_code']
        
        logger.info(f"✅ Loaded {len(df):,} records from Silver layer")
        logger.info(f"   Columns: {list(df.columns)}")
        
        return df
    
    def load_gold_features(self):
        """Load aggregated features from Gold layer"""
        logger.info("🏆 Loading features from Gold layer...")
        
        gold_features = {}
        
        try:
            # Airline statistics
            query = "SELECT * FROM gold.gold_avg_fare_by_airline"
            gold_features['airline_stats'] = self.postgres_hook.get_pandas_df(query)
            logger.info(f"   ✅ Airline stats: {len(gold_features['airline_stats'])} airlines")
        except Exception as e:
            logger.warning(f"   ⚠️ Could not load airline stats: {e}")
            gold_features['airline_stats'] = pd.DataFrame()
        
        try:
            # Route statistics
            query = "SELECT * FROM gold.gold_popular_routes"
            gold_features['route_stats'] = self.postgres_hook.get_pandas_df(query)
            logger.info(f"   ✅ Route stats: {len(gold_features['route_stats'])} routes")
        except Exception as e:
            logger.warning(f"   ⚠️ Could not load route stats: {e}")
            gold_features['route_stats'] = pd.DataFrame()
        
        try:
            # Seasonal statistics
            query = "SELECT * FROM gold.gold_seasonal_fare_analysis"
            gold_features['seasonal_stats'] = self.postgres_hook.get_pandas_df(query)
            logger.info(f"   ✅ Seasonal stats: {len(gold_features['seasonal_stats'])} seasons")
        except Exception as e:
            logger.warning(f"   ⚠️ Could not load seasonal stats: {e}")
            gold_features['seasonal_stats'] = pd.DataFrame()
        
        try:
            # Travel class statistics
            query = "SELECT * FROM gold.gold_fare_by_class"
            gold_features['class_stats'] = self.postgres_hook.get_pandas_df(query)
            logger.info(f"   ✅ Class stats: {len(gold_features['class_stats'])} classes")
        except Exception as e:
            logger.warning(f"   ⚠️ Could not load class stats: {e}")
            gold_features['class_stats'] = pd.DataFrame()
        
        return gold_features
    
    def enrich_with_gold_features(self, df_silver, gold_features):
        """Enrich Silver data with Gold layer aggregations"""
        logger.info("🔧 Enriching Silver data with Gold features...")
        
        df_enriched = df_silver.copy()
        
        # Merge airline features
        if 'airline_stats' in gold_features and not gold_features['airline_stats'].empty:
            # Get relevant columns from airline stats
            airline_cols = [col for col in gold_features['airline_stats'].columns 
                          if col not in df_enriched.columns or col == 'airline']
            
            df_enriched = df_enriched.merge(
                gold_features['airline_stats'][airline_cols],
                on='airline',
                how='left',
                suffixes=('', '_airline')
            )
            logger.info("   ✅ Added airline features")
        
        # Merge route features
        if 'route_stats' in gold_features and not gold_features['route_stats'].empty:
            route_cols = [col for col in gold_features['route_stats'].columns 
                         if col not in df_enriched.columns or col == 'route']
            
            df_enriched = df_enriched.merge(
                gold_features['route_stats'][route_cols],
                on='route',
                how='left',
                suffixes=('', '_route')
            )
            logger.info("   ✅ Added route features")
        
        # Merge seasonal features
        if 'seasonal_stats' in gold_features and not gold_features['seasonal_stats'].empty:
            seasonal_cols = [col for col in gold_features['seasonal_stats'].columns 
                           if col not in df_enriched.columns or col == 'seasonality']
            
            df_enriched = df_enriched.merge(
                gold_features['seasonal_stats'][seasonal_cols],
                on='seasonality',
                how='left',
                suffixes=('', '_seasonal')
            )
            logger.info("   ✅ Added seasonal features")
        
        # Merge class features
        if 'class_stats' in gold_features and not gold_features['class_stats'].empty:
            class_cols = [col for col in gold_features['class_stats'].columns 
                         if col not in df_enriched.columns or col == 'travel_class']
            
            df_enriched = df_enriched.merge(
                gold_features['class_stats'][class_cols],
                on='travel_class',
                how='left',
                suffixes=('', '_class')
            )
            logger.info("   ✅ Added class features")
        
        logger.info(f"✅ Enrichment complete: {df_silver.shape[1]} → {df_enriched.shape[1]} columns")
        
        return df_enriched
    
    def check_data_changes(self, layer='silver', since_date=None):
        """
        Check if data has changed since a given date
        
        Args:
            layer (str): Which layer to check ('silver' or 'bronze')
            since_date (datetime): Check for changes after this date
        
        Returns:
            dict: Change information
        """
        logger.info(f"🔍 Checking for data changes in {layer.upper()} layer...")
        
        table_map = {
            'bronze': 'bronze.validated_flights',
            'silver': 'silver.silver_cleaned_flights'
        }
        
        if layer not in table_map:
            raise ValueError(f"Invalid layer: {layer}. Choose 'bronze' or 'silver'")
        
        try:
            # Get record count (Silver doesn't have updated_at/created_at)
            if since_date:
                query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    CURRENT_TIMESTAMP as latest_update
                FROM {table_map[layer]}
                """
            else:
                query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    CURRENT_TIMESTAMP as latest_update
                FROM {table_map[layer]}
                """
            
            result = self.postgres_hook.get_pandas_df(query)
            
            total_records = int(result['total_records'].iloc[0])
            latest_update = result['latest_update'].iloc[0]
            
            # For Silver, we can't detect incremental changes without timestamps
            # So we assume all records are "new" for first training
            new_records = total_records
            change_percentage = 100.0
            
            change_info = {
                'layer': layer,
                'total_records': total_records,
                'new_records': new_records,
                'latest_update': latest_update,
                'has_changes': total_records > 0,
                'change_percentage': change_percentage
            }
            
            logger.info(f"📊 Change Detection ({layer.upper()}):")
            logger.info(f"   Total records: {total_records:,}")
            logger.info(f"   Has data: {change_info['has_changes']}")
            
            return change_info
            
        except Exception as e:
            logger.error(f"❌ Failed to check changes: {e}")
            return None
    
    def get_load_statistics(self, days=7):
        """
        Get recent data load statistics from Bronze layer
        
        Args:
            days (int): Number of days to look back
        
        Returns:
            pd.DataFrame: Load statistics
        """
        logger.info(f"📊 Getting load statistics for last {days} days...")
        
        try:
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
            """
            
            stats = self.postgres_hook.get_pandas_df(query)
            
            if not stats.empty:
                logger.info(f"✅ Found {len(stats)} load events in last {days} days")
            else:
                logger.info(f"ℹ️ No load events found in last {days} days")
            
            return stats
            
        except Exception as e:
            logger.warning(f"⚠️ Could not get load statistics: {e}")
            return pd.DataFrame()
    
    def get_training_data(self, enrich_with_gold=True, limit=None):
        """
        Main method to get training data
        
        Args:
            enrich_with_gold (bool): Add Gold layer features
            limit (int): Optional record limit for testing
        
        Returns:
            pd.DataFrame: Complete training dataset
        """
        # Load Silver data
        df = self.load_silver_data(limit=limit)
        
        # Optionally enrich with Gold features
        if enrich_with_gold:
            gold_features = self.load_gold_features()
            df = self.enrich_with_gold_features(df, gold_features)
        
        return df
