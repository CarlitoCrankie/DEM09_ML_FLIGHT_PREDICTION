"""
Log ML training events to database
"""
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

logger = logging.getLogger(__name__)

class ModelLogger:
    """Log model training to PostgreSQL"""
    
    def __init__(self, postgres_conn_id='postgres_analytics'):
        self.postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    def log_training_event(self, metadata):
        """Log model training to database"""
        logger.info(" Logging training event to database...")
        
        try:
            insert_query = """
            INSERT INTO ml_tracking.model_training_history (
                model_name, model_type, training_records, test_records,
                features_used, test_r2, test_mae, test_rmse,
                cv_r2_mean, cv_r2_std, model_path, is_production, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            metrics = metadata.get('metrics', {})
            
            self.postgres_hook.run(
                insert_query,
                parameters=(
                    metadata.get('model_name'),
                    metadata.get('model_type'),
                    metadata.get('training_records'),
                    metadata.get('test_records'),
                    metadata.get('features_used'),
                    metrics.get('test_r2'),
                    metrics.get('test_mae'),
                    metrics.get('test_rmse'),
                    metrics.get('cv_r2_mean'),
                    metrics.get('cv_r2_std'),
                    metadata.get('model_path'),
                    True,
                    json.dumps(metadata)
                ),
                autocommit=True
            )
            
            logger.info(" Training event logged successfully")
            
        except Exception as e:
            logger.error(f" Failed to log training event: {e}")
    
    def get_latest_model_info(self):
        """Get latest production model info"""
        query = """
        SELECT model_name, model_type, training_timestamp, test_r2, test_mae, model_path
        FROM ml_tracking.model_training_history
        WHERE is_production = TRUE
        ORDER BY training_timestamp DESC
        LIMIT 1
        """
        
        result = self.postgres_hook.get_first(query)
        
        if result:
            return {
                'model_name': result[0],
                'model_type': result[1],
                'training_timestamp': result[2],
                'test_r2': float(result[3]),
                'test_mae': float(result[4]),
                'model_path': result[5]
            }
        return None
