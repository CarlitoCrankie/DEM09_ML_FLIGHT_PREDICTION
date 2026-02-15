"""
Configuration for Flight Fare ML Pipeline
Integrated with Airflow data pipeline
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================
# DATABASE CONNECTIONS
# ============================================

# PostgreSQL Analytics (from Airflow environment)
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres-analytics'),
    'port': int(os.getenv('POSTGRES_ANALYTICS_PORT', 5432)),
    'database': os.getenv('POSTGRES_ANALYTICS_DB', 'flight_analytics'),
    'user': os.getenv('POSTGRES_ANALYTICS_USER', 'analytics_user'),
    'password': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'analytics_pass')
}

# MySQL Staging (if needed for direct access)
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'database': os.getenv('MYSQL_DATABASE', 'flight_staging'),
    'user': os.getenv('MYSQL_USER', 'flight_user'),
    'password': os.getenv('MYSQL_PASSWORD', 'flight_pass')
}

# ============================================
# PROJECT SETTINGS
# ============================================

PROJECT_CONFIG = {
    'random_state': 42,
    'test_size': 0.2,
    'cv_folds': 5,
    'target_column': 'total_fare',
    'data_source': 'pipeline',  # Use existing pipeline data
    'data_layer': 'silver',     # Default layer to use
    'enrich_with_gold': True    # Add Gold layer features
}

# ============================================
# MODEL PARAMETERS
# ============================================

MODEL_CONFIG = {
    'linear_regression': {},
    
    'ridge': {
        'alpha': [0.1, 1.0, 10.0, 100.0]
    },
    
    'lasso': {
        'alpha': [0.1, 1.0, 10.0, 100.0]
    },
    
    'random_forest': {
        'n_estimators': [100, 200, 300],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10],
        'random_state': 42,
        'n_jobs': -1
    }
}

# ============================================
# FEATURE ENGINEERING SETTINGS
# ============================================

FEATURE_CONFIG = {
    'top_k_features': 50,           # Number of features to select
    'feature_selection_method': 'ensemble',  # 'ensemble', 'random_forest', 'mutual_info'
    'scale_features': True,         # Apply StandardScaler
    'encode_categoricals': True,    # Encode categorical variables
    'create_temporal_features': True,  # Extract date/time features
    'create_derived_features': True    # Create business logic features
}

# ============================================
# TRAINING SETTINGS
# ============================================

TRAINING_CONFIG = {
    'min_change_percentage': 5.0,      # Retrain if >5% data changed
    'min_new_records': 1000,           # Retrain if >1000 new records
    'max_days_since_training': 30,     # Retrain if model >30 days old
    'force_retrain_on_full_load': True # Always retrain on full data reload
}

# ============================================
# MODEL PATHS
# ============================================

MODEL_PATHS = {
    'base_dir': os.getenv('MODEL_PATH', '/opt/airflow/models'),
    'latest': os.path.join(os.getenv('MODEL_PATH', '/opt/airflow/models'), 'latest'),
    'archive': os.path.join(os.getenv('MODEL_PATH', '/opt/airflow/models'), 'archive')
}

# ============================================
# API SETTINGS (for future deployment)
# ============================================

API_CONFIG = {
    'host': os.getenv('API_HOST', '0.0.0.0'),
    'port': int(os.getenv('API_PORT', 8000)),
    'model_path': MODEL_PATHS['latest'],
    'reload_on_change': True
}

# ============================================
# MLFLOW SETTINGS (for experiment tracking)
# ============================================

MLFLOW_CONFIG = {
    'tracking_uri': os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000'),
    'experiment_name': 'flight_fare_prediction',
    'artifact_location': '/mlflow/artifacts'
}

# ============================================
# LOGGING SETTINGS
# ============================================

LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'log_dir': '/opt/airflow/logs'
}
