"""
Simplified configuration for Airflow DAGs
Uses environment variables from docker-compose
"""

import os

# ============================================
# DATABASE CONNECTIONS
# ============================================

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres-analytics'),
    'port': int(os.getenv('POSTGRES_ANALYTICS_PORT', 5432)),
    'database': os.getenv('POSTGRES_ANALYTICS_DB', 'flight_analytics'),
    'user': os.getenv('POSTGRES_ANALYTICS_USER', 'analytics_user'),
    'password': os.getenv('POSTGRES_ANALYTICS_PASSWORD', 'analytics_pass')
}

# ============================================
# PROJECT SETTINGS
# ============================================

PROJECT_CONFIG = {
    'random_state': 42,
    'test_size': 0.2,
    'cv_folds': 5,
    'target_column': 'total_fare',
    'data_layer': 'silver',
    'enrich_with_gold': True
}

# ============================================
# MODEL CONFIGURATION
# ============================================

MODEL_CONFIG = {
    'linear_regression': {},
    'ridge': {'alpha': [0.1, 1.0, 10.0, 100.0]},
    'lasso': {'alpha': [0.1, 1.0, 10.0, 100.0]},
    'random_forest': {
        'n_estimators': [100, 200, 300],
        'max_depth': [10, 20, None],
        'min_samples_split': [2, 5, 10]
    }
}

# ============================================
# TRAINING THRESHOLDS
# ============================================

TRAINING_CONFIG = {
    'min_change_percentage': 5.0,
    'min_new_records': 1000,
    'max_days_since_training': 30
}

# ============================================
# MODEL PATHS
# ============================================

MODEL_DIR = '/opt/airflow/models'
