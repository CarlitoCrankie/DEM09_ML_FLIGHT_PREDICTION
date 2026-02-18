"""
ML Training Tasks for Flight Price Pipeline
FIXED VERSION with comprehensive NaN handling
"""

import sys
import os
from datetime import datetime
from sklearn.model_selection import train_test_split
import joblib
import json
import numpy as np
import pandas as pd

# Add ml module to path
sys.path.append(os.path.dirname(__file__))

from ml.data_loader import MLDataLoader
from ml.feature_engineer import FeatureEngineer
from ml.feature_selector import SmartFeatureSelector
from ml.model_trainer import ModelTrainer  
from ml.model_evaluator import ModelEvaluator
from ml.model_logger import ModelLogger

MODEL_DIR = '/opt/airflow/models'

def decide_model_retraining(**context) -> str:
    """Decide whether to retrain model"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    import logging
    
    logger = logging.getLogger(__name__)
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    try:
        logger.info(" Deciding whether to retrain model...")
        
        ti = context['ti']
        load_type = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='load_type')
        records_inserted = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_inserted') or 0
        change_percentage = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='change_percentage') or 0
        
        logger.info(f"Load type: {load_type}, Records: {records_inserted:,}, Change: {change_percentage:.2f}%")
        
        # Decision thresholds
        MIN_CHANGE_PCT = 5.0
        MIN_NEW_RECORDS = 1000
        
        model_logger = ModelLogger(postgres_conn_id='postgres_analytics')
        current_model = model_logger.get_latest_model_info()
        
        should_retrain = False
        reasons = []
        
        if current_model:
            days_since_training = (datetime.now() - current_model['training_timestamp']).days
            logger.info(f"Current model: {current_model['model_name']}, Age: {days_since_training} days")
            
            if change_percentage >= MIN_CHANGE_PCT:
                should_retrain = True
                reasons.append(f"Data changed by {change_percentage:.2f}%")
            
            if records_inserted >= MIN_NEW_RECORDS:
                should_retrain = True
                reasons.append(f"{records_inserted:,} new records")
            
            if days_since_training >= 30:
                should_retrain = True
                reasons.append(f"Model is {days_since_training} days old")
            
            if load_type == 'FULL':
                should_retrain = True
                reasons.append("Full data reload")
        else:
            should_retrain = True
            reasons = ["No existing model"]
        
        decision = 'retrain' if should_retrain else 'skip'
        logger.info(f"Decision: {decision.upper()}")
        
        if should_retrain:
            for r in reasons:
                logger.info(f"  - {r}")
        
        log_pipeline_event(dag_id, dag_run_id, task_id, 'completed', {'decision': decision})
        
        return decision
        
    except Exception as e:
        logger.exception(f"Decision failed: {e}")
        return 'skip'


def retrain_ml_model(**context) -> dict:
    """Retrain ML model with comprehensive NaN handling"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    import logging
    
    logger = logging.getLogger(__name__)
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(dag_id, dag_run_id, task_id, 'started')
    
    try:
        logger.info("=" * 70)
        logger.info(" STARTING ML MODEL RETRAINING")
        logger.info("=" * 70)
        
        os.makedirs(f"{MODEL_DIR}/latest", exist_ok=True)
        os.makedirs(f"{MODEL_DIR}/archive", exist_ok=True)
        
        # ====================================
        # Step 1: Load data
        # ====================================
        logger.info(" Loading training data...")
        data_loader = MLDataLoader(postgres_conn_id='postgres_analytics')
        df_raw = data_loader.get_training_data(enrich_with_gold=False)
        logger.info(f"    Loaded {len(df_raw):,} records")
        logger.info(f"   Columns: {list(df_raw.columns)}")
        
        # Check for NaN in raw data
        nan_summary = df_raw.isna().sum()
        nan_cols = nan_summary[nan_summary > 0]
        if not nan_cols.empty:
            logger.warning(f"    NaN values in raw data:")
            for col, count in nan_cols.items():
                logger.warning(f"      {col}: {count} ({count/len(df_raw)*100:.1f}%)")
        
        # ====================================
        # Step 2: Feature engineering
        # ====================================
        logger.info(" Feature engineering...")
        feature_engineer = FeatureEngineer()
        df_features = feature_engineer.engineer_features(df_raw, fit=True)
        
        # Verify no NaN after feature engineering
        nan_check = df_features.isna().sum().sum()
        if nan_check > 0:
            logger.error(f"    {nan_check} NaN values remain after feature engineering!")
            nan_cols = df_features.columns[df_features.isna().any()].tolist()
            logger.error(f"   Problematic columns: {nan_cols}")
            raise ValueError(f"NaN values detected after feature engineering in columns: {nan_cols}")
        
        logger.info(f"    Feature engineering complete: {df_features.shape}")
        
        # ====================================
        # Step 3: Prepare X and y
        # ====================================
        logger.info(" Preparing features and target...")
        
        # Separate features and target
        target_col = 'total_fare_bdt'
        id_cols = ['id']
        
        # Ensure target exists
        if target_col not in df_features.columns:
            raise ValueError(f"Target column '{target_col}' not found in features")
        
        # Drop target and ID to get features
        drop_cols = [target_col] + [col for col in id_cols if col in df_features.columns]
        X = df_features.drop(columns=drop_cols, errors='ignore')
        y = df_features[target_col]
        
        logger.info(f"   Features shape: {X.shape}")
        logger.info(f"   Target shape: {y.shape}")
        
        # ====================================
        # Step 4: Ensure all features are numeric
        # ====================================
        logger.info("🔍 Ensuring all features are numeric...")
        
        numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()
        non_numeric_cols = [col for col in X.columns if col not in numeric_cols]
        
        if non_numeric_cols:
            logger.warning(f"    Dropping {len(non_numeric_cols)} non-numeric columns:")
            for col in non_numeric_cols[:10]:  # Show first 10
                logger.warning(f"      - {col} (dtype: {X[col].dtype})")
            if len(non_numeric_cols) > 10:
                logger.warning(f"      ... and {len(non_numeric_cols) - 10} more")
            X = X[numeric_cols]
        
        logger.info(f"    All features numeric: {X.shape}")
        
        # Final NaN check in X
        x_nan_check = X.isna().sum().sum()
        if x_nan_check > 0:
            logger.error(f"    {x_nan_check} NaN values found in feature matrix!")
            nan_cols_x = X.columns[X.isna().any()].tolist()
            logger.error(f"   Columns with NaN: {nan_cols_x}")
            
            # Try to fix by filling with 0
            logger.warning("   Attempting emergency fill with 0...")
            X = X.fillna(0)
            
            # Verify fix
            final_nan = X.isna().sum().sum()
            if final_nan > 0:
                raise ValueError(f"Could not eliminate NaN values. {final_nan} remain.")
            logger.warning("   Emergency fill successful")
        
        # Check target for NaN
        y_nan_check = y.isna().sum()
        if y_nan_check > 0:
            logger.error(f"   {y_nan_check} NaN values in target variable!")
            raise ValueError("Target variable contains NaN values")
        
        logger.info(f"    Data validation complete")
        logger.info(f"      X: {X.shape[0]} samples, {X.shape[1]} features")
        logger.info(f"      y: {len(y)} values")
        logger.info(f"      X NaN count: {X.isna().sum().sum()}")
        logger.info(f"      y NaN count: {y.isna().sum()}")
        
        # ====================================
        # Step 5: Feature selection
        # ====================================
        logger.info("🎯 Feature selection...")
        feature_selector = SmartFeatureSelector()
        
        # Select top features (limit to avoid overfitting)
        n_features = min(50, X.shape[1])
        selected_features = feature_selector.select_top_features(X, y, top_k=n_features)
        
        X_selected = X[selected_features]
        logger.info(f"    Selected {len(selected_features)} features")
        
        # ====================================
        # Step 6: Train-test split
        # ====================================
        logger.info(" Splitting data...")
        X_train, X_test, y_train, y_test = train_test_split(
            X_selected, y, test_size=0.2, random_state=42
        )
        logger.info(f"   Training: {X_train.shape}")
        logger.info(f"   Test: {X_test.shape}")
        
        # ====================================
        # Step 7: Train models
        # ====================================
        logger.info(" Training models...")
        trainer = ModelTrainer()
        result = trainer.train_all(X_train, y_train, X_test, y_test)

        best_model_info = {
            'name':    result['best_model_name'],
            'model':   result['best_model'],
            'metrics': result['metrics']
        }

        # Log all model results
        logger.info(" All model results:")
        for model_name, metrics in result['all_results'].items():
            logger.info(f"   {model_name}: R²={metrics['test_r2']:.4f}, MAE={metrics['test_mae']:,.2f}")

        logger.info(f"    Best model: {best_model_info['name']}")
        logger.info(f"      R²:   {best_model_info['metrics']['test_r2']:.4f}")
        logger.info(f"      MAE:  {best_model_info['metrics']['test_mae']:.2f}")
        logger.info(f"      RMSE: {best_model_info['metrics']['test_rmse']:.2f}")
        logger.info(f"      MAPE: {best_model_info['metrics']['test_mape']:.2f}%")
        
        # ====================================
        # Step 8: Evaluate
        # ====================================
        # Log all metrics
        evaluator = ModelEvaluator()
        result = evaluator.evaluate_model(best_model_info['model'], X_test, y_test, best_model_info['name'])
        logger.info(" Model evaluation metrics:")
        for metric_name, metric_value in result.items():
            logger.info(f"   {metric_name}: {metric_value:.4f}")
        
        # ====================================
        # Step 9: Save model
        # ====================================
        logger.info(" Saving model artifacts...")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save to latest
        model_path_latest = f"{MODEL_DIR}/latest/model.pkl"
        joblib.dump(best_model_info['model'], model_path_latest)
        
        # Save to archive
        model_path_archive = f"{MODEL_DIR}/archive/model_{timestamp}.pkl"
        joblib.dump(best_model_info['model'], model_path_archive)
        
        # Save transformers
        joblib.dump(feature_engineer, f"{MODEL_DIR}/latest/feature_engineer.pkl")
        joblib.dump(feature_selector, f"{MODEL_DIR}/latest/feature_selector.pkl")
        
        # Save metadata
        metadata = {
            'model_name': best_model_info['name'],
            'model_type': type(best_model_info['model']).__name__,
            'training_date': datetime.now().isoformat(),
            'training_records': len(X_train),
            'test_records': len(X_test),
            'features_used': len(selected_features),
            'selected_features': selected_features,
            'metrics': {k: float(v) for k, v in best_model_info['metrics'].items()},
            'version': '1.0',
            'dag_run_id': dag_run_id
        }
        
        with open(f"{MODEL_DIR}/latest/metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # ====================================
        # Step 10: Log to database
        # ====================================
        logger.info(" Logging to database...")
        model_logger = ModelLogger(postgres_conn_id='postgres_analytics')
        model_logger.log_training_event(metadata)
        
        logger.info("=" * 70)
        logger.info(" MODEL RETRAINING COMPLETED SUCCESSFULLY")
        logger.info(f"   Model: {best_model_info['name']}")        
        logger.info(f"      R²: {best_model_info['metrics']['test_r2']:.4f}")
        logger.info(f"      MAE: {best_model_info['metrics']['test_mae']:.2f}")
        logger.info(f"      RMSE: {best_model_info['metrics']['test_rmse']:.2f}")
        logger.info(f"      MAPE: {best_model_info['metrics']['test_mape']:.2f}%")
        logger.info(f"   Path: {model_path_latest}")
        logger.info("=" * 70)
        
        log_pipeline_event(dag_id, dag_run_id, task_id, 'completed', {
            'model_name': best_model_info['name'],
            'test_r2': float(best_model_info['metrics']['test_r2']),
            'test_mae': float(best_model_info['metrics']['test_mae']),
            'test_rmse': float(best_model_info['metrics']['test_rmse']),
            'test_mape': float(best_model_info['metrics']['test_mape']),
            'retrained': True
        })
        
        return {
            'model_path': model_path_latest,
            'model_name': best_model_info['name'],
            'metrics': metadata['metrics'],
            'retrained': True
        }
        
    except Exception as e:
        logger.exception(f" Model retraining failed: {e}")
        log_pipeline_event(dag_id, dag_run_id, task_id, 'failed', {'error': str(e)})
        raise


def skip_retraining(**context):
    """Skip retraining"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    import logging
    
    logger = logging.getLogger(__name__)
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    logger.info("⏭ Model retraining skipped")
    log_pipeline_event(dag_id, dag_run_id, task_id, 'completed', {'retrained': False})
    
    return {'retrained': False}


def retraining_complete(**context):
    """Join task"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    import logging
    
    logger = logging.getLogger(__name__)
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    ti = context['ti']
    decision = ti.xcom_pull(task_ids='decide_model_retraining')
    
    logger.info(f" Retraining branch completed (decision: {decision})")
    log_pipeline_event(dag_id, dag_run_id, task_id, 'completed')