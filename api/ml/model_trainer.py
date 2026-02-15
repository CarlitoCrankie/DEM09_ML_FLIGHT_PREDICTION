# """
# Production ML Training Pipeline
# Integrates with existing data pipeline and MLflow
# """

# import os
# import sys
# import pandas as pd
# import numpy as np
# import joblib
# import mlflow
# import mlflow.sklearn
# from datetime import datetime
# from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
# from sklearn.linear_model import LinearRegression, Ridge, Lasso
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
# import warnings
# warnings.filterwarnings('ignore')

# from .data_loader import MLDataLoader
# from .feature_engineer import FeatureEngineer
# from .model_evaluator import ModelEvaluator
# import logging
# logger = logging.getLogger(__name__)

# class MLTrainingPipeline:
#     def __init__(self):
#         self.logger = logging.getLogger('ml_training')
#         self.models = {}
#         self.best_model = None
#         self.feature_engineer = FeatureEngineer()
#         self.evaluator = ModelEvaluator()
        
#         # MLflow setup
#         mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000'))
#         mlflow.set_experiment('flight_fare_prediction')
        
#     def load_and_prepare_data(self):
#         """Load data from existing pipeline and prepare for ML"""
#         self.logger.info("Loading data from existing pipeline...")
        
#         loader = MLDataLoader()
#         df_raw = loader.load_silver_data()
        
#         if df_raw is None or df_raw.empty:
#             raise ValueError("Failed to load data from pipeline")
        
#         self.logger.info(f"Loaded dataset: {df_raw.shape}")
        
#         # Feature engineering
#         self.logger.info("Applying feature engineering...")
#         df_features = self.feature_engineer.engineer_features(df_raw, fit_transform=True)
        
#         # Prepare X and y
#         target_col = 'total_fare'
#         if target_col not in df_features.columns:
#             raise ValueError(f"Target column '{target_col}' not found")
        
#         X = df_features.drop(columns=[target_col])
#         y = df_features[target_col]
        
#         # Train-test split
#         X_train, X_test, y_train, y_test = train_test_split(
#             X, y, test_size=0.2, random_state=42, stratify=pd.qcut(y, q=5, duplicates='drop')
#         )
        
#         self.logger.info(f"Training set: {X_train.shape}, Test set: {X_test.shape}")
        
#         return X_train, X_test, y_train, y_test, X.columns.tolist()
    
#     def train_models(self, X_train, y_train, X_test, y_test):
#         """Train multiple models with hyperparameter tuning"""
        
#         models_config = {
#             'linear_regression': {
#                 'model': LinearRegression(),
#                 'params': {}
#             },
#             'ridge': {
#                 'model': Ridge(),
#                 'params': {'alpha': [0.1, 1.0, 10.0, 100.0]}
#             },
#             'lasso': {
#                 'model': Lasso(),
#                 'params': {'alpha': [0.1, 1.0, 10.0, 100.0]}
#             },
#             'random_forest': {
#                 'model': RandomForestRegressor(random_state=42),
#                 'params': {
#                     'n_estimators': [100, 200],
#                     'max_depth': [10, 20, None],
#                     'min_samples_split': [2, 5]
#                 }
#             }
#         }
        
#         best_score = -np.inf
        
#         for model_name, config in models_config.items():
#             self.logger.info(f"Training {model_name}...")
            
#             with mlflow.start_run(run_name=f"{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                
#                 # Hyperparameter tuning
#                 if config['params']:
#                     grid_search = GridSearchCV(
#                         config['model'], 
#                         config['params'], 
#                         cv=5, 
#                         scoring='r2',
#                         n_jobs=-1
#                     )
#                     grid_search.fit(X_train, y_train)
#                     best_model = grid_search.best_estimator_
#                     best_params = grid_search.best_params_
#                 else:
#                     best_model = config['model']
#                     best_model.fit(X_train, y_train)
#                     best_params = {}
                
#                 # Predictions
#                 y_pred_train = best_model.predict(X_train)
#                 y_pred_test = best_model.predict(X_test)
                
#                 # Metrics
#                 metrics = {
#                     'train_r2': r2_score(y_train, y_pred_train),
#                     'test_r2': r2_score(y_test, y_pred_test),
#                     'train_mae': mean_absolute_error(y_train, y_pred_train),
#                     'test_mae': mean_absolute_error(y_test, y_pred_test),
#                     'train_rmse': np.sqrt(mean_squared_error(y_train, y_pred_train)),
#                     'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test))
#                 }
                
#                 # Cross-validation
#                 cv_scores = cross_val_score(best_model, X_train, y_train, cv=5, scoring='r2')
#                 metrics['cv_r2_mean'] = cv_scores.mean()
#                 metrics['cv_r2_std'] = cv_scores.std()
                
#                 # Log to MLflow
#                 mlflow.log_params(best_params)
#                 mlflow.log_metrics(metrics)
#                 mlflow.sklearn.log_model(best_model, f"model_{model_name}")
                
#                 # Store model
#                 self.models[model_name] = {
#                     'model': best_model,
#                     'metrics': metrics,
#                     'params': best_params
#                 }
                
#                 # Track best model
#                 if metrics['test_r2'] > best_score:
#                     best_score = metrics['test_r2']
#                     self.best_model = model_name
                
#                 self.logger.info(f"{model_name} - Test R²: {metrics['test_r2']:.4f}, Test MAE: {metrics['test_mae']:.2f}")
        
#         self.logger.info(f"Best model: {self.best_model} (R² = {best_score:.4f})")
    
#     def save_production_model(self, feature_names):
#         """Save the best model for production use"""
#         if not self.best_model:
#             raise ValueError("No best model found")
        
#         model_info = self.models[self.best_model]
        
#         # Create model directory
#         model_dir = '/app/models/latest'
#         os.makedirs(model_dir, exist_ok=True)
        
#         # Save model
#         joblib.dump(model_info['model'], f'{model_dir}/model.pkl')
        
#         # Save feature engineer
#         joblib.dump(self.feature_engineer, f'{model_dir}/feature_engineer.pkl')
        
#         # Save metadata
#         metadata = {
#             'model_name': self.best_model,
#             'model_type': type(model_info['model']).__name__,
#             'metrics': model_info['metrics'],
#             'params': model_info['params'],
#             'feature_names': feature_names,
#             'training_date': datetime.now().isoformat(),
#             'version': '1.0'
#         }
        
#         import json
#         with open(f'{model_dir}/metadata.json', 'w') as f:
#             json.dump(metadata, f, indent=2)
        
#         self.logger.info(f"Production model saved to {model_dir}")
        
#         # Also save versioned copy
#         version_dir = f'/app/models/{datetime.now().strftime("%Y%m%d_%H%M%S")}'
#         os.makedirs(version_dir, exist_ok=True)
#         joblib.dump(model_info['model'], f'{version_dir}/model.pkl')
#         joblib.dump(self.feature_engineer, f'{version_dir}/feature_engineer.pkl')
#         with open(f'{version_dir}/metadata.json', 'w') as f:
#             json.dump(metadata, f, indent=2)
        
#         return metadata
    
#     def run_pipeline(self):
#         """Run the complete training pipeline"""
#         try:
#             self.logger.info("Starting ML training pipeline...")
            
#             # Load and prepare data
#             X_train, X_test, y_train, y_test, feature_names = self.load_and_prepare_data()
            
#             # Train models
#             self.train_models(X_train, y_train, X_test, y_test)
            
#             # Save production model
#             metadata = self.save_production_model(feature_names)
            
#             self.logger.info("ML training pipeline completed successfully!")
#             return metadata
            
#         except Exception as e:
#             self.logger.error(f"Training pipeline failed: {str(e)}")
#             raise

# def main():
#     pipeline = MLTrainingPipeline()
#     pipeline.run_pipeline()

# if __name__ == "__main__":
#     main()

"""
Production ML Training Pipeline
MLflow DISABLED - no tracking server needed
"""

import os
import sys
import pandas as pd
import numpy as np
import joblib
# import mlflow  # DISABLED - no server
# import mlflow.sklearn  # DISABLED
from datetime import datetime
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings
warnings.filterwarnings('ignore')

from .data_loader import MLDataLoader
from .feature_engineer import FeatureEngineer
from .model_evaluator import ModelEvaluator
import logging

class MLTrainingPipeline:
    def __init__(self):
        self.logger = logging.getLogger('ml_training')  # FIXED
        self.models = {}
        self.best_model = None
        self.feature_engineer = FeatureEngineer()
        self.evaluator = ModelEvaluator()
        
        # MLflow setup - DISABLED
        # mlflow.set_tracking_uri(os.getenv('MLFLOW_TRACKING_URI', 'http://localhost:5000'))
        # mlflow.set_experiment('flight_fare_prediction')
        self.logger.info("MLflow tracking disabled - training locally only")
    
    def train_baseline_models(self, X_train, y_train, X_test, y_test):
        """Train multiple baseline models"""
        
        models_config = {
            'linear_regression': {
                'model': LinearRegression(),
                'params': {}
            },
            'ridge': {
                'model': Ridge(),
                'params': {'alpha': [0.1, 1.0, 10.0]}
            },
            'lasso': {
                'model': Lasso(max_iter=2000),
                'params': {'alpha': [0.1, 1.0, 10.0]}
            },
            'random_forest': {
                'model': RandomForestRegressor(random_state=42, n_jobs=-1),
                'params': {
                    'n_estimators': [100, 200],
                    'max_depth': [10, 20],
                    'min_samples_split': [2, 5]
                }
            }
        }
        
        best_score = -np.inf
        
        for model_name, config in models_config.items():
            self.logger.info(f"Training {model_name}...")
            
            # Disabled MLflow context
            # with mlflow.start_run(run_name=f"{model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            
            # Hyperparameter tuning
            if config['params']:
                grid_search = GridSearchCV(
                    config['model'], 
                    config['params'], 
                    cv=3,  # Reduced from 5 for speed
                    scoring='r2',
                    n_jobs=-1,
                    verbose=0
                )
                grid_search.fit(X_train, y_train)
                best_model = grid_search.best_estimator_
                best_params = grid_search.best_params_
            else:
                best_model = config['model']
                best_model.fit(X_train, y_train)
                best_params = {}
            
            # Predictions
            y_pred_train = best_model.predict(X_train)
            y_pred_test = best_model.predict(X_test)
            
            # Metrics
            metrics = {
                'train_r2': r2_score(y_train, y_pred_train),
                'test_r2': r2_score(y_test, y_pred_test),
                'train_mae': mean_absolute_error(y_train, y_pred_train),
                'test_mae': mean_absolute_error(y_test, y_pred_test),
                'train_rmse': np.sqrt(mean_squared_error(y_train, y_pred_train)),
                'test_rmse': np.sqrt(mean_squared_error(y_test, y_pred_test)),
                'train_mape': np.mean(np.abs((y_train - y_pred_train) / y_train)) * 100 if np.sum(y_train) != 0 else 0,
                'test_mape': np.mean(np.abs((y_test - y_pred_test) / y_test)) * 100 if np.sum(y_test) != 0 else 0,
            }
            
            # MLflow logging - DISABLED
            # mlflow.log_params(best_params)
            # mlflow.log_metrics(metrics)
            # mlflow.sklearn.log_model(best_model, f"model_{model_name}")
            
            # Store model
            self.models[model_name] = {
                'model': best_model,
                'metrics': metrics,
                'params': best_params,
                'name': model_name
            }
            
            # Track best model
            if metrics['test_r2'] > best_score:
                best_score = metrics['test_r2']
                self.best_model = model_name
            
            self.logger.info(f"   {model_name}: R²={metrics['test_r2']:.4f}, MAE={metrics['test_mae']:.2f}")
        
        self.logger.info(f"Best model: {self.best_model} (R²={best_score:.4f})")
        return self.models
    
    def get_best_model_info(self):
        """Get info about best model"""
        if not self.best_model:
            raise ValueError("No models trained yet")
        return self.models[self.best_model]
