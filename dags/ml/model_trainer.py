import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import logging

logger = logging.getLogger(__name__)


class ModelTrainer:

    def __init__(self):
        self.models = {
            'linear_regression': {'model': LinearRegression(), 'needs_scaling': False},
            'ridge':             {'model': Ridge(alpha=10.0), 'needs_scaling': False},
            'lasso':             {'model': Lasso(alpha=10.0, max_iter=10000), 'needs_scaling': False},
            'random_forest':     {'model': RandomForestRegressor(n_estimators=200, max_depth=15,
                                      min_samples_split=5, n_jobs=-1, random_state=42), 'needs_scaling': False},
            'gradient_boosting': {'model': GradientBoostingRegressor(n_estimators=200, learning_rate=0.1,
                                      max_depth=5, min_samples_split=5, subsample=0.8, random_state=42),
                                  'needs_scaling': False},
        }
        self.results = {}
        self.best_model = None
        self.best_model_name = None

    def train_all(self, X_train, y_train, X_test, y_test):
        logger.info(f"Training {len(self.models)} models...")

        for name, config in self.models.items():
            logger.info(f"Training {name}...")
            model = config['model']
            try:
                model.fit(X_train, y_train)
                train_preds = model.predict(X_train)
                test_preds  = model.predict(X_test)

                train_r2   = r2_score(y_train, train_preds)
                test_r2    = r2_score(y_test, test_preds)
                test_mae   = mean_absolute_error(y_test, test_preds)
                test_rmse  = np.sqrt(mean_squared_error(y_test, test_preds))
                test_mape  = np.mean(np.abs((y_test - test_preds) / y_test)) * 100

                self.results[name] = {
                    'model': model,
                    'metrics': {
                        'train_r2': train_r2, 'test_r2': test_r2,
                        'test_mae': test_mae, 'test_rmse': test_rmse,
                        'test_mape': test_mape
                    }
                }
                logger.info(f"   {name}: R²={test_r2:.4f}, MAE={test_mae:,.2f}")

            except Exception as e:
                logger.error(f"   {name} failed: {e}")
                continue

        self.best_model_name = max(self.results, key=lambda k: self.results[k]['metrics']['test_r2'])
        self.best_model = self.results[self.best_model_name]['model']
        logger.info(f"Best model: {self.best_model_name} (R²={self.results[self.best_model_name]['metrics']['test_r2']:.4f})")

        return {
            'best_model_name': self.best_model_name,
            'best_model':      self.best_model,
            'metrics':         self.results[self.best_model_name]['metrics'],
            'all_results':     {k: v['metrics'] for k, v in self.results.items()}
        }
