"""
Model Trainer - Updated to include Gradient Boosting
"""

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, Ridge, Lasso
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import logging

logger = logging.getLogger(__name__)


class MLTrainingPipeline:
    """Trains and compares multiple regression models"""

    def __init__(self):
        self.models = {
            'linear_regression': {
                'model': LinearRegression(),
                'needs_scaling': True
            },
            'ridge': {
                'model': Ridge(alpha=10.0),
                'needs_scaling': True
            },
            'lasso': {
                'model': Lasso(alpha=10.0, max_iter=10000),
                'needs_scaling': True
            },
            'random_forest': {
                'model': RandomForestRegressor(
                    n_estimators=200,
                    max_depth=15,
                    min_samples_split=5,
                    n_jobs=-1,
                    random_state=42
                ),
                'needs_scaling': False
            },
            'gradient_boosting': {
                'model': GradientBoostingRegressor(
                    n_estimators=200,
                    learning_rate=0.1,
                    max_depth=5,
                    min_samples_split=5,
                    subsample=0.8,
                    random_state=42
                ),
                'needs_scaling': False
            }
        }
        self.results = {}
        self.best_model = None
        self.best_model_name = None

    def train_all(self, X_train, y_train, X_test, y_test,
                  X_train_scaled=None, X_test_scaled=None):
        """Train all models and return results"""

        logger.info("MLflow tracking disabled - training locally only")
        logger.info(f"Training {len(self.models)} models...")

        for name, config in self.models.items():
            logger.info(f"Training {name}...")

            model = config['model']
            needs_scaling = config['needs_scaling']

            # Use scaled data if model needs it
            X_tr = X_train_scaled if (needs_scaling and X_train_scaled is not None) else X_train
            X_te = X_test_scaled if (needs_scaling and X_test_scaled is not None) else X_test

            try:
                # Train
                model.fit(X_tr, y_train)

                # Predict
                train_preds = model.predict(X_tr)
                test_preds = model.predict(X_te)

                # Metrics
                train_r2 = r2_score(y_train, train_preds)
                test_r2 = r2_score(y_test, test_preds)
                train_mae = mean_absolute_error(y_train, train_preds)
                test_mae = mean_absolute_error(y_test, test_preds)
                train_rmse = np.sqrt(mean_squared_error(y_train, train_preds))
                test_rmse = np.sqrt(mean_squared_error(y_test, test_preds))

                # MAPE
                train_mape = np.mean(np.abs((y_train - train_preds) / y_train)) * 100
                test_mape = np.mean(np.abs((y_test - test_preds) / y_test)) * 100

                # Cross-validation
                cv_scores = cross_val_score(model, X_tr, y_train,
                                           cv=5, scoring='r2', n_jobs=-1)

                self.results[name] = {
                    'model': model,
                    'metrics': {
                        'train_r2': train_r2,
                        'test_r2': test_r2,
                        'train_mae': train_mae,
                        'test_mae': test_mae,
                        'train_rmse': train_rmse,
                        'test_rmse': test_rmse,
                        'train_mape': train_mape,
                        'test_mape': test_mape,
                        'cv_r2_mean': cv_scores.mean(),
                        'cv_r2_std': cv_scores.std()
                    }
                }

                logger.info(
                    f"   {name}: R²={test_r2:.4f}, "
                    f"MAE={test_mae:,.2f}, "
                    f"CV={cv_scores.mean():.4f}±{cv_scores.std():.4f}"
                )

            except Exception as e:
                logger.error(f"   ❌ {name} failed: {e}")
                continue

        # Select best model by test R²
        best_name = max(
            self.results,
            key=lambda k: self.results[k]['metrics']['test_r2']
        )
        self.best_model_name = best_name
        self.best_model = self.results[best_name]['model']

        logger.info(
            f"Best model: {best_name} "
            f"(R²={self.results[best_name]['metrics']['test_r2']:.4f})"
        )

        return {
            'best_model_name': best_name,
            'best_model': self.best_model,
            'metrics': self.results[best_name]['metrics'],
            'all_results': {
                k: v['metrics'] for k, v in self.results.items()
            }
        }

    def get_comparison_table(self):
        """Return a DataFrame comparing all models"""
        if not self.results:
            return pd.DataFrame()

        rows = []
        for name, result in self.results.items():
            m = result['metrics']
            rows.append({
                'Model': name,
                'Train R²': round(m['train_r2'], 4),
                'Test R²': round(m['test_r2'], 4),
                'CV R² Mean': round(m.get('cv_r2_mean', 0), 4),
                'Test MAE': round(m['test_mae'], 2),
                'Test RMSE': round(m['test_rmse'], 2),
                'Test MAPE': round(m['test_mape'], 2)
            })

        df = pd.DataFrame(rows).sort_values('Test R²', ascending=False)
        df = df.set_index('Model')
        return df