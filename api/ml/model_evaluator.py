"""
Model evaluation utilities
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib

class ModelEvaluator:
    def __init__(self):
        self.evaluation_results = {}

                
    def evaluate_model(self, model, X_test, y_test, model_name):
        """Comprehensive model evaluation"""
        
        # Predictions
        y_pred = model.predict(X_test)
        
        # Metrics
        metrics = {
            'r2': r2_score(y_test, y_pred),
            'mae': mean_absolute_error(y_test, y_pred),
            'rmse': np.sqrt(mean_squared_error(y_test, y_pred)),
            'mape': np.mean(np.abs((y_test - y_pred) / y_test)) * 100
        }
        
        # Store results
        self.evaluation_results[model_name] = {
            'metrics': metrics,
            'predictions': y_pred,
            'actuals': y_test
        }
        
        return metrics
    
    def plot_predictions(self, model_name, save_path=None):
        """Plot actual vs predicted values"""
        if model_name not in self.evaluation_results:
            raise ValueError(f"No evaluation results for {model_name}")
        
        results = self.evaluation_results[model_name]
        y_test = results['actuals']
        y_pred = results['predictions']
        
        plt.figure(figsize=(10, 8))
        
        # Scatter plot
        plt.scatter(y_test, y_pred, alpha=0.5)
        
        # Perfect prediction line
        min_val = min(y_test.min(), y_pred.min())
        max_val = max(y_test.max(), y_pred.max())
        plt.plot([min_val, max_val], [min_val, max_val], 'r--', lw=2)
        
        plt.xlabel('Actual Fare')
        plt.ylabel('Predicted Fare')
        plt.title(f'{model_name} - Actual vs Predicted')
        
        # Add R² to plot
        r2 = results['metrics']['r2']
        plt.text(0.05, 0.95, f'R² = {r2:.4f}', transform=plt.gca().transAxes, 
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    def generate_report(self):
        """Generate evaluation report"""
        if not self.evaluation_results:
            return "No evaluation results available"
        
        report_df = pd.DataFrame({
            model: results['metrics'] 
            for model, results in self.evaluation_results.items()
        }).T
        
        return report_df.round(4)
