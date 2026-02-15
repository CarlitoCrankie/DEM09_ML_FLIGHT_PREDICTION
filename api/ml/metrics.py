# src/evaluation/__init__.py
"""Evaluation module"""

# src/evaluation/metrics.py
"""
Custom evaluation metrics
"""
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def calculate_mape(y_true, y_pred):
    """Calculate Mean Absolute Percentage Error"""
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def calculate_all_metrics(y_true, y_pred):
    """Calculate all regression metrics"""
    return {
        'r2': r2_score(y_true, y_pred),
        'mae': mean_absolute_error(y_true, y_pred),
        'rmse': np.sqrt(mean_squared_error(y_true, y_pred)),
        'mape': calculate_mape(y_true, y_pred)
    }
