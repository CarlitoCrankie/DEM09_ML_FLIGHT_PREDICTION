"""
Smart Feature Selection
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import mutual_info_regression
import logging

logger = logging.getLogger(__name__)

class SmartFeatureSelector:
    """Select most important features"""
    
    def __init__(self):
        self.selected_features = []
        self.feature_importance_scores = {}
    
    def calculate_feature_importance(self, X, y, method='random_forest'):
        """Calculate feature importance"""
        logger.info(f"🔍 Calculating feature importance using {method}...")
        
        if method == 'random_forest':
            rf = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            rf.fit(X, y)
            importances = rf.feature_importances_
            
        elif method == 'mutual_info':
            importances = mutual_info_regression(X, y, random_state=42)
        
        importance_df = pd.DataFrame({
            'feature': X.columns,
            'importance': importances
        }).sort_values('importance', ascending=False)
        
        return importance_df
    
    def select_top_features(self, X, y, top_k=50):
        """Select top K features"""
        logger.info(f"🎯 Selecting top {top_k} features...")
        
        rf_importance = self.calculate_feature_importance(X, y, 'random_forest')
        mi_importance = self.calculate_feature_importance(X, y, 'mutual_info')
        
        rf_importance['rf_rank'] = rf_importance['importance'].rank(ascending=False)
        mi_importance['mi_rank'] = mi_importance['importance'].rank(ascending=False)
        
        combined = rf_importance[['feature', 'rf_rank']].merge(
            mi_importance[['feature', 'mi_rank']], 
            on='feature'
        )
        combined['avg_rank'] = (combined['rf_rank'] + combined['mi_rank']) / 2
        combined = combined.sort_values('avg_rank')
        
        self.selected_features = combined.head(top_k)['feature'].tolist()
        self.feature_importance_scores = combined.set_index('feature')['avg_rank'].to_dict()
        
        logger.info(f"✅ Selected {len(self.selected_features)} features")
        logger.info(f"   Top 10: {self.selected_features[:10]}")
        
        return self.selected_features
