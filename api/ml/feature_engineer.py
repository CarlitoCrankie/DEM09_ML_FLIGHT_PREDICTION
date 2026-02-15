"""
Feature Engineering - API Version (Lightweight)
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
import logging

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Feature engineering for flight fare prediction"""
    
    def __init__(self):
        self.label_encoders = {}
        self.scaler = None
        self.feature_names = []
        self.numerical_fill_values = {}
    
    def create_derived_features(self, df):
        """Create derived business features"""
        df = df.copy()
        
        # Route popularity - calculate from route column
        if 'route' in df.columns and 'route_popularity' not in df.columns:
            df['route_popularity'] = 1  # Default for unseen routes
            df['route_popularity_log'] = np.log1p(df['route_popularity'])
        
        # Peak season indicator
        if 'is_peak_season' in df.columns:
            df['is_peak_season_numeric'] = df['is_peak_season'].fillna(False).astype(int)
        
        # Route type - derive from source/destination if available
        if 'route_type' not in df.columns:
            # Set a default or derive from codes
            df['route_type'] = 'Domestic'  # Default
        
        # Season category - map from seasonality if not present
        if 'season_category' not in df.columns and 'seasonality' in df.columns:
            season_map = {
                'Regular': 'Regular',
                'Peak': 'Peak', 
                'Off-Peak': 'Off-Peak',
                'Eid': 'Peak',
                'Summer': 'Peak',
                'Winter': 'Regular'
            }
            df['season_category'] = df['seasonality'].map(season_map).fillna('Regular')
        
        return df
    
    def handle_missing_values(self, df, fit=True):
        """Handle missing values"""
        df = df.copy()
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        exclude_cols = ['total_fare_bdt', 'id']
        numerical_cols = [col for col in numerical_cols if col not in exclude_cols]
        
        if fit:
            for col in numerical_cols:
                if df[col].isna().any():
                    fill_value = df[col].median()
                    if pd.isna(fill_value):
                        fill_value = 0
                    self.numerical_fill_values[col] = fill_value
        
        for col in numerical_cols:
            if col in self.numerical_fill_values:
                df[col] = df[col].fillna(self.numerical_fill_values[col])
        
        return df
    
    def encode_categorical_features(self, df, fit=True):
        """Encode categorical variables - INCLUDING route"""
        df = df.copy()
        categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
        exclude_cols = ['total_fare_bdt', 'id']
        categorical_cols = [col for col in categorical_cols if col not in exclude_cols]
        
        for col in categorical_cols:
            df[col] = df[col].fillna('missing').astype(str)
            
            if fit:
                le = LabelEncoder()
                df[f'{col}_encoded'] = le.fit_transform(df[col])
                self.label_encoders[col] = le
            else:
                if col in self.label_encoders:
                    le = self.label_encoders[col]
                    df[f'{col}_encoded'] = df[col].map(
                        lambda x: le.transform([x])[0] if x in le.classes_ else -1
                    )
                else:
                    # If encoder doesn't exist (new category), assign -1
                    df[f'{col}_encoded'] = -1
        
        df = df.drop(columns=categorical_cols, errors='ignore')
        return df
    
    def scale_numerical_features(self, df, fit=True):
        """Scale numerical features"""
        df = df.copy()
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        exclude_cols = ['total_fare_bdt', 'id']
        exclude_patterns = ['_encoded']
        
        numerical_cols = [
            col for col in numerical_cols 
            if col not in exclude_cols and not any(pattern in col for pattern in exclude_patterns)
        ]
        
        if numerical_cols:
            if fit:
                self.scaler = StandardScaler()
                df[numerical_cols] = self.scaler.fit_transform(df[numerical_cols])
            else:
                if self.scaler:
                    df[numerical_cols] = self.scaler.transform(df[numerical_cols])
        
        return df
    
    def final_nan_check(self, df):
        """Final NaN check"""
        nan_cols = df.columns[df.isna().any()].tolist()
        
        if nan_cols:
            for col in nan_cols:
                if df[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
                    fill_value = df[col].median()
                    if pd.isna(fill_value):
                        fill_value = 0
                    df[col] = df[col].fillna(fill_value)
                else:
                    df[col] = df[col].fillna(0)
        
        return df
    
    def engineer_features(self, df, fit=True):
        """Main feature engineering pipeline"""
        df = self.create_derived_features(df)
        df = self.handle_missing_values(df, fit=fit)
        df = self.encode_categorical_features(df, fit=fit)
        df = self.scale_numerical_features(df, fit=fit)
        df = self.final_nan_check(df)
        
        if fit:
            self.feature_names = [col for col in df.columns if col != 'total_fare_bdt']
        
        return df
