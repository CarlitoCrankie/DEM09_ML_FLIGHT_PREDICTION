"""
Feature Engineering for Flight Fare Prediction
FIXED VERSION - Handles NaN values properly
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, StandardScaler
import logging

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """Feature engineering for flight fare prediction with robust NaN handling"""
    
    def __init__(self):
        self.label_encoders = {}
        self.scaler = None
        self.feature_names = []
        self.numerical_fill_values = {}
    
    def create_derived_features(self, df):
        """Create derived business features"""
        logger.info(" Creating derived features...")
        
        df = df.copy()
        
        # Route popularity (if not already from Gold)
        if 'route' in df.columns and 'route_popularity' not in df.columns:
            route_counts = df['route'].value_counts()
            df['route_popularity'] = df['route'].map(route_counts)
            # Fill NaN with median
            df['route_popularity'] = df['route_popularity'].fillna(df['route_popularity'].median())
            df['route_popularity_log'] = np.log1p(df['route_popularity'])
            
        # Peak season indicator (already in data as is_peak_season)
        if 'is_peak_season' in df.columns:
            # Convert boolean to int, fill NaN with 0 (not peak)
            df['is_peak_season_numeric'] = df['is_peak_season'].fillna(False).astype(int)
        
        logger.info("    Created derived features")
        return df
    
    def handle_missing_values(self, df, fit=True):
        """
        Handle missing values in numerical features BEFORE encoding
        This is the critical step that was missing!
        """
        logger.info("🔧 Handling missing values...")
        
        df = df.copy()
        
        # Get numerical columns (exclude target and ID)
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        exclude_cols = ['total_fare_bdt', 'id']
        numerical_cols = [col for col in numerical_cols if col not in exclude_cols]
        
        if fit:
            # Store fill values for each column
            for col in numerical_cols:
                if df[col].isna().any():
                    # Use median for most columns
                    fill_value = df[col].median()
                    # If median is still NaN (all values are NaN), use 0
                    if pd.isna(fill_value):
                        fill_value = 0
                    self.numerical_fill_values[col] = fill_value
                    logger.info(f"   Column '{col}': {df[col].isna().sum()} NaN values, filling with {fill_value:.2f}")
        
        # Apply fill values
        for col in numerical_cols:
            if col in self.numerical_fill_values:
                df[col] = df[col].fillna(self.numerical_fill_values[col])
        
        logger.info(f"    Handled missing values in {len(numerical_cols)} numerical columns")
        
        return df
    
    def encode_categorical_features(self, df, fit=True):
        """Encode categorical variables and drop originals"""
        logger.info(" Encoding categorical features...")
        
        df = df.copy()
        
        # Identify categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'category', 'bool']).columns.tolist()
        
        # Remove target column if present
        exclude_cols = ['total_fare_bdt', 'id']
        categorical_cols = [col for col in categorical_cols if col not in exclude_cols]
        
        for col in categorical_cols:
            # Fill NaN in categorical columns with 'missing'
            df[col] = df[col].fillna('missing').astype(str)
            
            if fit:
                # Fit and transform
                le = LabelEncoder()
                df[f'{col}_encoded'] = le.fit_transform(df[col])
                self.label_encoders[col] = le
            else:
                # Transform only (using fitted encoder)
                if col in self.label_encoders:
                    le = self.label_encoders[col]
                    # Handle unseen categories
                    df[f'{col}_encoded'] = df[col].map(
                        lambda x: le.transform([x])[0] if x in le.classes_ else -1
                    )
        
        # DROP ORIGINAL CATEGORICAL COLUMNS
        df = df.drop(columns=categorical_cols)
        
        logger.info(f"    Encoded {len(categorical_cols)} categorical features")
        return df
    
    def scale_numerical_features(self, df, fit=True):
        """Scale numerical features"""
        logger.info(" Scaling numerical features...")
        
        df = df.copy()
        
        # Identify numerical columns (excluding target)
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Exclude target and already encoded columns
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
            
            logger.info(f"    Scaled {len(numerical_cols)} numerical features")
        
        return df
    
    def final_nan_check(self, df):
        """
        Final check and cleanup of any remaining NaN values
        """
        logger.info("🔍 Final NaN check...")
        
        # Check for any remaining NaN values
        nan_cols = df.columns[df.isna().any()].tolist()
        
        if nan_cols:
            logger.warning(f"    Found NaN values in {len(nan_cols)} columns: {nan_cols}")
            
            for col in nan_cols:
                nan_count = df[col].isna().sum()
                logger.warning(f"      {col}: {nan_count} NaN values")
                
                # Fill remaining NaN values
                if df[col].dtype in [np.float64, np.float32, np.int64, np.int32]:
                    # Numerical: fill with median or 0
                    fill_value = df[col].median()
                    if pd.isna(fill_value):
                        fill_value = 0
                    df[col] = df[col].fillna(fill_value)
                    logger.warning(f"      Filled with {fill_value}")
                else:
                    # Should not happen after encoding, but just in case
                    df[col] = df[col].fillna(0)
                    logger.warning(f"      Filled with 0")
        else:
            logger.info("    No NaN values found")
        
        return df
    
    def engineer_features(self, df, fit=True):
        """
        Main feature engineering pipeline with robust NaN handling
        
        Args:
            df (pd.DataFrame): Input data
            fit (bool): Whether to fit transformers
        
        Returns:
            pd.DataFrame: Engineered features
        """
        logger.info(" Starting feature engineering pipeline...")
        
        # Log initial state
        initial_nan = df.isna().sum().sum()
        logger.info(f"   Initial NaN count: {initial_nan}")
        
        # Create derived features
        df = self.create_derived_features(df)
        
        # CRITICAL: Handle missing values BEFORE encoding
        df = self.handle_missing_values(df, fit=fit)
        
        # Encode categorical features
        df = self.encode_categorical_features(df, fit=fit)
        
        # Scale numerical features
        df = self.scale_numerical_features(df, fit=fit)
        
        # Final NaN check and cleanup
        df = self.final_nan_check(df)
        
        # Store feature names (excluding target)
        if fit:
            self.feature_names = [col for col in df.columns if col != 'total_fare_bdt']
        
        # Final verification
        final_nan = df.isna().sum().sum()
        if final_nan > 0:
            logger.error(f"    WARNING: {final_nan} NaN values remain after feature engineering!")
            # Log which columns still have NaN
            nan_summary = df.isna().sum()
            nan_summary = nan_summary[nan_summary > 0]
            logger.error(f"   Columns with NaN: {nan_summary.to_dict()}")
        else:
            logger.info(f"    All NaN values handled successfully")
        
        logger.info(f" Feature engineering complete: {len(self.feature_names)} features")
        
        return df