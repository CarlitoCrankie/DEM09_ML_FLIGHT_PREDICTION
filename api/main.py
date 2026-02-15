"""
FastAPI REST API for Flight Fare Prediction
Serves predictions from trained Random Forest model
"""

import sys
sys.path.insert(0, '/app/api')

import os
import json
import joblib
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Pydantic models
class FlightPredictionRequest(BaseModel):
    airline: str = Field(..., example="Biman Bangladesh Airlines")
    source_code: str = Field(..., example="DAC")
    destination_code: str = Field(..., example="CXB")
    travel_class: str = Field(..., example="Economy")
    seasonality: str = Field(..., example="Regular")
    is_peak_season: bool = Field(False, example=False)

class FlightPredictionResponse(BaseModel):
    predicted_fare_bdt: float
    model_name: str
    model_version: str
    prediction_timestamp: str
    confidence_interval: dict

class ModelStatus(BaseModel):
    model_loaded: bool
    model_name: str
    model_version: str
    last_training_date: str
    test_r2: float
    test_mae: float

# Prediction Service
class PredictionService:
    def __init__(self):
        self.model = None
        self.feature_engineer = None
        self.feature_selector = None
        self.metadata = None
        self.load_model()
    
    def load_model(self):
        """Load trained model and artifacts"""
        model_path = os.getenv('MODEL_PATH', '/app/models/latest')
        
        try:
            print(f"Loading model from {model_path}...")
            self.model = joblib.load(f'{model_path}/model.pkl')
            print(f"✅ Model loaded")
            
            self.feature_engineer = joblib.load(f'{model_path}/feature_engineer.pkl')
            print(f"✅ Feature engineer loaded")
            
            self.feature_selector = joblib.load(f'{model_path}/feature_selector.pkl')
            print(f"✅ Feature selector loaded")
            
            with open(f'{model_path}/metadata.json', 'r') as f:
                self.metadata = json.load(f)
            
            print(f"✅ Model loaded: {self.metadata['model_name']}")
            print(f"   R²: {self.metadata['metrics']['test_r2']:.4f}")
            print(f"   MAE: {self.metadata['metrics']['test_mae']:.2f}")
            
        except Exception as e:
            print(f"❌ Failed to load model: {e}")
            import traceback
            traceback.print_exc()
            self.model = None
    
    def predict(self, request: FlightPredictionRequest) -> FlightPredictionResponse:
        """Make fare prediction"""
        if not self.model:
            raise HTTPException(status_code=503, detail="Model not loaded")
        
        try:
            # Convert request to DataFrame
            input_data = pd.DataFrame([{
                'airline': request.airline,
                'source_code': request.source_code,
                'destination_code': request.destination_code,
                'travel_class': request.travel_class,
                'seasonality': request.seasonality,
                'is_peak_season': request.is_peak_season,
                'route': f"{request.source_code}_to_{request.destination_code}"
            }])
            
            # Apply feature engineering
            features = self.feature_engineer.engineer_features(input_data, fit=False)
            
            # Select features used in training
            selected_features = self.metadata['selected_features']
            
            # Ensure all features exist (fill missing with 0)
            for feature in selected_features:
                if feature not in features.columns:
                    features[feature] = 0
            
            # Select and order features
            features = features[selected_features]
            
            # Make prediction
            prediction = self.model.predict(features)[0]
            
            # Calculate confidence interval
            rmse = self.metadata['metrics'].get('test_rmse', 50000)
            confidence_interval = {
                'lower': max(0, prediction - 1.96 * rmse),
                'upper': prediction + 1.96 * rmse
            }
            
            return FlightPredictionResponse(
                predicted_fare_bdt=round(prediction, 2),
                model_name=self.metadata['model_name'],
                model_version=self.metadata['version'],
                prediction_timestamp=datetime.now().isoformat(),
                confidence_interval=confidence_interval
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")
    
    def get_status(self) -> ModelStatus:
        """Get model status"""
        if not self.metadata:
            return ModelStatus(
                model_loaded=False,
                model_name="None",
                model_version="None",
                last_training_date="Never",
                test_r2=0.0,
                test_mae=0.0
            )
        
        return ModelStatus(
            model_loaded=True,
            model_name=self.metadata['model_name'],
            model_version=self.metadata['version'],
            last_training_date=self.metadata['training_date'],
            test_r2=self.metadata['metrics']['test_r2'],
            test_mae=self.metadata['metrics']['test_mae']
        )

# Initialize FastAPI
app = FastAPI(
    title="Flight Fare Prediction API",
    description="Predict Bangladesh flight fares using Random Forest model",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize service
print("Initializing Prediction Service...")
prediction_service = PredictionService()

# Routes
@app.get("/")
async def root():
    return {"message": "Flight Fare Prediction API", "status": "running"}

@app.post("/predict", response_model=FlightPredictionResponse)
async def predict_fare(request: FlightPredictionRequest):
    """Predict flight fare"""
    return prediction_service.predict(request)

@app.get("/status", response_model=ModelStatus)
async def get_status():
    """Get model status"""
    return prediction_service.get_status()

@app.post("/reload")
async def reload_model():
    """Reload model (after retraining)"""
    prediction_service.load_model()
    return {"message": "Model reloaded", "status": "success"}

@app.get("/health")
async def health():
    """Health check"""
    return {
        "status": "healthy",
        "model_loaded": prediction_service.model is not None,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", 8000)),
        reload=False
    )
