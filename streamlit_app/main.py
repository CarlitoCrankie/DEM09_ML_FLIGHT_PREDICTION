"""
Streamlit App for Flight Fare Prediction
Interactive UI for making predictions
"""

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Flight Fare Predictor",
    page_icon="✈️",
    layout="wide"
)

# API URL
API_URL = "http://flight-api:8000"

# Functions
def get_model_status():
    """Get model status from API"""
    try:
        response = requests.get(f"{API_URL}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def predict_fare(flight_data):
    """Make prediction via API"""
    try:
        response = requests.post(f"{API_URL}/predict", json=flight_data, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Prediction failed: {response.text}")
            return None
    except Exception as e:
        st.error(f"API connection failed: {str(e)}")
        return None

# Main app
def main():
    st.title("✈️ Bangladesh Flight Fare Predictor")
    st.markdown("Predict flight fares using machine learning")
    
    # Sidebar - Model Status
    st.sidebar.header("🤖 Model Status")
    status = get_model_status()
    
    if status and status['model_loaded']:
        st.sidebar.success("✅ Model Loaded")
        st.sidebar.metric("Model", status['model_name'])
        st.sidebar.metric("R² Score", f"{status['test_r2']:.4f}")
        st.sidebar.metric("MAE", f"৳{status['test_mae']:,.0f}")
        st.sidebar.info(f"Last trained: {status['last_training_date'][:10]}")
    else:
        st.sidebar.error("❌ Model Not Available")
        st.error("Model is not loaded. Please check the API service.")
        return
    
    # Main content
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("🎯 Flight Details")
        
        with st.form("flight_form"):
            airline = st.selectbox(
                "Airline",
                ["Biman Bangladesh Airlines", "US-Bangla Airlines", "Novoair", 
                 "Regent Airways", "Air Astra"]
            )
            
            col_route = st.columns(2)
            with col_route[0]:
                source = st.selectbox("From", ["DAC", "CXB", "JSR", "ZYL", "RJH", "SPD"])
            
            with col_route[1]:
                destination = st.selectbox("To", ["DAC", "CXB", "JSR", "ZYL", "RJH", "SPD"])
            
            travel_class = st.selectbox("Class", ["Economy", "Business", "First"])
            
            seasonality = st.selectbox(
                "Season",
                ["Regular", "Eid", "Winter Holidays", "Hajj"]
            )
            
            is_peak = st.checkbox("Peak Season")
            
            submitted = st.form_submit_button("🔮 Predict Fare", type="primary")
    
    with col2:
        st.header("📊 Prediction Results")
        
        if submitted:
            if source == destination:
                st.error("Source and destination cannot be the same!")
                return
            
            flight_data = {
                "airline": airline,
                "source_code": source,
                "destination_code": destination,
                "travel_class": travel_class,
                "seasonality": seasonality,
                "is_peak_season": is_peak
            }
            
            with st.spinner("Predicting fare..."):
                result = predict_fare(flight_data)
            
            if result:
                predicted_fare = result['predicted_fare_bdt']
                confidence = result['confidence_interval']
                
                st.success(f"💰 Predicted Fare: **৳{predicted_fare:,.2f}**")
                st.info(f"📈 Range: ৳{confidence['lower']:,.0f} - ৳{confidence['upper']:,.0f}")
                
                # Visualization
                fig = go.Figure()
                
                fig.add_trace(go.Bar(
                    x=['Predicted Fare'],
                    y=[predicted_fare],
                    name='Prediction',
                    marker_color='lightblue',
                    text=[f"৳{predicted_fare:,.0f}"],
                    textposition='outside'
                ))
                
                fig.add_trace(go.Scatter(
                    x=['Predicted Fare', 'Predicted Fare'],
                    y=[confidence['lower'], confidence['upper']],
                    mode='markers',
                    name='95% Confidence',
                    marker=dict(color='red', size=10, symbol='diamond')
                ))
                
                fig.update_layout(
                    title="Fare Prediction with Confidence Interval",
                    yaxis_title="Fare (৳)",
                    showlegend=True,
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("📋 Prediction Details"):
                    st.write(f"**Model**: {result['model_name']}")
                    st.write(f"**Version**: {result['model_version']}")
                    st.write(f"**Timestamp**: {result['prediction_timestamp'][:19]}")

if __name__ == "__main__":
    main()
