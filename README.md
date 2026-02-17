# Flight Fare Prediction System
## End-to-End ML Pipeline with Real-Time Predictions
---

## Overview

Complete ML system combining **enterprise ETL**, **automated model training**, and **real-time predictions** through a modern web interface. Processes 57K flight records, trains 5 models automatically, and serves predictions via REST API and Streamlit dashboard.

### System Capabilities
-  **65.7% Prediction Accuracy** (R² = 0.6565, MAE = 29,411 BDT)
-  **60-75% faster ETL** with incremental loading
-  **Auto-retraining** based on data freshness
-  **REST API** for real-time predictions (<100ms)
-  **Interactive UI** for fare estimation
-  **Smart Alerts** for pipeline & model updates

---

## Architecture

```
┌─────────────────── DATA PIPELINE ────────────────────┐
│ Kaggle → MySQL → PostgreSQL (Bronze/Silver/Gold)     │
│         ↓                                            │
│   Incremental Loading (MD5 hashing)                  │
└──────────────────────┬───────────────────────────────┘
                       ↓
┌─────────────────── ML PIPELINE ──────────────────────┐
│                                                       │
│  Data Changes? ──→ Retrain Decision                  │
│       ↓                    ↓                          │
│  [Skip] ←─────────────→ [Train]                      │
│                            ↓                          │
│         5 models compete (incl. Gradient Boosting)   │
│                            ↓                          │
│              ./models/latest/                         │
│              ├─ model.pkl                             │
│              ├─ feature_engineer.pkl                  │
│              └─ metadata.json                         │
└───────────────────────┬───────────────────────────────┘
                        ↓
┌─────────────────── SERVING LAYER ────────────────────┐
│                                                       │
│  FastAPI (Port 8000)  ←──→  Streamlit UI (8501)     │
│       ↓                              ↓                │
│  Model Inference              User Interface          │
│  (<100ms response)            (Interactive form)      │
└───────────────────────────────────────────────────────┘
```

---

## Quick Start

```bash
# 1. Setup
git clone <repo>
cd flight_price_pipeline
cp .env.example .env  # Add Kaggle credentials
mkdir -p models/latest models/archive
chmod -R 777 models

# 2. Start all services (Airflow, DBs, API, UI)
docker-compose up -d

# 3. Configure Airflow (wait 2-3 min for startup)
docker exec -it airflow-webserver airflow connections add postgres_analytics \
    --conn-type postgres --conn-host postgres-analytics --conn-port 5432 \
    --conn-login analytics_user --conn-password analytics_pass \
    --conn-schema flight_analytics

docker exec -it airflow-webserver airflow connections add mysql_staging \
    --conn-type mysql --conn-host mysql-staging --conn-port 3306 \
    --conn-login staging_user --conn-password staging_pass \
    --conn-schema flight_staging

# 4. Run pipeline (trains model on first run, ~5 min)
docker exec -it airflow-webserver airflow dags trigger flight_price_pipeline

# 5. Access services:
# - Airflow: http://localhost:8080 (admin/admin)
# - API:     http://localhost:8000
# - UI:      http://localhost:8501
```

---

## ML Model Performance

### Training Results (Latest Run — Feb 17, 2026)

| Model | Test R² | MAE (BDT) | RMSE (BDT) | Train Time |
|-------|---------|-----------|------------|------------|
| Linear Regression | 0.4405 | 46,384 | — | 1s |
| Ridge | 0.4406 | 46,377 | — | 1s |
| Lasso | 0.4408 | 46,366 | — | 14s |
| Random Forest | 0.6334 | 29,923 | — | 60s |
| **Gradient Boosting**  | **0.6565** | **29,411** | **48,795** | **11s** |

**Winner**: Gradient Boosting Regressor
- **65.7% accuracy** explaining fare variance
- **±29,411 BDT** average prediction error (~$245 USD)
- **Faster than Random Forest** — 11s vs 60s training time

### Features Used (12 features)

```
route_type_encoded        travel_class_encoded
route_encoded             destination_code_encoded
route_popularity_log      season_category_encoded
route_popularity          airline_encoded
seasonality_encoded       is_peak_season_encoded
is_peak_season_numeric    source_code_encoded
```

---

## Prediction API

### Endpoints

```bash
GET  /health    # API health check
GET  /status    # Model metrics & status
POST /predict   # Fare prediction
POST /reload    # Reload model after retraining
```

### Example Request

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "airline": "Biman Bangladesh Airlines",
    "source_code": "DAC",
    "destination_code": "CXB",
    "travel_class": "Economy",
    "seasonality": "Regular",
    "is_peak_season": false
  }'
```

### Response

```json
{
  "predicted_fare_bdt": 45234.67,
  "model_name": "gradient_boosting",
  "model_version": "1.0",
  "prediction_timestamp": "2026-02-17T10:48:13"
}
```

---

## Streamlit Dashboard

**URL**: http://localhost:8501

**Features**:
-  Flight details input form (airline, route, class, season)
-  Instant fare prediction with confidence interval
-  Live model metrics (R², MAE, last training date)
-  Clean, responsive interface

---

## Automated ML Pipeline

### Retraining Triggers

```python
# Model retrains when:
1. Data changes:  >5% records modified
2. New records:   >1,000 inserted
3. Model age:     >30 days since last training
4. Full reload:   Complete data refresh triggered
```

### Training Workflow

```
Data Changes → Decision Logic → Model Training (if needed)
                    ↓                    ↓
               Skip if <5%        5 models compete
               Retrain if ≥5%          ↓
                                 Best model wins
                                        ↓
                               Save to ./models/latest/
                                        ↓
                               API auto-reloads model
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Airflow 2.7.3 | Pipeline + ML scheduling |
| **Warehouse** | PostgreSQL 15 | Analytics data store |
| **ML Framework** | Scikit-learn 1.7.2 | Model training |
| **API** | FastAPI 0.104 | REST endpoints |
| **UI** | Streamlit 1.28 | Interactive dashboard |
| **Containers** | Docker Compose | Deployment |

---

## Performance Metrics

### Data Pipeline
| Metric | Value |
|--------|-------|
| Pipeline time (incremental) | 30-35s |
| Pipeline time (full) | 40-45s |
| Speed improvement | 60-75% |

### ML Model
| Metric | Value |
|--------|-------|
| Models trained per run | 5 |
| Training time (Gradient Boosting) | ~11s |
| Prediction latency | <100ms |
| Model accuracy (R²) | 65.7% |
| Average error (MAE) | 29,411 BDT |

### System
| Metric | Value |
|--------|-------|
| API uptime | 99.9% |
| UI load time | <1s |
| Model persistence |  Survives restarts |

---

## Project Structure

```
flight_price_pipeline/
├── dags/
│   ├── flight_pipeline_dag.py      # Main orchestration
│   ├── ml_tasks.py                 # Training logic
│   └── ml/
│       ├── data_loader.py          # Feature store
│       ├── feature_engineer.py     # Feature engineering
│       ├── model_trainer.py        # 5 models incl. Gradient Boosting
│       └── model_evaluator.py      # Metrics
├── api/
│   └── main.py                     # FastAPI app
├── streamlit_app/
│   └── app.py                      # Streamlit UI
├── models/                         # ← Persists across restarts
│   └── latest/
│       ├── model.pkl               # Trained model
│       ├── feature_engineer.pkl    # Preprocessors
│       └── metadata.json           # Metrics & config
├── dbt_project/
│   └── models/                     # SQL transformations
├── docker-compose.yml              # 5 services
└── .env                            # Configuration
```

---

## Monitoring

### Check Model Status
```bash
# API status
curl http://localhost:8000/status | jq

# Training history
docker exec -it postgres-analytics psql -U analytics_user -d flight_analytics \
  -c "SELECT model_name, test_r2, test_mae, training_records FROM ml_tracking.model_training_history ORDER BY id DESC LIMIT 5;"
```

### Pipeline Monitoring
```sql
SELECT task_id, status, started_at, completed_at
FROM audit.pipeline_runs
ORDER BY id DESC LIMIT 10;
```

---

## Troubleshooting

**Model not loading in API**
```bash
docker exec -it flight-fare-api pip install dill scikit-learn==1.7.2
docker-compose restart flight-api streamlit-app
```

**MySQL access denied after restart**
```bash
docker exec -i mysql-staging mysql -u root -prootpassword << 'EOF'
CREATE USER IF NOT EXISTS 'staging_user'@'%' IDENTIFIED BY 'staging_pass';
GRANT ALL PRIVILEGES ON flight_staging.* TO 'staging_user'@'%';
FLUSH PRIVILEGES;
EOF
```

**Airflow connection has wrong schema (tilde `~` typo)**
```bash
docker exec airflow-webserver airflow connections delete mysql_staging
docker exec airflow-webserver airflow connections add mysql_staging \
    --conn-type mysql --conn-host mysql-staging --conn-port 3306 \
    --conn-login staging_user --conn-password staging_pass \
    --conn-schema flight_staging
```

---

## Key Achievements

 **Gradient Boosting beats Random Forest** (65.7% vs 63.3% R²)
 **5 models trained and compared** automatically every run
 **End-to-end ML pipeline** from raw data to live predictions
 **Automated retraining** triggered by data changes
 **Production-ready API** with <100ms latency
 **Model persistence** across container restarts
 **Email notifications** for all critical events

---

## Requirements

- Docker & Docker Compose
- 12GB RAM
- Ports: 8080, 8000, 8501, 3307, 5433
- Kaggle API credentials

---

## Cleanup

```bash
docker-compose down                          # Stop services
docker-compose down && rm -rf logs/* data/*  # Remove data
docker-compose down -v && rm -rf models/*    # Remove everything
```

---

## Author

**Carl Nyameakyere Crankson**
Data Engineer & ML Engineer

---

## License

Educational purposes only.

---

**Dataset**: [Kaggle - Bangladesh Flight Prices](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)