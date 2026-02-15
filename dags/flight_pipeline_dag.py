"""
Flight Price Analysis Pipeline - WITH INCREMENTAL LOADING & ML RETRAINING
==========================================================================

Pipeline Flow:
    0. Extract from Kaggle (download, track metadata, evolve schema)
    1. Load CSV → MySQL (raw_flight_data)
    2. Validate data in MySQL (validated_flight_data)
    3. Transfer to PostgreSQL with INCREMENTAL LOADING (bronze.validated_flights)
    4. Run DBT transformations in PARALLEL:
       - Silver layer (cleaned data)
       - Gold layer (aggregated analytics)
    5. Retrain ML model (if new data detected)
    6. Send completion email with statistics
    
NEW FEATURES:
    - Email notifications (start, change detection, completion)
    - Parallel DBT execution (silver + gold simultaneously)
    - ML model retraining task (triggered on new data)
    - Rich email templates with statistics
"""

from datetime import datetime, timedelta
import subprocess
import logging
import hashlib
import json
import os
import zipfile

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.email import send_email
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql

from utils.logging_utils import log_pipeline_event, get_task_context
from utils.incremental_loader import IncrementalDataLoader

from ml_tasks import (
    decide_model_retraining,
    retrain_ml_model,
    skip_retraining,
    retraining_complete
)

logger = logging.getLogger("airflow.task")


# ============================================
# Configuration
# ============================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['carlcrankson966@gmail.com'], 
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'sla': timedelta(minutes=15),
}

# Kaggle dataset configuration
KAGGLE_DATASET = 'mahatiratusher/flight-price-dataset-of-bangladesh'
KAGGLE_FILENAME = 'Flight_Price_Dataset_of_Bangladesh.csv'
DATA_DIR = '/opt/airflow/data'
CSV_FILE_PATH = f'{DATA_DIR}/{KAGGLE_FILENAME}'
DBT_PROJECT_DIR = '/opt/airflow/dbt_project'
BATCH_SIZE = 5000

# Incremental loading configuration
FULL_LOAD_THRESHOLD = 50.0  # If >50% of data changed, do full reload

# ML model configuration
MODEL_DIR = '/opt/airflow/models'
MODEL_RETRAIN_THRESHOLD = 0.01  # Retrain if >1% new data (100 records for 10k dataset)

# Email configuration
ENABLE_EMAIL_NOTIFICATIONS = True  # Set to False to disable emails

# Column mapping: CSV column names → Database column names
COLUMN_MAPPING = {
    'Airline': 'airline',
    'Source': 'source_code',
    'Source Name': 'source_name',
    'Destination': 'destination_code',
    'Destination Name': 'destination_name',
    'Departure Date & Time': 'departure_datetime',
    'Arrival Date & Time': 'arrival_datetime',
    'Duration (hrs)': 'duration_hrs',
    'Stopovers': 'stopovers',
    'Aircraft Type': 'aircraft_type',
    'Class': 'travel_class',
    'Booking Source': 'booking_source',
    'Base Fare (BDT)': 'base_fare_bdt',
    'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
    'Total Fare (BDT)': 'total_fare_bdt',
    'Seasonality': 'seasonality',
    'Days Before Departure': 'days_before_departure'
}

# Python type to MySQL type mapping
PYTHON_TO_MYSQL_TYPE = {
    'int64': 'BIGINT',
    'float64': 'DECIMAL(12, 2)',
    'object': 'VARCHAR(255)',
    'datetime64[ns]': 'DATETIME',
    'bool': 'BOOLEAN',
}


# ============================================
# Email Notification Functions
# ============================================

def send_pipeline_start_email(**context):
    """Send email when pipeline starts."""
    if not ENABLE_EMAIL_NOTIFICATIONS:
        logger.info("Email notifications disabled - skipping start email")
        return
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    subject = f"🚀 Flight Price Pipeline Started - {dag_run_id}"
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; }}
            .info-box {{ background-color: #f0f0f0; padding: 15px; margin: 10px 0; border-radius: 5px; }}
            .footer {{ background-color: #f9f9f9; padding: 10px; text-align: center; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚀 Flight Price Pipeline Started</h1>
        </div>
        <div class="content">
            <h2>Pipeline Execution Details</h2>
            <div class="info-box">
                <strong>DAG:</strong> {dag_id}<br>
                <strong>Run ID:</strong> {dag_run_id}<br>
                <strong>Start Time:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                <strong>Triggered By:</strong> {context.get('dag_run').run_type}
            </div>
            
            <h3>Pipeline Steps:</h3>
            <ol>
                <li>Extract data from Kaggle</li>
                <li>Load to MySQL staging</li>
                <li>Validate data quality</li>
                <li>Incremental load to PostgreSQL</li>
                <li>DBT transformations (Silver + Gold in parallel)</li>
                <li>ML model retraining (if needed)</li>
            </ol>
            
            <p><em>You will receive updates when changes are detected and upon completion.</em></p>
        </div>
        <div class="footer">
            Flight Price Analytics Pipeline • Airflow Automated Notification
        </div>
    </body>
    </html>
    """
    
    try:
        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content
        )
        logger.info(f"✅ Start email sent to {default_args['email']}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send start email: {e}")


def send_change_detection_email(**context):
    """Send email after change detection with statistics."""
    if not ENABLE_EMAIL_NOTIFICATIONS:
        logger.info("Email notifications disabled - skipping change detection email")
        return
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    # Get metrics from XCom
    load_type = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='load_type')
    records_inserted = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_inserted')
    records_deleted = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_deleted')
    change_percentage = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='change_percentage')
    active_records = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='active_records')
    
    # Determine email style based on change level
    if change_percentage == 0:
        header_color = "#2196F3"  # Blue - no changes
        status_icon = "✓"
        status_text = "No Changes Detected"
    elif change_percentage < 5:
        header_color = "#4CAF50"  # Green - minor changes
        status_icon = "↻"
        status_text = "Minor Updates Detected"
    elif change_percentage < 50:
        header_color = "#FF9800"  # Orange - moderate changes
        status_icon = "⚠"
        status_text = "Moderate Changes Detected"
    else:
        header_color = "#F44336"  # Red - major changes
        status_icon = "⚠"
        status_text = "Major Changes Detected"
    
    subject = f"{status_icon} Flight Price Pipeline - {status_text} ({change_percentage:.1f}% changed)"
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: {header_color}; color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; }}
            .stats-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin: 20px 0; }}
            .stat-box {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; text-align: center; }}
            .stat-number {{ font-size: 32px; font-weight: bold; color: {header_color}; }}
            .stat-label {{ font-size: 14px; color: #666; }}
            .load-type {{ 
                display: inline-block; 
                padding: 5px 15px; 
                background-color: {header_color}; 
                color: white; 
                border-radius: 20px; 
                font-weight: bold;
            }}
            .footer {{ background-color: #f9f9f9; padding: 10px; text-align: center; font-size: 12px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f5f5f5; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>{status_icon} {status_text}</h1>
            <p>Incremental Load Completed</p>
        </div>
        <div class="content">
            <h2>Load Summary</h2>
            <p>Load Type: <span class="load-type">{load_type}</span></p>
            
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat-number">{records_inserted:,}</div>
                    <div class="stat-label">New Records</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{records_deleted:,}</div>
                    <div class="stat-label">Deleted Records</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{change_percentage:.2f}%</div>
                    <div class="stat-label">Change Percentage</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{active_records:,}</div>
                    <div class="stat-label">Total Active Records</div>
                </div>
            </div>
            
            <h3>What Happens Next?</h3>
            <table>
                <tr>
                    <th>Task</th>
                    <th>Status</th>
                </tr>
                <tr>
                    <td>✓ Data Extraction</td>
                    <td>Completed</td>
                </tr>
                <tr>
                    <td>✓ Data Validation</td>
                    <td>Completed</td>
                </tr>
                <tr>
                    <td>✓ Incremental Loading</td>
                    <td>Completed</td>
                </tr>
                <tr>
                    <td>⟳ DBT Silver Transformations</td>
                    <td>In Progress</td>
                </tr>
                <tr>
                    <td>⟳ DBT Gold Aggregations</td>
                    <td>In Progress</td>
                </tr>
                <tr>
                    <td>⟳ ML Model Retraining</td>
                    <td>{'Scheduled' if records_inserted > 0 else 'Skipped (no new data)'}</td>
                </tr>
            </table>
            
            <p><strong>Run ID:</strong> {dag_run_id}</p>
            <p><em>You will receive a final email when the entire pipeline completes.</em></p>
        </div>
        <div class="footer">
            Flight Price Analytics Pipeline • Airflow Automated Notification
        </div>
    </body>
    </html>
    """
    
    try:
        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content
        )
        logger.info(f"✅ Change detection email sent to {default_args['email']}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send change detection email: {e}")


def send_completion_email(**context):
    """Send final completion email with full pipeline statistics."""
    if not ENABLE_EMAIL_NOTIFICATIONS:
        logger.info("Email notifications disabled - skipping completion email")
        return
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    # Get all metrics from XCom
    load_type = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='load_type')
    records_inserted = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_inserted') or 0
    change_percentage = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='change_percentage') or 0
    active_records = context['ti'].xcom_pull(task_ids='transfer_to_postgres_incremental', key='active_records') or 0
    
    # Check if processing was skipped
    processing_decision = context['ti'].xcom_pull(task_ids='decide_processing', key='return_value')
    processing_skipped = (processing_decision == 'skip')
    
    # Get model retraining status (only if processing happened)
    if not processing_skipped:
        model_retrained = context['ti'].xcom_pull(task_ids='decide_model_retraining', key='return_value') == 'retrain'
    else:
        model_retrained = False
    
    # Calculate pipeline duration
    dag_run = context['dag_run']
    from datetime import timezone
    now_utc = datetime.now(timezone.utc)
    duration = (now_utc - dag_run.start_date).total_seconds()
    duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"
    
    # Adjust subject and header based on whether processing was skipped
    if processing_skipped:
        subject = f"✅ Flight Price Pipeline Completed - No Processing Needed (0% change)"
        header_text = "Pipeline Completed - No Changes"
        header_color = "#2196F3"  # Blue for skip
    else:
        subject = f"✅ Flight Price Pipeline Completed Successfully - {change_percentage:.1f}% data change"
        header_text = "Pipeline Completed Successfully"
        header_color = "#4CAF50"  # Green for success
    
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: {header_color}; color: white; padding: 20px; text-align: center; }}
            .content {{ padding: 20px; }}
            .success-box {{ background-color: #d4edda; border-left: 4px solid #28a745; padding: 15px; margin: 15px 0; }}
            .info-box {{ background-color: #d1ecf1; border-left: 4px solid #0c5460; padding: 15px; margin: 15px 0; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
            .stat-box {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; text-align: center; }}
            .stat-number {{ font-size: 28px; font-weight: bold; color: {header_color}; }}
            .stat-label {{ font-size: 14px; color: #666; }}
            .footer {{ background-color: #f9f9f9; padding: 10px; text-align: center; font-size: 12px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f5f5f5; font-weight: bold; }}
            .badge {{ 
                display: inline-block; 
                padding: 3px 10px; 
                border-radius: 12px; 
                font-size: 12px; 
                font-weight: bold;
            }}
            .badge-success {{ background-color: #d4edda; color: #155724; }}
            .badge-info {{ background-color: #d1ecf1; color: #0c5460; }}
            .badge-skipped {{ background-color: #e2e3e5; color: #383d41; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>✅ {header_text}</h1>
            <p>{'All tasks executed without errors' if not processing_skipped else 'No processing needed - data unchanged'}</p>
        </div>
        <div class="content">
            {'<div class="success-box">' if not processing_skipped else '<div class="info-box">'}
                <strong>{'✓ Pipeline execution completed successfully' if not processing_skipped else 'ℹ️ Pipeline completed - no changes detected'} in {duration_str}</strong>
            </div>
            
            <h2>Execution Summary</h2>
            <div class="stats-grid">
                <div class="stat-box">
                    <div class="stat-number">{active_records:,}</div>
                    <div class="stat-label">Active Records</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{records_inserted:,}</div>
                    <div class="stat-label">New Records</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{change_percentage:.1f}%</div>
                    <div class="stat-label">Data Change</div>
                </div>
            </div>
            
            <h3>Task Completion Details</h3>
            <table>
                <tr>
                    <th>Task</th>
                    <th>Status</th>
                    <th>Details</th>
                </tr>
                <tr>
                    <td>📥 Data Extraction</td>
                    <td><span class="badge badge-success">Success</span></td>
                    <td>Kaggle dataset loaded</td>
                </tr>
                <tr>
                    <td>📊 Data Validation</td>
                    <td><span class="badge badge-success">Success</span></td>
                    <td>Quality checks passed</td>
                </tr>
                <tr>
                    <td>🔄 Incremental Loading</td>
                    <td><span class="badge badge-success">Success</span></td>
                    <td>Load type: <strong>{load_type or 'N/A'}</strong></td>
                </tr>
                <tr>
                    <td>🔧 Silver Transformations</td>
                    <td><span class="badge badge-{'success' if not processing_skipped else 'skipped'}">{'Success' if not processing_skipped else 'Skipped'}</span></td>
                    <td>{'Data cleaned and standardized' if not processing_skipped else 'No changes to process'}</td>
                </tr>
                <tr>
                    <td>📈 Gold Aggregations</td>
                    <td><span class="badge badge-{'success' if not processing_skipped else 'skipped'}">{'Success' if not processing_skipped else 'Skipped'}</span></td>
                    <td>{'Analytics tables updated' if not processing_skipped else 'No changes to process'}</td>
                </tr>
                <tr>
                    <td>🤖 ML Model</td>
                    <td><span class="badge badge-{'success' if model_retrained else 'info' if not processing_skipped else 'skipped'}">{'Retrained' if model_retrained else 'Skipped' if not processing_skipped else 'Not Evaluated'}</span></td>
                    <td>{'Model updated with new data' if model_retrained else 'No retraining needed' if not processing_skipped else 'Processing skipped'}</td>
                </tr>
            </table>
            
            <h3>Pipeline Summary</h3>
            <ul>
                <li><strong>Processing Mode:</strong> {'Full Pipeline' if not processing_skipped else 'Fast Track (No Changes)'}</li>
                <li><strong>Load Strategy:</strong> {load_type or 'N/A'} ({'Optimized incremental' if load_type == 'INCREMENTAL' else 'Full refresh' if load_type == 'FULL' else 'N/A'})</li>
                <li><strong>Data Freshness:</strong> Checked {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</li>
                <li><strong>Pipeline Duration:</strong> {duration_str}</li>
                <li><strong>Records in Database:</strong> {active_records:,}</li>
            </ul>
            
            <p><strong>Run ID:</strong> {dag_run_id}</p>
            <p><strong>Execution Time:</strong> {dag_run.start_date.strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div style="margin-top: 30px; padding: 15px; background-color: {'#e7f3ff' if not processing_skipped else '#f8f9fa'}; border-radius: 5px;">
                <strong>💡 {'Next Steps' if not processing_skipped else 'Status'}:</strong>
                <ul>
                    {'<li>Review analytics dashboards for updated insights</li>' if not processing_skipped else '<li>No action needed - data is up to date</li>'}
                    {'<li>Check model predictions if retraining occurred</li>' if model_retrained else ''}
                    {'<li>Monitor data quality metrics in PostgreSQL</li>' if not processing_skipped else '<li>Next pipeline run will check for new data</li>'}
                </ul>
            </div>
        </div>
        <div class="footer">
            Flight Price Analytics Pipeline • Airflow Automated Notification<br>
            DAG: {dag_id} • Run: {dag_run_id}
        </div>
    </body>
    </html>
    """
    
    try:
        send_email(
            to=default_args['email'],
            subject=subject,
            html_content=html_content
        )
        logger.info(f"✅ Completion email sent to {default_args['email']}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to send completion email: {e}")


# ============================================
# Helper Functions
# ============================================

def get_mysql_connection_params():
    conn = BaseHook.get_connection('mysql_staging')
    return {
        'host': conn.host,
        'port': conn.port or 3306,
        'user': conn.login,
        'password': conn.password,
        'database': conn.schema
    }


def get_mysql_engine():
    params = get_mysql_connection_params()
    connection_string = (
        f"mysql+pymysql://{params['user']}:{params['password']}@"
        f"{params['host']}:{params['port']}/{params['database']}"
    )
    return create_engine(connection_string)


def get_mysql_connection():
    params = get_mysql_connection_params()
    return pymysql.connect(
        host=params['host'],
        port=params['port'],
        user=params['user'],
        password=params['password'],
        database=params['database']
    )


def execute_mysql_query(query):
    engine = get_mysql_engine()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return df
    finally:
        engine.dispose()


def get_schema_hash(columns: list) -> str:
    """Generate a hash of column names for change detection."""
    columns_str = ','.join(sorted(columns))
    return hashlib.sha256(columns_str.encode()).hexdigest()


def get_mysql_table_columns(table_name: str) -> list:
    """Get existing columns from MySQL table."""
    query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'flight_staging' 
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """
    df = execute_mysql_query(query)
    return df['COLUMN_NAME'].tolist()


def infer_mysql_type(dtype: str, sample_values: pd.Series) -> str:
    """Infer MySQL column type from pandas dtype."""
    dtype_str = str(dtype)
    
    if dtype_str in PYTHON_TO_MYSQL_TYPE:
        return PYTHON_TO_MYSQL_TYPE[dtype_str]
    
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DECIMAL(12, 2)'
    elif 'datetime' in dtype_str:
        return 'DATETIME'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    else:
        max_len = sample_values.astype(str).str.len().max()
        if pd.isna(max_len) or max_len < 255:
            return 'VARCHAR(255)'
        else:
            return 'TEXT'


# ============================================
# Task Functions (Existing - Keeping as-is)
# ============================================

def download_from_kaggle_with_retry(dataset_id: str, output_dir: str, max_retries: int = 3) -> dict:
    """Download dataset from Kaggle with retry logic and exponential backoff."""
    import time
    delay = 5
    last_error = None
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Kaggle download attempt {attempt + 1}/{max_retries}")
            
            result = subprocess.run(
                ['kaggle', 'datasets', 'download', '-d', dataset_id, '-p', output_dir, '--unzip'],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                logger.info("Kaggle download successful")
                return {'success': True, 'attempts': attempt + 1}
            
            last_error = result.stderr
            logger.warning(f"Kaggle download failed: {result.stderr}")
            
        except subprocess.TimeoutExpired:
            last_error = "Download timed out after 5 minutes"
            logger.warning(last_error)
        except Exception as e:
            last_error = str(e)
            logger.warning(f"Kaggle download error: {e}")
        
        if attempt < max_retries - 1:
            logger.info(f"Retrying in {delay} seconds...")
            time.sleep(delay)
            delay *= 2
    
    return {'success': False, 'attempts': max_retries, 'error': last_error}


def extract_from_kaggle(**context) -> dict:
    """Task 0: Download dataset from Kaggle and track metadata."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        # Kaggle credential setup
        kaggle_username = os.environ.get('KAGGLE_USERNAME')
        kaggle_key = os.environ.get('KAGGLE_KEY')
        credential_source = None
        
        if kaggle_username and kaggle_key:
            credential_source = 'environment variables (.env)'
        else:
            try:
                kaggle_username = Variable.get('KAGGLE_USERNAME', default_var=None)
                kaggle_key = Variable.get('KAGGLE_KEY', default_var=None)
                if kaggle_username and kaggle_key:
                    os.environ['KAGGLE_USERNAME'] = kaggle_username
                    os.environ['KAGGLE_KEY'] = kaggle_key
                    credential_source = 'Airflow Variables'
            except:
                pass
        
        if not credential_source:
            kaggle_json_path = '/home/airflow/.kaggle/kaggle.json'
            if os.path.exists(kaggle_json_path):
                credential_source = 'kaggle.json'
            else:
                raise ValueError(
                    "Kaggle credentials not found. Provide them via:\n"
                    "1. .env file (KAGGLE_USERNAME, KAGGLE_KEY)\n"
                    "2. Airflow Variables\n"
                    "3. ~/.kaggle/kaggle.json"
                )
        
        logger.info(f"Kaggle credentials loaded from {credential_source}")
        
        os.makedirs(DATA_DIR, exist_ok=True)
        
        logger.info(f"Downloading dataset: {KAGGLE_DATASET}")
        download_result = download_from_kaggle_with_retry(KAGGLE_DATASET, DATA_DIR, max_retries=3)
        
        if not download_result['success']:
            if os.path.exists(CSV_FILE_PATH):
                logger.warning(
                    f"Kaggle download failed after {download_result['attempts']} attempts, "
                    f"but existing file found: {CSV_FILE_PATH}"
                )
                logger.warning(f"Last error: {download_result.get('error')}")
            else:
                raise Exception(
                    f"Kaggle download failed after {download_result['attempts']} attempts: "
                    f"{download_result.get('error')}"
                )
        
        logger.info(f"Reading CSV: {CSV_FILE_PATH}")
        df = pd.read_csv(CSV_FILE_PATH)
        
        csv_columns = list(df.columns)
        row_count = len(df)
        file_size = os.path.getsize(CSV_FILE_PATH)
        schema_hash = get_schema_hash(csv_columns)
        
        logger.info(f"CSV has {row_count} rows and {len(csv_columns)} columns")
        
        db_columns = [
            COLUMN_MAPPING.get(col, col.lower().replace(' ', '_').replace('(', '').replace(')', '')) 
            for col in csv_columns
        ]
        
        existing_columns = get_mysql_table_columns('raw_flight_data')
        system_columns = ['id', 'loaded_at', 'source_file', 'metadata_id']
        data_columns = [col for col in existing_columns if col not in system_columns]
        
        new_columns = [col for col in db_columns if col not in existing_columns]
        schema_changed = len(new_columns) > 0
        
        if schema_changed:
            logger.warning(f"Schema change detected! New columns: {new_columns}")
            
            conn = get_mysql_connection()
            try:
                cursor = conn.cursor()
                
                for new_col in new_columns:
                    csv_col_idx = db_columns.index(new_col)
                    csv_col_name = csv_columns[csv_col_idx]
                    mysql_type = infer_mysql_type(df[csv_col_name].dtype, df[csv_col_name])
                    
                    for table in ['raw_flight_data', 'validated_flight_data']:
                        alter_sql = f"ALTER TABLE {table} ADD COLUMN `{new_col}` {mysql_type} NULL"
                        logger.info(f"Executing: {alter_sql}")
                        cursor.execute(alter_sql)
                
                conn.commit()
                logger.info(f"Added {len(new_columns)} new columns to MySQL tables")
                
            finally:
                cursor.close()
                conn.close()
        else:
            logger.info("No schema changes detected")
        
        try:
            previous_metadata = execute_mysql_query(
                "SELECT id, schema_hash FROM dataset_metadata ORDER BY id DESC LIMIT 1"
            )
            previous_metadata_id = int(previous_metadata.iloc[0]['id']) if len(previous_metadata) > 0 else None
        except:
            previous_metadata_id = None
        
        columns_info = {col: str(df[col].dtype) for col in csv_columns}
        
        conn = get_mysql_connection()
        try:
            cursor = conn.cursor()
            
            insert_sql = """
                INSERT INTO dataset_metadata 
                (dataset_name, kaggle_dataset_id, file_name, file_size_bytes, 
                 row_count, column_count, columns_json, schema_hash, 
                 schema_changed, new_columns_added, previous_metadata_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                'Flight Price Dataset of Bangladesh',
                KAGGLE_DATASET,
                KAGGLE_FILENAME,
                file_size,
                row_count,
                len(csv_columns),
                json.dumps(columns_info),
                schema_hash,
                schema_changed,
                json.dumps(new_columns) if new_columns else None,
                previous_metadata_id
            ))
            
            metadata_id = cursor.lastrowid
            conn.commit()
            
            logger.info(f"Recorded metadata with ID: {metadata_id}")
            
        finally:
            cursor.close()
            conn.close()
        
        context['ti'].xcom_push(key='metadata_id', value=metadata_id)
        context['ti'].xcom_push(key='schema_changed', value=schema_changed)
        context['ti'].xcom_push(key='new_columns', value=new_columns)
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=row_count,
            metadata={
                'kaggle_dataset': KAGGLE_DATASET,
                'credential_source': credential_source,
                'download_attempts': download_result.get('attempts', 1),
                'file_size_bytes': file_size,
                'row_count': row_count,
                'column_count': len(csv_columns),
                'schema_changed': schema_changed,
                'new_columns': new_columns,
                'metadata_id': metadata_id
            }
        )
        
        return {
            'metadata_id': metadata_id,
            'row_count': row_count,
            'column_count': len(csv_columns),
            'schema_changed': schema_changed,
            'new_columns': new_columns
        }
        
    except Exception as e:
        logger.exception(f"Kaggle extraction failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def load_csv_to_mysql(**context) -> dict:
    """Task 1: Load CSV file into MySQL raw_flight_data table."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    metadata_id = context['ti'].xcom_pull(task_ids='extract_from_kaggle', key='metadata_id')
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info(f"Reading CSV from {CSV_FILE_PATH}")
        
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")

        if os.path.getsize(CSV_FILE_PATH) == 0:
            raise ValueError(f"CSV file is empty: {CSV_FILE_PATH}")

        df = pd.read_csv(CSV_FILE_PATH)
        total_rows = len(df)
        logger.info(f"Read {total_rows} rows from CSV")
        
        logger.info("Renaming columns to match database schema")
        df = df.rename(columns=COLUMN_MAPPING)
        
        for col in df.columns:
            if col not in COLUMN_MAPPING.values():
                new_col = col.lower().replace(' ', '_').replace('(', '').replace(')', '')
                if new_col != col:
                    df = df.rename(columns={col: new_col})
        
        logger.info(f"Columns after rename: {list(df.columns)}")
        
        logger.info("Converting data types")
        
        if 'departure_datetime' in df.columns:
            df['departure_datetime'] = pd.to_datetime(df['departure_datetime'], errors='coerce')
        if 'arrival_datetime' in df.columns:
            df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'], errors='coerce')
        
        numeric_columns = [
            'duration_hrs', 'base_fare_bdt', 'tax_surcharge_bdt', 
            'total_fare_bdt', 'days_before_departure'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['source_file'] = KAGGLE_FILENAME
        df['metadata_id'] = metadata_id
        
        rows_with_issues = df.isna().any(axis=1).sum()
        if rows_with_issues > 0:
            logger.warning(f"{rows_with_issues} rows have NULL values after conversion")
        
        logger.info("Clearing existing data from MySQL tables")
        conn = get_mysql_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("TRUNCATE TABLE validated_flight_data")
                cursor.execute("TRUNCATE TABLE raw_flight_data")
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                conn.commit()
            finally:
                cursor.close()
        finally:
            conn.close()

        logger.info(f"Inserting {total_rows} rows into raw_flight_data")
        
        engine = get_mysql_engine()
        try:
            df.to_sql(
                name='raw_flight_data',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=BATCH_SIZE
            )
        finally:
            engine.dispose()
        
        logger.info(f"Successfully loaded {total_rows} rows into MySQL")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=total_rows,
            rows_failed=0,
            metadata={
                'source_file': CSV_FILE_PATH,
                'rows_with_null_values': int(rows_with_issues),
                'metadata_id': metadata_id
            }
        )
        
        return {
            'rows_loaded': total_rows,
            'rows_with_issues': int(rows_with_issues)
        }
        
    except Exception as e:
        logger.exception(f"Failed to load CSV: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def validate_mysql_data(**context) -> dict:
    """Task 2: Validate data in MySQL."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Fetching raw data for validation")
        
        columns = get_mysql_table_columns('raw_flight_data')
        columns_str = ', '.join([f'`{col}`' for col in columns])
        
        raw_data_query = f"SELECT {columns_str} FROM raw_flight_data"
        
        df = execute_mysql_query(raw_data_query)
        total_rows = len(df)
        logger.info(f"Fetched {total_rows} rows for validation")
        
        logger.info("Applying validation rules")
        df['is_valid'] = True
        df['validation_errors'] = ''
        
        required_fields = ['airline', 'source_code', 'destination_code']
        for field in required_fields:
            if field in df.columns:
                mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} is required; '
        
        fare_fields = ['base_fare_bdt', 'total_fare_bdt']
        for field in fare_fields:
            if field in df.columns:
                mask = (df[field].isna()) | (df[field] <= 0)
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} must be positive; '

        if 'tax_surcharge_bdt' in df.columns:
            mask = (df['tax_surcharge_bdt'].isna()) | (df['tax_surcharge_bdt'] < 0)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'tax_surcharge_bdt cannot be negative; '

        for field in ['source_code', 'destination_code']:
            if field in df.columns:
                mask = df[field].astype(str).str.len() != 3
                df.loc[mask, 'is_valid'] = False
                df.loc[mask, 'validation_errors'] += f'{field} must be 3 characters; '
        
        if 'duration_hrs' in df.columns:
            mask = (df['duration_hrs'].isna()) | (df['duration_hrs'] <= 0)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'duration_hrs must be positive; '
        
        if 'days_before_departure' in df.columns:
            mask = (df['days_before_departure'].isna()) | (df['days_before_departure'] < 1)
            df.loc[mask, 'is_valid'] = False
            df.loc[mask, 'validation_errors'] += 'days_before_departure must be at least 1; '
        
        df['validation_errors'] = df['validation_errors'].str.rstrip('; ')
        df.loc[df['validation_errors'] == '', 'validation_errors'] = None
        
        valid_count = df['is_valid'].sum()
        invalid_count = total_rows - valid_count
        
        logger.info(f"Validation complete - Valid: {valid_count}, Invalid: {invalid_count}")
        
        df = df.rename(columns={'id': 'raw_id'})
        
        validated_columns = get_mysql_table_columns('validated_flight_data')
        columns_to_insert = [col for col in df.columns if col in validated_columns and col != 'id']
        
        logger.info(f"Inserting columns: {columns_to_insert}")
        
        engine = get_mysql_engine()
        try:
            df[columns_to_insert].to_sql(
                name='validated_flight_data',
                con=engine,
                if_exists='append',
                index=False,
                chunksize=BATCH_SIZE
            )
        finally:
            engine.dispose()
        
        logger.info(f"Inserted {total_rows} rows into validated_flight_data")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=int(valid_count),
            rows_failed=int(invalid_count),
            metadata={
                'total_rows': total_rows,
                'valid_rows': int(valid_count),
                'invalid_rows': int(invalid_count),
                'validation_rate': f"{(valid_count/total_rows)*100:.2f}%"
            }
        )
        
        return {
            'total_rows': total_rows,
            'valid_rows': int(valid_count),
            'invalid_rows': int(invalid_count)
        }
        
    except Exception as e:
        logger.exception(f"Validation failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def transfer_to_postgres_incremental(**context) -> dict:
    """Task 3: Transfer validated data from MySQL to PostgreSQL using incremental loading."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("=" * 70)
        logger.info("🚀 STARTING INCREMENTAL DATA TRANSFER")
        logger.info("=" * 70)
        
        loader = IncrementalDataLoader(
            postgres_conn_id='postgres_analytics',
            mysql_conn_id='mysql_staging'
        )
        
        df_new = loader.load_new_data_from_mysql()
        
        if df_new.empty:
            logger.warning("⚠️ No valid data to transfer")
            log_pipeline_event(
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                task_id=task_id,
                status='completed',
                rows_processed=0,
                metadata={'message': 'No valid rows to transfer'}
            )
            
            # Push empty metrics to XCom
            context['ti'].xcom_push(key='load_type', value='NONE')
            context['ti'].xcom_push(key='records_inserted', value=0)
            context['ti'].xcom_push(key='records_deleted', value=0)
            context['ti'].xcom_push(key='change_percentage', value=0.0)
            context['ti'].xcom_push(key='active_records', value=0)
            
            return {'rows_transferred': 0, 'load_type': 'NONE'}
        
        changes = loader.detect_changes(df_new)
        change_percentage = changes['change_percentage']
        
        if change_percentage > FULL_LOAD_THRESHOLD:
            logger.info("=" * 70)
            logger.info(f"⚠️  CHANGE RATE: {change_percentage:.1f}% (>{FULL_LOAD_THRESHOLD}%)")
            logger.info("   → Performing FULL LOAD (truncate and reload)")
            logger.info("=" * 70)
            
            loader.apply_full_load(df_new)
            load_type = 'FULL'
            rows_transferred = len(df_new)
            
        else:
            logger.info("=" * 70)
            logger.info(f"✅ CHANGE RATE: {change_percentage:.1f}% (<={FULL_LOAD_THRESHOLD}%)")
            logger.info("   → Performing INCREMENTAL LOAD")
            logger.info("=" * 70)
            
            loader.apply_incremental_load(changes, load_type='INCREMENTAL')
            load_type = 'INCREMENTAL'
            rows_transferred = loader.load_stats['new']
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_analytics')
        verify_query = "SELECT COUNT(*) FROM bronze.validated_flights WHERE is_active = TRUE"
        result = postgres_hook.get_first(verify_query)
        active_count = result[0]
        
        logger.info(f"✅ Verified: {active_count:,} active records in PostgreSQL")
        
        stats = loader.get_load_statistics(days=7)
        
        if not stats.empty:
            logger.info("\n" + "=" * 70)
            logger.info("📊 RECENT LOAD STATISTICS (Last 7 Days)")
            logger.info("=" * 70)
            logger.info("\n" + stats.to_string(index=False))
            logger.info("=" * 70)
        
        # Push metrics to XCom
        context['ti'].xcom_push(key='load_type', value=load_type)
        context['ti'].xcom_push(key='records_inserted', value=loader.load_stats['new'])
        context['ti'].xcom_push(key='records_deleted', value=loader.load_stats['deleted'])
        context['ti'].xcom_push(key='change_percentage', value=change_percentage)
        context['ti'].xcom_push(key='active_records', value=active_count)
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            rows_processed=rows_transferred,
            metadata={
                'load_type': load_type,
                'change_percentage': float(change_percentage),
                'records_inserted': loader.load_stats['new'],
                'records_deleted': loader.load_stats['deleted'],
                'records_unchanged': loader.load_stats['unchanged'],
                'active_records_total': active_count,
                'full_load_threshold': FULL_LOAD_THRESHOLD
            }
        )
        
        logger.info("\n" + "=" * 70)
        logger.info(f"✅ DATA TRANSFER COMPLETED USING {load_type} LOAD")
        logger.info("=" * 70)
        
        return {
            'rows_transferred': rows_transferred,
            'load_type': load_type,
            'change_percentage': float(change_percentage),
            'active_records': active_count
        }
        
    except Exception as e:
        logger.exception(f"Transfer failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


# ============================================
# DBT Tasks (Separate for Parallel Execution)
# ============================================

def run_dbt_snapshot(**context) -> dict:
    """Task 4a: Run DBT snapshot."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Starting DBT snapshot")
        
        result = subprocess.run(
            ['dbt', 'snapshot', '--profiles-dir', '.'],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logger.info(result.stdout)
        
        if result.returncode != 0:
            logger.error(f"DBT snapshot failed with return code {result.returncode}")
            if result.stderr:
                logger.error(result.stderr)
            raise Exception(f"DBT snapshot failed: {result.stderr[:500] if result.stderr else 'Unknown error'}")
        
        logger.info("DBT snapshot completed successfully")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed'
        )
        
        return {'status': 'success'}
        
    except Exception as e:
        logger.exception(f"DBT snapshot failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def run_dbt_silver(**context) -> dict:
    """Task 4b: Run DBT silver layer transformations."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Starting DBT silver layer transformations")
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'silver.*', '--profiles-dir', '.'],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logger.info(result.stdout)
        
        if result.returncode != 0:
            logger.error(f"DBT silver run failed with return code {result.returncode}")
            if result.stderr:
                logger.error(result.stderr)
            raise Exception(f"DBT silver run failed: {result.stderr[:500] if result.stderr else 'Unknown error'}")
        
        logger.info("DBT silver layer completed successfully")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed'
        )
        
        return {'status': 'success'}
        
    except Exception as e:
        logger.exception(f"DBT silver layer failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def run_dbt_gold(**context) -> dict:
    """Task 4c: Run DBT gold layer transformations (PARALLEL)."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Starting DBT gold layer transformations")
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'gold.*', '--profiles-dir', '.'],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logger.info(result.stdout)
        
        if result.returncode != 0:
            logger.error(f"DBT gold run failed with return code {result.returncode}")
            if result.stderr:
                logger.error(result.stderr)
            raise Exception(f"DBT gold run failed: {result.stderr[:500] if result.stderr else 'Unknown error'}")
        
        logger.info("DBT gold layer completed successfully")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed'
        )
        
        return {'status': 'success'}
        
    except Exception as e:
        logger.exception(f"DBT gold layer failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise


def run_dbt_tests(**context) -> dict:
    """Task 4d: Run DBT tests."""
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='started'
    )
    
    try:
        logger.info("Starting DBT tests")
        
        result = subprocess.run(
            ['dbt', 'test', '--profiles-dir', '.'],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True
        )
        
        if result.stdout:
            logger.info(result.stdout)
        
        if result.returncode != 0:
            logger.error(f"DBT tests failed with return code {result.returncode}")
            if result.stderr:
                logger.error(result.stderr)
            raise Exception(f"DBT tests failed: {result.stderr[:500] if result.stderr else 'Unknown error'}")
        
        logger.info("DBT tests completed successfully")
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed'
        )
        
        return {'status': 'success'}
        
    except Exception as e:
        logger.exception(f"DBT tests failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        raise
    
def decide_processing(**context):
    """Decide whether to run DBT and ML pipeline based on data changes"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    try:
        logger.info("=" * 70)
        logger.info("🤔 DECIDING WHETHER TO PROCESS DATA")
        logger.info("=" * 70)
        
        ti = context['ti']
        
        # Get transfer statistics
        load_type = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='load_type')
        records_inserted = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_inserted') or 0
        records_deleted = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='records_deleted') or 0
        change_percentage = ti.xcom_pull(task_ids='transfer_to_postgres_incremental', key='change_percentage') or 0
        
        logger.info(f"📊 Data Change Summary:")
        logger.info(f"   Load type: {load_type}")
        logger.info(f"   Records inserted: {records_inserted:,}")
        logger.info(f"   Records deleted: {records_deleted:,}")
        logger.info(f"   Change percentage: {change_percentage:.2f}%")
        
        # Decision logic
        MIN_CHANGE_THRESHOLD = 0.1
        total_changes = records_inserted + records_deleted
        
        should_process = False
        reasons = []
        
        if load_type == 'FULL':
            should_process = True
            reasons.append("Full data reload detected")
        elif change_percentage >= MIN_CHANGE_THRESHOLD:
            should_process = True
            reasons.append(f"Data changed by {change_percentage:.2f}%")
        elif total_changes > 0:
            should_process = True
            reasons.append(f"{total_changes:,} records changed")
        else:
            reasons.append("No data changes detected")
        
        logger.info("\n" + "=" * 70)
        if should_process:
            decision = 'notify_change_detection'  # ✅ FIXED
            logger.info("✅ DECISION: PROCESS DATA")
            logger.info("   Will run: DBT transformations + ML retraining")
            logger.info("   Reasons:")
            for r in reasons:
                logger.info(f"   - {r}")
        else:
            decision = 'skip_processing'
            logger.info("⏭️ DECISION: SKIP PROCESSING")
            logger.info("   Will skip: DBT transformations + ML retraining")
            logger.info("   Reasons:")
            for r in reasons:
                logger.info(f"   - {r}")
        
        logger.info("=" * 70)
        
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='completed',
            metadata={
                'decision': decision,
                'reasons': reasons,
                'change_percentage': change_percentage,
                'records_changed': total_changes
            }
        )
        
        return decision
        
    except Exception as e:
        logger.exception(f"Decision failed: {e}")
        log_pipeline_event(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            status='failed',
            error_message=str(e)
        )
        return 'skip_processing'


def skip_processing(**context):
    """Dummy task when processing is skipped"""
    from utils.logging_utils import get_task_context, log_pipeline_event
    
    dag_id, dag_run_id, task_id = get_task_context(context)
    
    logger.info("⏭️ Processing skipped - no significant data changes")
    logger.info("   DBT transformations: SKIPPED")
    logger.info("   ML retraining: SKIPPED")
    
    log_pipeline_event(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        status='completed',
        metadata={'processed': False, 'reason': 'no_changes'}
    )
    
    return {'processed': False}






# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='ETL pipeline with incremental loading and parallel DBT transformations',
    schedule_interval=None,  # Set to '@daily' or cron expression for scheduled runs
    catchup=False,
    tags=['flight', 'etl', 'bangladesh', 'kaggle', 'incremental'],
) as dag:
    
    # ====================================
    # Email: Pipeline Start Notification
    # ====================================
    
    notify_start = PythonOperator(
        task_id='notify_pipeline_start',
        python_callable=send_pipeline_start_email,
    )
    
    # ====================================
    # Data Extraction & Loading Tasks
    # ====================================
    
    task_extract = PythonOperator(
        task_id='extract_from_kaggle',
        python_callable=extract_from_kaggle,
    )
    
    task_load_csv = PythonOperator(
        task_id='load_csv_to_mysql',
        python_callable=load_csv_to_mysql,
    )
    
    task_validate = PythonOperator(
        task_id='validate_mysql_data',
        python_callable=validate_mysql_data,
    )
    
    task_transfer = PythonOperator(
        task_id='transfer_to_postgres_incremental',
        python_callable=transfer_to_postgres_incremental,
    )
    
       
    # Decision task - check if processing needed
    task_decide_processing = BranchPythonOperator(
        task_id='decide_processing',
        python_callable=decide_processing,
        dag=dag,
    )
    
    # ====================================
    # Email: Change Detection Notification
    # ====================================
    
    notify_changes = PythonOperator(
        task_id='notify_change_detection',
        python_callable=send_change_detection_email,
    )
    
    # ====================================
    # DBT Transformations
    # ====================================
    
    task_dbt_snapshot = PythonOperator(
        task_id='dbt_snapshot',
        python_callable=run_dbt_snapshot,
    )
    
    task_dbt_silver = PythonOperator(
        task_id='dbt_run_silver',
        python_callable=run_dbt_silver,
    )
    
    task_dbt_gold = PythonOperator(
        task_id='dbt_run_gold',
        python_callable=run_dbt_gold,
    )
    
    task_dbt_tests = PythonOperator(
        task_id='dbt_run_tests',
        python_callable=run_dbt_tests,
    )
    
    
    task_decide_retraining = PythonOperator(
        task_id='decide_model_retraining',
        python_callable=decide_model_retraining,
        dag=dag,
    )

    task_retrain_model = PythonOperator(
        task_id='retrain_model',
        python_callable=retrain_ml_model,
        dag=dag,
    )

    task_skip_retraining = PythonOperator(
        task_id='skip_retraining',
        python_callable=skip_retraining,
        dag=dag,
    )

    task_retraining_complete = PythonOperator(
        task_id='retraining_complete',
        python_callable=retraining_complete,
        trigger_rule='none_failed_min_one_success',
        dag=dag,
    )

    # Skip processing task
    task_skip_processing = PythonOperator(
        task_id='skip_processing',
        python_callable=skip_processing,
        dag=dag,
    )

    # ====================================
    # Email: Completion Notification
    # ====================================
    
    notify_completion = PythonOperator(
        task_id='notify_pipeline_completion',
        python_callable=send_completion_email,
        trigger_rule='none_failed_min_one_success',
        dag=dag,
    )
    
    # ====================================
    # Pipeline Flow Definition
    # ====================================
  # ============================================
    # TASK DEPENDENCIES - SMART BRANCHING
    # ============================================

    # Start notification
    notify_start >> task_extract

    # Data pipeline (always runs)
    task_extract >> task_load_csv >> task_validate >> task_transfer

    # DECISION POINT: Check if changes detected
    task_transfer >> task_decide_processing

    # BRANCH 1: Changes detected - full processing
    task_decide_processing >> notify_changes
    notify_changes >> task_dbt_silver
    task_dbt_silver >> task_dbt_snapshot
    task_dbt_snapshot >> task_dbt_gold
    task_dbt_gold >> task_dbt_tests
    task_dbt_tests >> task_decide_retraining
    task_decide_retraining >> [task_retrain_model, task_skip_retraining]
    [task_retrain_model, task_skip_retraining] >> task_retraining_complete

    # BRANCH 2: No changes - skip processing
    task_decide_processing >> task_skip_processing

    # Both branches converge at completion
    [task_retraining_complete, task_skip_processing] >> notify_completion


# ============================================
# DAG Documentation
# ============================================

dag.doc_md = """
# Flight Price Analysis Pipeline

## Overview
End-to-end data pipeline for Bangladesh flight price analysis with:
- Incremental loading (MD5 hash-based change detection)
- Parallel DBT transformations (Silver + Gold)
- Conditional ML model retraining
- Email notifications at key stages

## Pipeline Flow

### 1. Data Extraction (Kaggle)
- Downloads latest dataset
- Tracks metadata and schema changes
- Handles retries and fallbacks

### 2. Data Loading (MySQL)
- Loads raw data to staging
- Validates data quality
- Handles schema evolution

### 3. Incremental Transfer (PostgreSQL)
- Smart loading: FULL vs INCREMENTAL
- Hash-based change detection
- Soft deletes for historical tracking

### 4. DBT Transformations (Sequential)
- **Silver layer**: Data cleaning and standardization (runs first)
- **Snapshots**: SCD Type 2 tracking of fare and route changes (after Silver)
  - Depend on silver_cleaned_flights table
- **Gold layer**: Business aggregations and metrics (after Snapshots)
  - Some models reference snapshot tables for historical analysis
- **Tests**: Data quality validation (after Gold)

### 5. Email Notifications
- **Start**: Pipeline execution begins
- **Change Detection**: After incremental load with statistics
- **Completion**: Final summary with all metrics

## Configuration

### Email Settings
Update `email` in default_args (line ~84) with your email address.
Set `ENABLE_EMAIL_NOTIFICATIONS = False` to disable emails.

### Thresholds
- `FULL_LOAD_THRESHOLD = 50.0`: >50% change triggers full reload instead of incremental

### Scheduling
Currently manual trigger only. To schedule:
```python
schedule_interval='@daily'  # or cron expression
```

## Monitoring
Check email for:
- Pipeline start confirmation
- Data change statistics
- Final completion summary with all metrics
"""