"""
Pipeline logging utilities.
Tracks task execution in the audit.pipeline_runs table.
"""

from datetime import datetime
import json
from typing import Optional
from airflow.providers.postgres.hooks.postgres import PostgresHook


def log_pipeline_event(dag_id, dag_run_id, task_id, status, metadata=None, 
                      rows_processed=None, rows_failed=None, error_message=None):
    """Log pipeline events to audit table"""
    import json
    from datetime import datetime
    
    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    # Convert metadata dict to JSON string
    metadata_json = json.dumps(metadata) if metadata else None
    
    insert_query = """
        INSERT INTO audit.pipeline_runs 
        (dag_id, dag_run_id, task_id, status, started_at, completed_at, 
         rows_processed, rows_failed, error_message, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
    """
    
    if status == 'started':
        started_at = datetime.now()
        completed_at = None
    else:
        started_at = None
        completed_at = datetime.now()
    
    hook.run(
        insert_query,
        parameters=(
            dag_id, dag_run_id, task_id, status,
            started_at, completed_at,
            rows_processed, rows_failed, error_message,
            metadata_json
        )
    )


def get_task_context(context: dict) -> tuple:
    """
    Extract common identifiers from Airflow task context.
    
    Args:
        context: The **context passed to a PythonOperator callable
    
    Returns:
        Tuple of (dag_id, dag_run_id, task_id)
    
    Example:
        def my_task(**context):
            dag_id, dag_run_id, task_id = get_task_context(context)
    """
    
    dag_id = context['dag'].dag_id
    dag_run_id = context['run_id']
    task_id = context['task'].task_id
    
    return dag_id, dag_run_id, task_id
