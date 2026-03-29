"""
airflow/dags/ecommerce_pipeline_dag.py
Airflow DAG — orchestrates the full ELT pipeline
Install: pip install apache-airflow
Run locally: astro dev start (using Astronomer CLI)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

log = logging.getLogger(__name__)

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "karthik_surya",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── Task Functions ────────────────────────────────────────────────────────────
def task_extract(**context):
    import sys
    sys.path.append("/opt/airflow/project")
    from pipeline.extract import extract
    log.info("Running Extract stage...")
    extract()
    log.info("Extract complete ✅")


def task_transform(**context):
    import sys
    sys.path.append("/opt/airflow/project")
    from pipeline.transform import transform
    log.info("Running Transform stage...")
    transform()
    log.info("Transform complete ✅")


def task_load(**context):
    import sys
    sys.path.append("/opt/airflow/project")
    from pipeline.load import load
    log.info("Running Load stage...")
    load()
    log.info("Load complete ✅")


def task_notify_success(**context):
    run_id   = context["run_id"]
    exec_dt  = context["execution_date"]
    log.info(f"✅ Pipeline SUCCESS | Run: {run_id} | Date: {exec_dt}")
    # Could add Slack/email notification here


def task_notify_failure(**context):
    log.error("❌ Pipeline FAILED — check task logs")


def task_data_quality_gate(**context):
    """Gate task — fails DAG if quality checks didn't pass"""
    import os
    import pandas as pd
    fact_path = "data/mart/fact_orders.parquet"
    if not os.path.exists(fact_path):
        raise FileNotFoundError("fact_orders.parquet not found — pipeline may have failed")
    df = pd.read_parquet(fact_path)
    if len(df) == 0:
        raise ValueError("fact_orders is empty!")
    log.info(f"✅ Data quality gate passed — {len(df):,} orders in fact table")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description="Daily ELT pipeline — Raw Delta Lake → Staging → Mart → Quality Check",
    schedule_interval="0 1 * * *",   # 1 AM daily
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "elt", "production", "delta-lake"],
    doc_md="""
## Ecommerce ELT Pipeline

**Built by:** Karthik Surya J  
**Stack:** Python + Delta Lake + Parquet + Airflow

### Flow
```
[Delta Lake Raw] → [Extract & Validate] → [Transform]
→ [Staging Layer] → [Mart Layer] → [Quality Gate] → [Load]
```

### Schedule
Runs daily at 1:00 AM

### Layers
- **Raw**: Delta Lake ingestion from source
- **Staging**: Cleaned, deduplicated, enriched
- **Mart**: Star schema — fact + dimensions + aggregations
    """,
) as dag:

    # ── Tasks ─────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    extract_task = PythonOperator(
        task_id="extract_from_delta_lake",
        python_callable=task_extract,
        doc_md="Reads raw data from Delta Lake, validates schema and nulls",
    )

    transform_task = PythonOperator(
        task_id="transform_staging_and_mart",
        python_callable=task_transform,
        doc_md="Builds staging models, dimension tables, fact table, and aggregated marts",
    )

    quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=task_data_quality_gate,
        doc_md="Validates fact table exists and has rows before proceeding",
    )

    load_task = PythonOperator(
        task_id="load_to_delta_mart",
        python_callable=task_load,
        doc_md="Runs final quality checks and writes mart tables to Delta Lake",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify_success,
        trigger_rule="all_success",
    )

    end = EmptyOperator(
        task_id="pipeline_end",
        trigger_rule="all_done",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    start >> extract_task >> transform_task >> quality_gate >> load_task >> notify_success >> end
