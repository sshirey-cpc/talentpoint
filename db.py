"""
TalentPoint — BigQuery client wrapper.

Provides a shared, tenant-aware database layer for all modules.
All queries use parameterized SQL to prevent injection.
"""

import os
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery


# Module-level client (initialized once per process)
_client = None


def get_client():
    """Get or create the BigQuery client."""
    global _client
    if _client is None:
        _client = bigquery.Client(project=get_project_id())
    return _client


def get_project_id():
    return os.environ.get('GCP_PROJECT_ID', 'confluence-point-consulting')


def get_dataset():
    return os.environ.get('BQ_DATASET', 'talentpoint')


def table_ref(table_name):
    """Return fully-qualified table reference: project.dataset.table"""
    return f"{get_project_id()}.{get_dataset()}.{table_name}"


def generate_id():
    """Generate an 8-character uppercase ID from a UUID."""
    return uuid.uuid4().hex[:8].upper()


def now_iso():
    """Current UTC timestamp in ISO format for BigQuery."""
    return datetime.now(timezone.utc).isoformat()


def query(sql, params=None):
    """
    Execute a parameterized BigQuery query and return rows as list of dicts.

    Args:
        sql: SQL string with @param placeholders
        params: list of bigquery.ScalarQueryParameter objects
    """
    client = get_client()
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    result = client.query(sql, job_config=job_config)
    return [dict(row) for row in result]


def query_one(sql, params=None):
    """Execute a query and return the first row as a dict, or None."""
    rows = query(sql, params)
    return rows[0] if rows else None


def execute(sql, params=None):
    """Execute a DML statement (INSERT/UPDATE/DELETE). Returns num_dml_affected_rows."""
    client = get_client()
    job_config = bigquery.QueryJobConfig()
    if params:
        job_config.query_parameters = params
    result = client.query(sql, job_config=job_config)
    result.result()  # Wait for completion
    return result.num_dml_affected_rows


def insert_row(table_name, data):
    """
    Insert a single row into a table using DML.

    Args:
        table_name: table name (without project/dataset prefix)
        data: dict of column_name -> value
    """
    columns = list(data.keys())
    placeholders = [f"@{col}" for col in columns]
    sql = f"INSERT INTO `{table_ref(table_name)}` ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

    params = []
    for col, val in data.items():
        param_type = _infer_bq_type(val)
        params.append(bigquery.ScalarQueryParameter(col, param_type, val))

    return execute(sql, params)


def update_row(table_name, pk_column, pk_value, data):
    """
    Update a single row by primary key.

    Args:
        table_name: table name
        pk_column: primary key column name
        pk_value: primary key value
        data: dict of column_name -> new_value
    """
    set_clauses = [f"{col} = @{col}" for col in data.keys()]
    sql = f"UPDATE `{table_ref(table_name)}` SET {', '.join(set_clauses)} WHERE {pk_column} = @_pk_value"

    params = [bigquery.ScalarQueryParameter("_pk_value", "STRING", pk_value)]
    for col, val in data.items():
        param_type = _infer_bq_type(val)
        params.append(bigquery.ScalarQueryParameter(col, param_type, val))

    return execute(sql, params)


def delete_row(table_name, pk_column, pk_value):
    """Delete a single row by primary key."""
    sql = f"DELETE FROM `{table_ref(table_name)}` WHERE {pk_column} = @pk_value"
    params = [bigquery.ScalarQueryParameter("pk_value", "STRING", pk_value)]
    return execute(sql, params)


def _infer_bq_type(value):
    """Infer BigQuery parameter type from a Python value."""
    if isinstance(value, bool):
        return "BOOL"
    elif isinstance(value, int):
        return "INT64"
    elif isinstance(value, float):
        return "FLOAT64"
    elif isinstance(value, datetime):
        return "TIMESTAMP"
    else:
        return "STRING"
