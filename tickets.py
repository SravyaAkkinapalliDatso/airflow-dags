from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy import create_engine, text, Table, Column, String, DateTime, MetaData
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

def get_engine(conn_id):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return create_engine(hook.get_uri().replace('postgres://', 'postgresql://'))

def create_schema_and_tables():
    engine = get_engine('dashboards')
    with engine.connect() as conn:
        conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS tickets;

        CREATE TABLE IF NOT EXISTS tickets.dim_project (
            project_id TEXT PRIMARY KEY,
            project_name TEXT
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_property (
            property_id TEXT PRIMARY KEY,
            property_name TEXT
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_issue_type (
            issue_type TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_severity (
            severity TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_priority (
            priority TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_status (
            status TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS tickets.dim_reporter (
            reported_by_email TEXT PRIMARY KEY
        );

        CREATE TABLE IF NOT EXISTS tickets.fact_issues (
            issue_id TEXT PRIMARY KEY,
            project_id TEXT,
            property_id TEXT,
            issue_type TEXT,
            severity TEXT,
            priority TEXT,
            status TEXT,
            reported_by_email TEXT,
            updated_at TIMESTAMP WITHOUT TIME ZONE
        );
        """))

def extract_transform_load():
    src_engine = get_engine('avenue')
    tgt_engine = get_engine('dashboards')

    query = """
    SELECT 
        im.issue_id, im.project_id, p.project_name,
        im.property_id, pr.name as property_name,
        im.reported_by_email, im.issue_type, im.severity, im.priority, im.status, im.updated_at
    FROM tickets.issue_master im
    LEFT JOIN master.projects p ON im.project_id = p.project_id
    LEFT JOIN master.properties pr ON im.property_id = pr.propertyid;
    """

    df = pd.read_sql(query, src_engine)

    with tgt_engine.begin() as conn:
        # Load dim_project
        existing = pd.read_sql("SELECT project_id FROM tickets.dim_project", conn)
        new = df[['project_id', 'project_name']].drop_duplicates()
        new = new[~new['project_id'].isin(existing['project_id'])]
        if not new.empty:
            new.to_sql('dim_project', conn, schema='tickets', if_exists='append', index=False)

        # Load dim_property
        existing = pd.read_sql("SELECT property_id FROM tickets.dim_property", conn)
        new = df[['property_id', 'property_name']].drop_duplicates()
        new = new[~new['property_id'].isin(existing['property_id'])]
        if not new.empty:
            new.to_sql('dim_property', conn, schema='tickets', if_exists='append', index=False)

        # dim_issue_type
        existing = pd.read_sql("SELECT issue_type FROM tickets.dim_issue_type", conn)
        new = df[['issue_type']].drop_duplicates()
        new = new[~new['issue_type'].isin(existing['issue_type'])]
        if not new.empty:
            new.to_sql('dim_issue_type', conn, schema='tickets', if_exists='append', index=False)

        # dim_severity
        existing = pd.read_sql("SELECT severity FROM tickets.dim_severity", conn)
        new = df[['severity']].drop_duplicates()
        new = new[~new['severity'].isin(existing['severity'])]
        if not new.empty:
            new.to_sql('dim_severity', conn, schema='tickets', if_exists='append', index=False)

        # dim_priority
        existing = pd.read_sql("SELECT priority FROM tickets.dim_priority", conn)
        new = df[['priority']].drop_duplicates()
        new = new[~new['priority'].isin(existing['priority'])]
        if not new.empty:
            new.to_sql('dim_priority', conn, schema='tickets', if_exists='append', index=False)

        # dim_status
        existing = pd.read_sql("SELECT status FROM tickets.dim_status", conn)
        new = df[['status']].drop_duplicates()
        new = new[~new['status'].isin(existing['status'])]
        if not new.empty:
            new.to_sql('dim_status', conn, schema='tickets', if_exists='append', index=False)

        # dim_reporter
        existing = pd.read_sql("SELECT reported_by_email FROM tickets.dim_reporter", conn)
        new = df[['reported_by_email']].drop_duplicates()
        new = new[~new['reported_by_email'].isin(existing['reported_by_email'])]
        if not new.empty:
            new.to_sql('dim_reporter', conn, schema='tickets', if_exists='append', index=False)

        # Upsert fact_issues
        df_fact = df[['issue_id', 'project_id', 'property_id', 'issue_type', 'severity', 'priority', 'status', 'reported_by_email', 'updated_at']]

        metadata = MetaData()
        fact_issues_table = Table(
            'fact_issues',
            metadata,
            Column('issue_id', String, primary_key=True),
            Column('project_id', String),
            Column('property_id', String),
            Column('issue_type', String),
            Column('severity', String),
            Column('priority', String),
            Column('status', String),
            Column('reported_by_email', String),
            Column('updated_at', DateTime),
            schema='tickets'
        )

        for _, row in df_fact.iterrows():
            stmt = insert(fact_issues_table).values(
                issue_id=row['issue_id'],
                project_id=row['project_id'],
                property_id=row['property_id'],
                issue_type=row['issue_type'],
                severity=row['severity'],
                priority=row['priority'],
                status=row['status'],
                reported_by_email=row['reported_by_email'],
                updated_at=row['updated_at']
            ).on_conflict_do_update(
                index_elements=['issue_id'],
                set_={
                    'project_id': row['project_id'],
                    'property_id': row['property_id'],
                    'issue_type': row['issue_type'],
                    'severity': row['severity'],
                    'priority': row['priority'],
                    'status': row['status'],
                    'reported_by_email': row['reported_by_email'],
                    'updated_at': row['updated_at']
                }
            )
            conn.execute(stmt)

default_args = {
    'start_date': datetime(2024, 2, 5),
    'retries': 1
}

dag = DAG(
    dag_id='hat',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

with dag:
    create_schema = PythonOperator(
        task_id='create_schema_and_tables',
        python_callable=create_schema_and_tables
    )

    etl = PythonOperator(
        task_id='etl_issue_master',
        python_callable=extract_transform_load
    )

    create_schema >> etl
