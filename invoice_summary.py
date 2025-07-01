from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

TARGET_SCHEMA = "analytics_core"

def upsert_invoice_summary():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='analytics_postgres')

    data = src.get_records("""
        SELECT 
            invoice_id, vendor_id, project_name, invoice_number, 
            invoice_type, total_amount, invoice_status, uploaded_by, uploaded_at
        FROM billing.invoice_master
        WHERE invoice_id IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                (
                    invoice_id, vendor_id, project_name, invoice_number,
                    invoice_type, total_amount, status, uploaded_by, uploaded_at
                ) = row

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_vendor (vendor_id)
                    VALUES (%s)
                    ON CONFLICT (vendor_id) DO NOTHING
                    RETURNING vendor_key
                """, (vendor_id,))
                vendor_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT vendor_key FROM {TARGET_SCHEMA}.dim_vendor WHERE vendor_id = %s
                """, parameters=(vendor_id,))[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_project (project_name)
                    VALUES (%s)
                    ON CONFLICT (project_name) DO NOTHING
                    RETURNING project_key
                """, (project_name,))
                project_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT project_key FROM {TARGET_SCHEMA}.dim_project WHERE project_name = %s
                """, parameters=(project_name,))[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_user (user_email)
                    VALUES (%s)
                    ON CONFLICT (user_email) DO NOTHING
                    RETURNING user_key
                """, (uploaded_by,))
                user_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT user_key FROM {TARGET_SCHEMA}.dim_user WHERE user_email = %s
                """, parameters=(uploaded_by,))[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.dim_status (status_name)
                    VALUES (%s)
                    ON CONFLICT (status_name) DO NOTHING
                    RETURNING status_key
                """, (status,))
                status_key = cur.fetchone()[0] if cur.rowcount else dest.get_first(f"""
                    SELECT status_key FROM {TARGET_SCHEMA}.dim_status WHERE status_name = %s
                """, parameters=(status,))[0]

                cur.execute(f"""
                    SELECT date_key FROM {TARGET_SCHEMA}.dim_date WHERE full_date = %s
                """, (uploaded_at.date(),))
                date_key = cur.fetchone()[0]

                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.fact_invoice_summary (
                        invoice_id, vendor_key, project_key, invoice_number,
                        invoice_type, total_amount, status_key,
                        uploaded_by_key, uploaded_date_key
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (invoice_id) DO UPDATE SET
                        vendor_key = EXCLUDED.vendor_key,
                        project_key = EXCLUDED.project_key,
                        invoice_number = EXCLUDED.invoice_number,
                        invoice_type = EXCLUDED.invoice_type,
                        total_amount = EXCLUDED.total_amount,
                        status_key = EXCLUDED.status_key,
                        uploaded_by_key = EXCLUDED.uploaded_by_key,
                        uploaded_date_key = EXCLUDED.uploaded_date_key
                """, (
                    invoice_id, vendor_key, project_key, invoice_number,
                    invoice_type, total_amount, status_key, user_key, date_key
                ))

            conn.commit()

def create_invoice_summary_dag():
    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    dag = DAG(
        dag_id='invoice_summary_analyt',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
        description='ETL for fact_invoice_summary into analytics_core'
    )

    with dag:
        run_etl = PythonOperator(
            task_id='upsert_invoice_summary',
            python_callable=upsert_invoice_summary
        )

    return dag

globals()['invoice_summary_analyt'] = create_invoice_summary_dag()
