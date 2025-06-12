from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def create_tables():
    hook = PostgresHook(postgres_conn_id='dashboards')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS dashboards;

                CREATE TABLE IF NOT EXISTS dashboards.d_invoice (
                    invoice_key SERIAL PRIMARY KEY,
                    invoice_id VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS dashboards.d_vendor (
                    vendor_key SERIAL PRIMARY KEY,
                    vendor_id VARCHAR(20) UNIQUE,
                    vendor_name VARCHAR(255)
                );

                CREATE TABLE IF NOT EXISTS dashboards.d_date (
                    date_key SERIAL PRIMARY KEY,
                    date TIMESTAMP WITHOUT TIME ZONE UNIQUE,
                    month INT,
                    year INT
                );

                CREATE TABLE IF NOT EXISTS dashboards.f_payments (
                    payment_key SERIAL PRIMARY KEY,
                    vendor_key INT REFERENCES dashboards.d_vendor(vendor_key),
                    invoice_key INT REFERENCES dashboards.d_invoice(invoice_key),
                    date_key INT REFERENCES dashboards.d_date(date_key),
                    total_bill_amount VARCHAR(100),
                    amount_to_pay NUMERIC,
                    paid_amount NUMERIC,
                    overdue_amount NUMERIC,
                    taxes_sgst NUMERIC,
                    taxes_cgst NUMERIC,
                    taxes_igst VARCHAR(50),
                    taxes_gst_percentage VARCHAR(50)
                );
            """)
            conn.commit()

def load_f_payments():
    src = PostgresHook(postgres_conn_id='avenue')
    dest = PostgresHook(postgres_conn_id='dashboards')

    data = src.get_records("""
        SELECT 
            bi.avenue_created_invoice_id,
            v.vendor_id,
            v.vendor_name,
            bi.created_date,
            bi.total_bill_amount,
            bi.amount_to_pay,
            bi.paid_amount,
            bi.overdue_amount,
            bi.taxes_sgst,
            bi.taxes_cgst,
            bi.taxes_igst,
            bi.taxes_gst_percentage
        FROM billing.invoice_master bi
        JOIN public.vendor v 
          ON LOWER(bi.supplier_name) ILIKE '%' || LOWER(v.vendor_name) || '%'
          OR LOWER(v.vendor_name) ILIKE '%' || LOWER(bi.supplier_name) || '%'
        WHERE bi.avenue_created_invoice_id IS NOT NULL
          AND bi.created_date IS NOT NULL
          AND bi.supplier_name IS NOT NULL
    """)

    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            for row in data:
                invoice_id, vendor_id, vendor_name, created_date, total_bill_amount, amount_to_pay, paid_amount, overdue_amount, taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage = row

                cur.execute("""
                    INSERT INTO dashboards.d_invoice (invoice_id)
                    VALUES (%s)
                    ON CONFLICT (invoice_id) DO NOTHING
                    RETURNING invoice_key
                """, (invoice_id,))
                invoice_key_row = cur.fetchone()
                if not invoice_key_row:
                    cur.execute("SELECT invoice_key FROM dashboards.d_invoice WHERE invoice_id = %s", (invoice_id,))
                    invoice_key = cur.fetchone()[0]
                else:
                    invoice_key = invoice_key_row[0]

                cur.execute("""
                    INSERT INTO dashboards.d_vendor (vendor_id, vendor_name)
                    VALUES (%s, %s)
                    ON CONFLICT (vendor_id) DO NOTHING
                    RETURNING vendor_key
                """, (vendor_id, vendor_name))
                vendor_key_row = cur.fetchone()
                if not vendor_key_row:
                    cur.execute("SELECT vendor_key FROM dashboards.d_vendor WHERE vendor_id = %s", (vendor_id,))
                    vendor_key = cur.fetchone()[0]
                else:
                    vendor_key = vendor_key_row[0]

                cur.execute("""
                    INSERT INTO dashboards.d_date (date, month, year)
                    VALUES (%s, EXTRACT(MONTH FROM %s), EXTRACT(YEAR FROM %s))
                    ON CONFLICT (date) DO NOTHING
                    RETURNING date_key
                """, (created_date, created_date, created_date))
                date_key_row = cur.fetchone()
                if not date_key_row:
                    cur.execute("SELECT date_key FROM dashboards.d_date WHERE date = %s", (created_date,))
                    date_key = cur.fetchone()[0]
                else:
                    date_key = date_key_row[0]

                cur.execute("""
                    INSERT INTO dashboards.f_payments (
                        vendor_key, invoice_key, date_key,
                        total_bill_amount, amount_to_pay, paid_amount, overdue_amount,
                        taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    vendor_key, invoice_key, date_key,
                    total_bill_amount, amount_to_pay, paid_amount, overdue_amount,
                    taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage
                ))
            conn.commit()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'vendors',
    default_args=default_args,
    description='ETL from transactional to dashboards schema',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

load_f_payments_task = PythonOperator(
    task_id='load_f_payments',
    python_callable=load_f_payments,
    dag=dag,
)

create_tables_task >> load_f_payments_task