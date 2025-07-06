from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import calendar

TARGET_SCHEMA = "analytics"

def insert_date_if_missing_and_get_key(cur, date):
    if not date:
        return None

    date_key = int(date.strftime("%Y%m%d"))

    cur.execute(f"""
        INSERT INTO {TARGET_SCHEMA}.d_date (date_key, date, month, month_name, quarter, year)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date) DO NOTHING
    """, (
        date_key,
        date,
        date.month,
        calendar.month_name[date.month],
        (date.month - 1) // 3 + 1,
        date.year
    ))

    return date_key

def create_tables():
    hook = PostgresHook(postgres_conn_id='superset')
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_invoice (
                    invoice_key SERIAL PRIMARY KEY,
                    invoice_id VARCHAR(100) UNIQUE
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_vendor (
                    vendor_key SERIAL PRIMARY KEY,
                    vendor_id VARCHAR(20) UNIQUE,
                    vendor_name VARCHAR(255)
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.d_date (
                    date_key INT PRIMARY KEY,
                    date DATE UNIQUE,
                    month INT,
                    month_name VARCHAR(20),
                    quarter INT,
                    year INT
                );

                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.f_payments (
                    payment_key SERIAL PRIMARY KEY,
                    vendor_key INT REFERENCES {TARGET_SCHEMA}.d_vendor(vendor_key),
                    invoice_key INT REFERENCES {TARGET_SCHEMA}.d_invoice(invoice_key),
                    date_key INT REFERENCES {TARGET_SCHEMA}.d_date(date_key),
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
    dest = PostgresHook(postgres_conn_id='superset')

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
                (
                    invoice_id, vendor_id, vendor_name, created_dt,
                    total_bill_amount, amount_to_pay, paid_amount, overdue_amount,
                    taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage
                ) = row

                date_only = created_dt.date()
                date_key = insert_date_if_missing_and_get_key(cur, date_only)
                if not date_key:
                    continue  # skip row if date invalid

                # Insert d_invoice
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_invoice (invoice_id)
                    VALUES (%s)
                    ON CONFLICT (invoice_id) DO NOTHING
                    RETURNING invoice_key
                """, (invoice_id,))
                invoice_key_row = cur.fetchone()
                if not invoice_key_row:
                    cur.execute(f"SELECT invoice_key FROM {TARGET_SCHEMA}.d_invoice WHERE invoice_id = %s", (invoice_id,))
                    invoice_key = cur.fetchone()[0]
                else:
                    invoice_key = invoice_key_row[0]

                # Insert d_vendor
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.d_vendor (vendor_id, vendor_name)
                    VALUES (%s, %s)
                    ON CONFLICT (vendor_id) DO NOTHING
                    RETURNING vendor_key
                """, (vendor_id, vendor_name))
                vendor_key_row = cur.fetchone()
                if not vendor_key_row:
                    cur.execute(f"SELECT vendor_key FROM {TARGET_SCHEMA}.d_vendor WHERE vendor_id = %s", (vendor_id,))
                    vendor_key = cur.fetchone()[0]
                else:
                    vendor_key = vendor_key_row[0]

                # Insert f_payments with deduplication
                cur.execute(f"""
                    INSERT INTO {TARGET_SCHEMA}.f_payments (
                        vendor_key, invoice_key, date_key,
                        total_bill_amount, amount_to_pay, paid_amount, overdue_amount,
                        taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage
                    )
                    SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {TARGET_SCHEMA}.f_payments
                        WHERE vendor_key = %s AND invoice_key = %s AND date_key = %s
                    )
                """, (
                    vendor_key, invoice_key, date_key,
                    total_bill_amount, amount_to_pay, paid_amount, overdue_amount,
                    taxes_sgst, taxes_cgst, taxes_igst, taxes_gst_percentage,
                    vendor_key, invoice_key, date_key
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
    'vendors_olap_etl_superset_25',
    default_args=default_args,
    description='ETL to Superset analytics DB with reusable d_date logic',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

create_tables_task = PythonOperator(
    task_id='create_olap_schema',
    python_callable=create_tables,
    dag=dag,
)

load_f_payments_task = PythonOperator(
    task_id='load_olap_data',
    python_callable=load_f_payments,
    dag=dag,
)

create_tables_task >> load_f_payments_task
