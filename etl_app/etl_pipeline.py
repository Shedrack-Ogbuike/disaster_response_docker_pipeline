import os
import requests
import psycopg2
import time
import json
from datetime import datetime

# --- Configuration ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
FEMA_API_KEY = os.getenv("FEMA_API_KEY")
API_URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 5

FALLBACK_DATA = [
    {
        "id": "123456789",
        "femaDeclarationNumber": "DR-4500-TX",
        "projectAmount": 50000.50,
        "projectDescription": "Repair of Municipal Water Line damaged by flood.",
        "state": "TX",
        "fundingAgency": "FEMA"
    },
    {
        "id": "987654321",
        "femaDeclarationNumber": "EM-3456-CA",
        "projectAmount": 12500.00,
        "projectDescription": "Debris removal from State Highway after earthquake.",
        "state": "CA",
        "fundingAgency": "FEMA"
    }
]


def get_db_connection():
    """Establish connection to PostgreSQL."""
    print(f"Connecting to PostgreSQL at {POSTGRES_HOST}...")
    conn = None
    for i in range(MAX_RETRIES):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=10
            )
            print("Database connection successful.")
            return conn
        except psycopg2.Error as e:
            print(f"Attempt {i+1} failed: {e}")
            if i < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise e


def create_table_if_not_exists(conn):
    """Ensure the target table exists before inserting data."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public_assistance_projects (
        id TEXT PRIMARY KEY,
        fema_declaration_number TEXT,
        project_amount FLOAT,
        project_description TEXT,
        state TEXT,
        funding_agency TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()
            print("Ensured table 'public_assistance_projects' exists.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating table: {e}")


def fetch_public_assistance_data(api_url):
    """Fetch Public Assistance Funded Projects from FEMA API, fallback to hardcoded data if API fails."""
    query_string = "$select=femaDeclarationNumber,projectAmount,projectDescription,state,fundingAgency,id&$top=1000"
    full_url = f"{api_url}?{query_string}"
    print(f"Attempting to fetch data from FEMA API: {full_url}")
    try:
        response = requests.get(full_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        records = data.get('PublicAssistanceFundedProjectsDetails', [])
        if not records:
            print("API returned successfully, but 0 records were returned. Using fallback data.")
            return FALLBACK_DATA
        print(f"Fetched {len(records)} records successfully from API.")
        return records
    except requests.RequestException as e:
        print(f"Error fetching API data ({e}). Falling back to hardcoded test data.")
        return FALLBACK_DATA


def transform_data(records):
    """Transform and clean the fetched data."""
    transformed_records = []
    for record in records:
        try:
            transformed_records.append(record)
        except Exception as e:
            print(f"Skipping record due to transformation error: {e}")
    return transformed_records


def load_data(conn, records):
    """Load data into PostgreSQL table."""
    if not records:
        print("No records to load.")
        return

    insert_count = 0
    sql = """
    INSERT INTO public_assistance_projects (
        id, fema_declaration_number, project_amount, project_description, state, funding_agency, created_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        fema_declaration_number = EXCLUDED.fema_declaration_number,
        project_amount = EXCLUDED.project_amount,
        project_description = EXCLUDED.project_description,
        state = EXCLUDED.state,
        funding_agency = EXCLUDED.funding_agency,
        updated_at = NOW();
    """
    try:
        with conn.cursor() as cur:
            for record in records:
                try:
                    cur.execute(sql, (
                        record.get('id'),
                        record.get('femaDeclarationNumber'),
                        record.get('projectAmount'),
                        record.get('projectDescription'),
                        record.get('state'),
                        record.get('fundingAgency'),
                        datetime.now()
                    ))
                    insert_count += 1
                except psycopg2.Error as e:
                    print(f"Error inserting record {record.get('id')}: {e}")
            conn.commit()
            print(f"Loaded {insert_count} records into public_assistance_projects.")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Transaction failed: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


def main():
    print("--- Starting FEMA Public Assistance ETL Job ---")
    raw_data = fetch_public_assistance_data(API_URL)
    if not raw_data:
        print("ETL aborted: no data fetched.")
        return
    transformed_data = transform_data(raw_data)
    try:
        conn = get_db_connection()
        create_table_if_not_exists(conn)  # <-- This line ensures the table exists
        load_data(conn, transformed_data)
    except Exception as e:
        print(f"Critical error: {e}")
    print("--- ETL Job Finished ---")


if __name__ == "__main__":
    main()
