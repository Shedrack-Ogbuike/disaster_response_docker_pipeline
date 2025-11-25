import os
import pandas as pd
import psycopg2
import requests
import time
import hashlib
import json
from contextlib import contextmanager


# CONFIGURATION
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

PUBLIC_ASSISTANCE_URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"


# DATABASE CONNECTION
@contextmanager
def get_db_cursor():
    conn = None
    cursor = None
    try:
        for attempt in range(5):
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                conn.autocommit = False
                break
            except psycopg2.OperationalError as e:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                else:
                    raise e
        cursor = conn.cursor()
        yield cursor
        conn.commit()
    except Exception as e:
        if conn:
            print(f"Transaction error: {e} - rolling back")
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# HASH FUNCTION
def generate_hash(record):
    record_str = json.dumps(record, sort_keys=True, default=str)
    return hashlib.md5(record_str.encode()).hexdigest()


# CLEAN RECORD
def clean_record_for_insertion(record: dict) -> dict:
    cleaned_record = record.copy()
    field_max_lengths = {
        'incident_type': 100, 'pw_number': 50, 'application_title': 500,
        'applicant_id': 50, 'damage_category_code': 10, 'damage_category_descrip': 255,
        'project_status': 50, 'project_process_step': 100, 'project_size': 50,
        'county': 100, 'county_code': 10, 'state_abbreviation': 10, 'state_number_code': 10,
        'gm_project_id': 50, 'gm_applicant_id': 50
    }
    for key, value in cleaned_record.items():
        if pd.isna(value) or value in ['NaT', 'nan', 'None', '']:
            cleaned_record[key] = None
        elif isinstance(value, str) and key in field_max_lengths:
            max_len = field_max_lengths[key]
            cleaned_record[key] = value[:max_len] if len(value) > max_len else value
    return cleaned_record


# DATA FETCHING
def fetch_public_assistance_data(limit: int, offset: int) -> pd.DataFrame:
    params = {"$top": limit, "$skip": offset}
    try:
        response = requests.get(PUBLIC_ASSISTANCE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("PublicAssistanceFundedProjectsDetails", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)

        # Convert column names to snake_case to match your database schema
        column_mapping = {
            'disasterNumber': 'disaster_number',
            'declarationDate': 'declaration_date',
            'incidentType': 'incident_type',
            'pwNumber': 'pw_number',
            'applicationTitle': 'application_title',
            'applicantId': 'applicant_id',
            'damageCategoryCode': 'damage_category_code',
            'damageCategoryDescrip': 'damage_category_descrip',
            'projectStatus': 'project_status',
            'projectProcessStep': 'project_process_step',
            'projectSize': 'project_size',
            'county': 'county',
            'countyCode': 'county_code',
            'stateAbbreviation': 'state_abbreviation',
            'stateNumberCode': 'state_number_code',
            'projectAmount': 'project_amount',
            'federalShareObligated': 'federal_share_obligated',
            'totalObligated': 'total_obligated',
            'lastObligationDate': 'last_obligation_date',
            'firstObligationDate': 'first_obligation_date',
            'mitigationAmount': 'mitigation_amount',
            'gmProjectId': 'gm_project_id',
            'gmApplicantId': 'gm_applicant_id',
            'lastRefresh': 'last_refresh',
            'hash': 'hash_value'
        }

        df = df.rename(columns=column_mapping)

        # Process date fields
        date_fields = [
            'declaration_date', 'last_obligation_date',
            'first_obligation_date', 'last_refresh'
        ]
        for field in date_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors='coerce')

        # Process numeric fields
        numeric_fields = [
            'project_amount', 'federal_share_obligated',
            'total_obligated', 'mitigation_amount'
        ]
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)

        return df
    except Exception as e:
        print(f"Error fetching public assistance data: {e}")
        return pd.DataFrame()


# BATCH INSERTION
def batch_insert(cursor, table: str, records: list, unique_keys: list):
    if not records:
        return 0

    # First, get the existing columns in the table
    cursor.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
    """)
    existing_columns = {row[0] for row in cursor.fetchall()}

    skipped_columns = set()
    filtered_records = []
    for record in records:
        filtered_record = {}
        for key, value in record.items():
            if key in existing_columns:
                filtered_record[key] = value
            else:
                skipped_columns.add(key)

        if 'hash_value' in existing_columns and 'hash_value' in record:
            filtered_record['hash_value'] = record['hash_value']

        if 'created_at' in existing_columns and 'created_at' not in filtered_record:
            filtered_record['created_at'] = None
        if 'updated_at' in existing_columns and 'updated_at' not in filtered_record:
            filtered_record['updated_at'] = None

        filtered_records.append(filtered_record)

    if skipped_columns:
        print(f"Skipping columns not in {table}: {sorted(skipped_columns)}")

    if not filtered_records:
        print("No valid records to insert after filtering")
        return 0

    cleaned = [clean_record_for_insertion(r) for r in filtered_records]

    valid_unique_keys = [key for key in unique_keys if key in existing_columns]
    if not valid_unique_keys:
        print(f"No valid unique keys found for {table}")
        return 0

    columns = cleaned[0].keys()
    if not columns:
        print("No columns to insert after filtering")
        return 0

    placeholders = ', '.join([f"%({c})s" for c in columns])
    conflict_clause = ', '.join(valid_unique_keys)
    update_clause = ', '.join([f"{c}=EXCLUDED.{c}" for c in columns if c not in valid_unique_keys])

    sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause}) DO UPDATE
        SET {update_clause};
    """

    print(f"Inserting {len(cleaned)} records into {table} with columns: {list(columns)}")
    cursor.executemany(sql, cleaned)
    return len(cleaned)


# PROCESSING FUNCTION
def process_public_assistance_in_batches():
    records_loaded = 0
    page_size = 100
    offset = 0
    print("=== LOADING PUBLIC ASSISTANCE PROJECTS ===")
    while True:
        df = fetch_public_assistance_data(page_size, offset)
        if df.empty:
            print("No more records to load")
            break

        with get_db_cursor() as cursor:
            loaded = batch_insert(
                cursor,
                'public_assistance_projects',
                df.to_dict(orient='records'),
                ['disaster_number', 'pw_number']
            )

        records_loaded += loaded
        print(f"Loaded {loaded} public assistance records (offset {offset})")

        if len(df) < page_size:
            print("Reached end of dataset")
            break

        offset += page_size
        time.sleep(1)

    return records_loaded


# TRANSFORMATION FUNCTIONS
def populate_fact_tables():
    print("Starting fact table transformations...")

    with get_db_cursor() as cursor:
        try:
            cursor.execute("TRUNCATE fact_disaster_metrics;")
            cursor.execute("TRUNCATE fact_project_samples;")

            print("Populating fact_disaster_metrics...")
            cursor.execute("""
                INSERT INTO fact_disaster_metrics (
                    disaster_number, state_abbreviation, declaration_date,
                    total_projects, total_funding, avg_project_amount, max_project_amount,
                    small_projects, medium_projects, large_projects
                )
                SELECT
                    disaster_number,
                    state_abbreviation,
                    MIN(declaration_date) as declaration_date,
                    COUNT(*) as total_projects,
                    SUM(project_amount) as total_funding,
                    AVG(project_amount) as avg_project_amount,
                    MAX(project_amount) as max_project_amount,
                    COUNT(CASE WHEN project_size = 'Small' THEN 1 END) as small_projects,
                    COUNT(CASE WHEN project_size = 'Medium' THEN 1 END) as medium_projects,
                    COUNT(CASE WHEN project_size = 'Large' THEN 1 END) as large_projects
                FROM public_assistance_projects
                GROUP BY disaster_number, state_abbreviation;
            """)

            metrics_count = cursor.rowcount
            print(f"Loaded {metrics_count} records into fact_disaster_metrics")

            print("Populating fact_project_samples...")
            cursor.execute("""
                INSERT INTO fact_project_samples (
                    disaster_number, state_abbreviation, declaration_date,
                    project_amount, damage_category, project_size,
                    is_large_project, amount_category
                )
                SELECT
                    disaster_number,
                    state_abbreviation,
                    declaration_date,
                    project_amount,
                    damage_category_descrip as damage_category,
                    project_size,
                    (project_amount > 100000) as is_large_project,
                    CASE
                        WHEN project_amount < 10000 THEN 'Small'
                        WHEN project_amount < 100000 THEN 'Medium'
                        ELSE 'Large'
                    END as amount_category
                FROM public_assistance_projects
                WHERE project_amount IS NOT NULL;
            """)

            samples_count = cursor.rowcount
            print(f"Loaded {samples_count} records into fact_project_samples")

            return metrics_count, samples_count

        except Exception as e:
            print(f"Error during fact table transformation: {e}")
            raise


def update_etl_control():
    with get_db_cursor() as cursor:
        cursor.execute("""
            INSERT INTO etl_control (process_name, last_run_timestamp, status)
            VALUES ('public_assistance_etl', NOW(), 'COMPLETED')
            ON CONFLICT (process_name)
            DO UPDATE SET
                last_run_timestamp = NOW(),
                status = 'COMPLETED',
                records_processed = (
                    SELECT COUNT(*) FROM public_assistance_projects
                );
        """)
    print("Updated ETL control table")


# MAIN ETL RUNNER
def run_etl_pipeline():
    print("Starting Public Assistance ETL Pipeline...")

    try:
        public_assistance_loaded = process_public_assistance_in_batches()
        metrics_count, samples_count = populate_fact_tables()
        update_etl_control()

        print("\nETL Summary:")
        print(f"{public_assistance_loaded} public assistance projects loaded")
        print(f"{metrics_count} disaster metrics records created")
        print(f"{samples_count} project samples records created")
        print("ETL Pipeline completed successfully!")

    except Exception as e:
        print(f"ETL Pipeline failed: {e}")
        with get_db_cursor() as cursor:
            cursor.execute("""
                INSERT INTO etl_control (process_name, last_run_timestamp, status)
                VALUES ('public_assistance_etl', NOW(), 'FAILED')
                ON CONFLICT (process_name)
                DO UPDATE SET
                    last_run_timestamp = NOW(),
                    status = 'FAILED';
            """)
        raise


if __name__ == "__main__":
    run_etl_pipeline()
