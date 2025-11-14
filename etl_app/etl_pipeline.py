import os
import pandas as pd
import psycopg2
import requests
import time
import hashlib
import json
from datetime import datetime
from contextlib import contextmanager

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres_db"),
    "database": os.getenv("POSTGRES_DB", "disaster_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "Students28")
}

FEMA_API_BASE_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
PUBLIC_ASSISTANCE_URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"

# --- DATABASE CONNECTION ---
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
            print(f"❌ Transaction error: {e} - rolling back")
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# --- HASH FUNCTION ---
def generate_hash(record):
    record_str = json.dumps(record, sort_keys=True, default=str)
    return hashlib.md5(record_str.encode()).hexdigest()

# --- CLEAN RECORD ---
def clean_record_for_insertion(record: dict) -> dict:
    cleaned_record = record.copy()
    field_max_lengths = {
        'declarationType': 50, 'state': 50, 'stateName': 100, 'countyName': 200,
        'incidentType': 200, 'declarationTitle': 500, 'pwNumber': 100, 'applicationTitle': 500,
        'applicantId': 100, 'damageCategoryCode': 50, 'damageCategoryDescrip': 500,
        'projectStatus': 100, 'projectProcessStep': 200, 'gmProjectId': 100, 'gmApplicantId': 100,
        'incidentBeginDate': 50, 'fyDeclared': 10
    }
    for key, value in cleaned_record.items():
        if pd.isna(value) or value in ['NaT', 'nan', 'None', '']:
            cleaned_record[key] = None
        elif isinstance(value, str) and key in field_max_lengths:
            cleaned_record[key] = value[:field_max_lengths[key]] if len(value) > field_max_lengths[key] else value
    return cleaned_record

# --- DATA FETCHING ---
def fetch_data(limit: int, offset: int) -> pd.DataFrame:
    params = {"$top": limit, "$skip": offset}
    try:
        response = requests.get(FEMA_API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get("DisasterDeclarationsSummaries", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        date_fields = ['declarationDate', 'firstObligationDate', 'lastObligationDate', 'incidentBeginDate']
        for field in date_fields:
            df[field] = pd.to_datetime(df.get(field, None), errors='coerce').where(lambda x: x.notna(), None)
        numeric_fields = ['projectAmount', 'federalShareObligated', 'totalObligated', 'mitigationAmount']
        for field in numeric_fields:
            df[field] = pd.to_numeric(df.get(field, 0), errors='coerce').fillna(0)
        boolean_fields = ['ihProgramDeclared', 'iaProgramDeclared', 'paProgramDeclared', 'hmProgramDeclared']
        for field in boolean_fields:
            df[field] = df.get(field, False).replace({'Y': True, 'N': False}).astype(bool)
        df['stateName'] = df.get('state', '')
        df['countyName'] = df.get('designatedArea', '').str.replace(r'\s+\(County\)', '', regex=True).str.strip()
        df['fyDeclared'] = pd.to_numeric(df.get('fyDeclared', 0), errors='coerce').fillna(0).astype(int)
        for col in ['disasterNumber', 'declarationType', 'state', 'stateName', 'countyName',
                    'incidentType', 'declarationTitle', 'pwNumber', 'applicationTitle',
                    'applicantId', 'damageCategoryCode', 'damageCategoryDescrip',
                    'projectStatus', 'projectProcessStep', 'gmProjectId', 'gmApplicantId']:
            if col not in df.columns:
                df[col] = ''
        return df
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

def fetch_public_assistance_data(limit: int, offset: int) -> pd.DataFrame:
    params = {"$top": limit, "$skip": offset}
    try:
        response = requests.get(PUBLIC_ASSISTANCE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json().get("PublicAssistanceFundedProjectsDetails", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        date_fields = ['declarationDate', 'lastObligationDate', 'firstObligationDate', 'lastRefresh']
        for field in date_fields:
            df[field] = pd.to_datetime(df.get(field, None), errors='coerce').where(lambda x: x.notna(), None)
        numeric_fields = ['projectAmount', 'federalShareObligated', 'totalObligated', 'mitigationAmount']
        for field in numeric_fields:
            df[field] = pd.to_numeric(df.get(field, 0), errors='coerce').fillna(0)
        for col in ['disasterNumber', 'incidentType', 'pwNumber', 'applicationTitle', 'applicantId',
                    'damageCategoryCode', 'damageCategoryDescrip', 'projectStatus', 'projectProcessStep',
                    'projectSize', 'county', 'countyCode', 'stateAbbreviation', 'stateNumberCode',
                    'gmProjectId', 'gmApplicantId']:
            if col not in df.columns:
                df[col] = ''
        return df
    except Exception as e:
        print(f"❌ Error fetching public assistance data: {e}")
        return pd.DataFrame()

# --- BATCH INSERTION ---
def batch_insert(cursor, table: str, records: list, unique_keys: list):
    if not records:
        return 0
    cleaned = [clean_record_for_insertion(r) for r in records]
    columns = cleaned[0].keys()
    values = [[r.get(c) for c in columns] for r in cleaned]
    placeholders = ', '.join([f"%({c})s" for c in columns])
    conflict_clause = ', '.join(unique_keys)
    update_clause = ', '.join([f"{c}=EXCLUDED.{c}" for c in columns if c not in unique_keys])
    sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause}) DO UPDATE
        SET {update_clause};
    """
    cursor.executemany(sql, cleaned)
    return len(cleaned)

def process_declarations_in_batches():
    records_loaded = 0
    page_size = 100
    offset = 0
    print("=== LOADING BASIC DISASTER DECLARATIONS ===")
    while True:
        df = fetch_data(page_size, offset)
        if df.empty:
            break
        with get_db_cursor() as cursor:
            for r in df.to_dict(orient='records'):
                r['hash'] = generate_hash(r)
            loaded = batch_insert(cursor, 'declarations', df.to_dict(orient='records'), ['disaster_number'])
        records_loaded += loaded
        print(f"✅ Loaded {loaded} declaration records (offset {offset})")
        if len(df) < page_size:
            break
        offset += page_size
        time.sleep(1)
    return records_loaded

def process_public_assistance_in_batches():
    records_loaded = 0
    page_size = 100
    offset = 0
    print("=== LOADING PUBLIC ASSISTANCE FINANCIAL DATA ===")
    while True:
        df = fetch_public_assistance_data(page_size, offset)
        if df.empty:
            break
        with get_db_cursor() as cursor:
            for r in df.to_dict(orient='records'):
                r['hash'] = generate_hash(r)
            loaded = batch_insert(cursor, 'public_assistance_projects', df.to_dict(orient='records'), ['disaster_number', 'pw_number'])
        records_loaded += loaded
        print(f"✅ Loaded {loaded} public assistance records (offset {offset})")
        if len(df) < page_size:
            break
        offset += page_size
        time.sleep(1)
    return records_loaded

# --- MAIN ETL RUNNER ---
def run_etl_pipeline():
    records_loaded = process_declarations_in_batches()
    public_assistance_loaded = process_public_assistance_in_batches()
    print(f"\n📈 ETL Summary: {records_loaded} declarations, {public_assistance_loaded} financial projects")

if __name__ == "__main__":
    print("🚀 Starting Disaster Analytics ETL Pipeline...")
    run_etl_pipeline()
