import os
import pandas as pd
import psycopg2
import requests
import time
from contextlib import contextmanager
from typing import List, Dict, Any

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "database": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

FEMA_API_BASE_URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
PUBLIC_ASSISTANCE_URL = "https://www.fema.gov/api/open/v2/PublicAssistanceFundedProjectsDetails"
DECLARATIONS_TABLE = "declarations"

@contextmanager
def get_db_cursor():
    conn = None
    try:
        # Retry connection logic for robustness against the PostgreSQL container startup time
        for attempt in range(5):
            try:
                conn = psycopg2.connect(**DB_CONFIG)
                break
            except psycopg2.OperationalError as e:
                if attempt < 4:
                    time.sleep(2 ** attempt)
                else:
                    raise e
        
        yield conn.cursor()
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
            print(f"Database transaction rolled back due to error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def fetch_data(limit: int, offset: int) -> pd.DataFrame:
    params = {"$top": limit, "$skip": offset}
    try:
        response = requests.get(FEMA_API_BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json().get("DisasterDeclarationsSummaries", [])
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        
        # *** ROBUSTNESS FIX ADDED HERE ***
        if df.empty:
            return df
        # **********************************

        # --- DATA CLEANING AND TYPE CONVERSION ---
        
        # 1. Date conversions
        df['declarationDate'] = pd.to_datetime(df.get('declarationDate', pd.NaT), errors='coerce')
        df['firstObligationDate'] = pd.to_datetime(df.get('firstObligationDate', pd.NaT), errors='coerce')
        df['lastObligationDate'] = pd.to_datetime(df.get('lastObligationDate', pd.NaT), errors='coerce')
        
        # 2. Numeric Field Cleaning (FIXED: Handling missing columns before operation)
        numeric_fields = ['projectAmount', 'federalShareObligated', 'totalObligated', 'mitigationAmount']
        for field in numeric_fields:
            # Ensure column exists; if not, create it as zeros before processing.
            if field not in df.columns:
                df[field] = 0.0
                continue # Skip cleaning/coercing if we just created it as 0.0

            # FIX for Python Error: df[field] must be a Pandas Series when using .astype()
            # Ensure the column is treated as a string, strip whitespace, then convert to numeric.
            df[field] = df[field].astype(str).str.strip()
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)


        # 3. Boolean fields (Minor improvement for robustness)
        boolean_fields = ['ihProgramDeclared', 'iaProgramDeclared', 'paProgramDeclared', 'hmProgramDeclared']
        for field in boolean_fields:
            # Check if the field exists, default to False series if missing
            if field not in df.columns:
                 df[field] = False
                 continue

            if pd.api.types.is_object_dtype(df[field].dtype):
                # Replace 'Y'/'N' and fill missing values with False, then cast to bool
                df[field] = df[field].fillna(False).replace({'Y': True, 'N': False}).astype(bool)

        # 4. String fields
        string_fields = [
            'disasterNumber', 'declarationType', 'state', 'stateName', 'countyName',
            'incidentType', 'declarationTitle', 'pwNumber', 'applicationTitle', 
            'applicantId', 'damageCategoryCode', 'damageCategoryDescrip', 
            'projectStatus', 'projectProcessStep', 'gmProjectId', 'gmApplicantId', 'hash'
        ]
        for field in string_fields:
            if field not in df.columns:
                df[field] = ''
            else:
                df[field] = df[field].astype(str).fillna('')

        # 5. FY Declared
        # Ensure column exists first
        if 'fyDeclared' not in df.columns:
            df['fyDeclared'] = 0
        else:
            df['fyDeclared'] = pd.to_numeric(df['fyDeclared'], errors='coerce').fillna(0).astype(int)

        return df

    except Exception as e:
        # Added logging for debugging purposes
        print(f"Error fetching data: {e}")
        return pd.DataFrame()

def fetch_public_assistance_data(limit: int, offset: int) -> pd.DataFrame:
    """Fetch financial project data from the correct endpoint"""
    params = {"$top": limit, "$skip": offset}
    try:
        print(f"📊 Fetching public assistance data: limit={limit}, offset={offset}")
        response = requests.get(PUBLIC_ASSISTANCE_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json().get("PublicAssistanceFundedProjectsDetails", [])
        print(f"✅ Received {len(data)} public assistance records")
        
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        
        # Data cleaning for public assistance data
        # 1. Date conversions
        df['declarationDate'] = pd.to_datetime(df.get('declarationDate', pd.NaT), errors='coerce')
        df['lastObligationDate'] = pd.to_datetime(df.get('lastObligationDate', pd.NaT), errors='coerce')
        df['firstObligationDate'] = pd.to_datetime(df.get('firstObligationDate', pd.NaT), errors='coerce')
        df['lastRefresh'] = pd.to_datetime(df.get('lastRefresh', pd.NaT), errors='coerce')
        
        # 2. Numeric Field Cleaning
        numeric_fields = ['projectAmount', 'federalShareObligated', 'totalObligated', 'mitigationAmount']
        for field in numeric_fields:
            if field not in df.columns:
                df[field] = 0.0
                continue
            df[field] = df[field].astype(str).str.strip()
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)

        # 3. String fields
        string_fields = [
            'disasterNumber', 'incidentType', 'pwNumber', 'applicationTitle', 
            'applicantId', 'damageCategoryCode', 'damageCategoryDescrip', 
            'projectStatus', 'projectProcessStep', 'projectSize', 'county', 
            'countyCode', 'stateAbbreviation', 'stateNumberCode', 'gmProjectId', 
            'gmApplicantId', 'hash'
        ]
        for field in string_fields:
            if field not in df.columns:
                df[field] = ''
            else:
                df[field] = df[field].astype(str).fillna('')

        # Debug: Check financial data
        if 'projectAmount' in df.columns:
            non_zero = (df['projectAmount'] > 0).sum()
            print(f"💰 Records with non-zero project amounts: {non_zero}/{len(df)}")
            if non_zero > 0:
                print(f"💵 Sample project amounts: {df[df['projectAmount'] > 0]['projectAmount'].head(3).tolist()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching public assistance data: {e}")
        return pd.DataFrame()

def insert_declaration_record(cursor, record: dict):
    # Convert all pandas NaT/NaN values to None in the entire record
    def clean_record_value(value):
        if pd.isna(value):
            return None
        return value
    
    # Create a cleaned copy of the record
    cleaned_record = {key: clean_record_value(value) for key, value in record.items()}
    
    cursor.execute(f"""
        INSERT INTO declarations (
            disaster_number, declaration_type, declaration_date, incidentbegindate,
            state, state_name, county_name, incident_type, project_amount, declaration_title,
            fy_declared, ih_program_declared, ia_program_declared, pa_program_declared, hm_program_declared,
            hash_value, last_refresh, pw_number, application_title, applicant_id,
            damage_category_code, damage_category_descrip, project_status, project_process_step,
            federal_share_obligated, total_obligated, last_obligation_date, first_obligation_date,
            mitigation_amount, gm_project_id, gm_applicant_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (disaster_number) DO UPDATE SET
            declaration_type = EXCLUDED.declaration_type,
            declaration_date = EXCLUDED.declaration_date,
            incidentbegindate = EXCLUDED.incidentbegindate,
            state = EXCLUDED.state,
            state_name = EXCLUDED.state_name,
            county_name = EXCLUDED.county_name,
            incident_type = EXCLUDED.incident_type,
            project_amount = EXCLUDED.project_amount,
            declaration_title = EXCLUDED.declaration_title,
            fy_declared = EXCLUDED.fy_declared,
            ih_program_declared = EXCLUDED.ih_program_declared,
            ia_program_declared = EXCLUDED.ia_program_declared,
            pa_program_declared = EXCLUDED.pa_program_declared,
            hm_program_declared = EXCLUDED.hm_program_declared,
            hash_value = EXCLUDED.hash_value,
            last_refresh = NOW(),
            pw_number = EXCLUDED.pw_number,
            application_title = EXCLUDED.application_title,
            applicant_id = EXCLUDED.applicant_id,
            damage_category_code = EXCLUDED.damage_category_code,
            damage_category_descrip = EXCLUDED.damage_category_descrip,
            project_status = EXCLUDED.project_status,
            project_process_step = EXCLUDED.project_process_step,
            federal_share_obligated = EXCLUDED.federal_share_obligated,
            total_obligated = EXCLUDED.total_obligated,
            last_obligation_date = EXCLUDED.last_obligation_date,
            first_obligation_date = EXCLUDED.first_obligation_date,
            mitigation_amount = EXCLUDED.mitigation_amount,
            gm_project_id = EXCLUDED.gm_project_id,
            gm_applicant_id = EXCLUDED.gm_applicant_id;
    """, (
        int(cleaned_record.get("disasterNumber", 0)), 
        cleaned_record.get("declarationType"),
        cleaned_record.get("declarationDate"),
        cleaned_record.get("incidentbegindate"),
        cleaned_record.get("state"), 
        cleaned_record.get("stateName"), 
        cleaned_record.get("countyName"),
        cleaned_record.get("incidentType"), 
        float(cleaned_record.get("projectAmount", 0)),
        cleaned_record.get("declarationTitle"), 
        int(cleaned_record.get("fyDeclared", 0)),
        cleaned_record.get("ihProgramDeclared", False), 
        cleaned_record.get("iaProgramDeclared", False),
        cleaned_record.get("paProgramDeclared", False), 
        cleaned_record.get("hmProgramDeclared", False),
        cleaned_record.get("hash", ''),
        cleaned_record.get("pwNumber"), 
        cleaned_record.get("applicationTitle"), 
        cleaned_record.get("applicantId"),
        cleaned_record.get("damageCategoryCode"), 
        cleaned_record.get("damageCategoryDescrip"),
        cleaned_record.get("projectStatus"), 
        cleaned_record.get("projectProcessStep"),
        float(cleaned_record.get("federalShareObligated", 0)), 
        float(cleaned_record.get("totalObligated", 0)),
        cleaned_record.get("lastObligationDate"),
        cleaned_record.get("firstObligationDate"),
        float(cleaned_record.get("mitigationAmount", 0)),
        cleaned_record.get("gmProjectId"), 
        cleaned_record.get("gmApplicantId")
    ))

def insert_public_assistance_project(cursor, record: dict):
    """Insert financial project data into public_assistance_projects table"""
    
    # Convert NaT to None for dates
    def clean_record_value(value):
        if pd.isna(value):
            return None
        return value
    
    # Create a cleaned copy of the record
    cleaned_record = {key: clean_record_value(value) for key, value in record.items()}
    
    cursor.execute("""
        INSERT INTO public_assistance_projects (
            disaster_number, declaration_date, incident_type, pw_number, 
            application_title, applicant_id, damage_category_code, 
            damage_category_descrip, project_status, project_process_step,
            project_size, county, county_code, state_abbreviation, state_number_code,
            project_amount, federal_share_obligated, total_obligated, 
            last_obligation_date, first_obligation_date, mitigation_amount,
            gm_project_id, gm_applicant_id, last_refresh, hash_value
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (disaster_number, pw_number) DO UPDATE SET
            project_amount = EXCLUDED.project_amount,
            federal_share_obligated = EXCLUDED.federal_share_obligated,
            total_obligated = EXCLUDED.total_obligated,
            last_obligation_date = EXCLUDED.last_obligation_date,
            first_obligation_date = EXCLUDED.first_obligation_date,
            mitigation_amount = EXCLUDED.mitigation_amount,
            last_refresh = EXCLUDED.last_refresh;
    """, (
        int(cleaned_record.get("disasterNumber", 0)),
        cleaned_record.get("declarationDate"),
        cleaned_record.get("incidentType"),
        cleaned_record.get("pwNumber"),
        cleaned_record.get("applicationTitle"),
        cleaned_record.get("applicantId"),
        cleaned_record.get("damageCategoryCode"),
        cleaned_record.get("damageCategoryDescrip"),
        cleaned_record.get("projectStatus"),
        cleaned_record.get("projectProcessStep"),
        cleaned_record.get("projectSize"),
        cleaned_record.get("county"),
        cleaned_record.get("countyCode"),
        cleaned_record.get("stateAbbreviation"),
        cleaned_record.get("stateNumberCode"),
        float(cleaned_record.get("projectAmount", 0)),
        float(cleaned_record.get("federalShareObligated", 0)),
        float(cleaned_record.get("totalObligated", 0)),
        cleaned_record.get("lastObligationDate"),
        cleaned_record.get("firstObligationDate"),
        float(cleaned_record.get("mitigationAmount", 0)),
        cleaned_record.get("gmProjectId"),
        cleaned_record.get("gmApplicantId"),
        cleaned_record.get("lastRefresh"),
        cleaned_record.get("hash", "")
    ))

def populate_dim_date(cursor):
    cursor.execute("""
        INSERT INTO dim_date (date_key, full_date, year, month, day, quarter, day_of_week)
        SELECT
            CAST(TO_CHAR(declaration_date, 'YYYYMMDD') AS INTEGER) AS date_key,
            declaration_date AS full_date,
            EXTRACT(YEAR FROM declaration_date)::INT AS year,
            EXTRACT(MONTH FROM declaration_date)::INT AS month,
            EXTRACT(DAY FROM declaration_date)::INT AS day,
            EXTRACT(QUARTER FROM declaration_date)::INT AS quarter,
            EXTRACT(DOW FROM declaration_date)::INT AS day_of_week
        FROM declarations
        WHERE declaration_date >= '2000-01-01'
        ON CONFLICT (date_key) DO NOTHING;
    """)

def run_star_schema_transformations(cursor):
    # This block requires a UNIQUE constraint on the dim_disaster table
    cursor.execute("""
        INSERT INTO dim_disaster (disaster_number, declaration_type, incident_type, declaration_title)
        SELECT DISTINCT disaster_number, declaration_type, incident_type, declaration_title
        FROM declarations
        ON CONFLICT (disaster_number) DO NOTHING;
    """)
    # This block requires a UNIQUE constraint on the dim_location table
    cursor.execute("""
        INSERT INTO dim_location (state, country_name, latitude, longitude)
        SELECT DISTINCT state, 'unknown', 0.0, 0.0
        FROM declarations
        ON CONFLICT (state, country_name) DO NOTHING;
    """)
    # Updated fact_claims to use actual financial data from public_assistance_projects
    cursor.execute("""
        INSERT INTO fact_claims (date_key, disaster_key, location_key, 
                                project_amount, federal_share_obligated, total_obligated, mitigation_amount,
                                is_ih_claim, is_pa_claim, declaration_date)
        SELECT
            CAST(TO_CHAR(COALESCE(p.declaration_date, d.declaration_date), 'YYYYMMDD') AS INTEGER) AS date_key,
            dd.disaster_key,
            dl.location_key,
            COALESCE(p.project_amount, 0),
            COALESCE(p.federal_share_obligated, 0),
            COALESCE(p.total_obligated, 0),
            COALESCE(p.mitigation_amount, 0),
            COALESCE(d.ih_program_declared, FALSE),
            COALESCE(d.pa_program_declared, FALSE),
            COALESCE(p.declaration_date, d.declaration_date)
        FROM declarations d
        LEFT JOIN public_assistance_projects p ON d.disaster_number = p.disaster_number
        LEFT JOIN dim_disaster dd ON d.disaster_number = dd.disaster_number
        LEFT JOIN dim_location dl ON d.state = dl.state
        ON CONFLICT (disaster_key, location_key, date_key)
        DO UPDATE SET
            project_amount = EXCLUDED.project_amount,
            federal_share_obligated = EXCLUDED.federal_share_obligated,
            total_obligated = EXCLUDED.total_obligated,
            mitigation_amount = EXCLUDED.mitigation_amount,
            is_ih_claim = EXCLUDED.is_ih_claim,
            is_pa_claim = EXCLUDED.is_pa_claim,
            declaration_date = EXCLUDED.declaration_date;
    """)

def run_etl_pipeline():
    records_loaded = 0
    public_assistance_loaded = 0
    page_size = 1000
    max_records = 1000000
    
    with get_db_cursor() as cursor:
        # 1. First, load basic disaster declarations
        print("=== LOADING BASIC DISASTER DECLARATIONS ===")
        offset = 0
        while offset < max_records:
            df = fetch_data(page_size, offset)
            if df.empty:
                break
            for record in df.to_dict(orient='records'):
                insert_declaration_record(cursor, record)
                records_loaded += 1
            if len(df) < page_size:
                break
            offset += page_size
            time.sleep(1)  # Be nice to the API
        
        # 2. Now load financial project data from the NEW endpoint
        print("\n=== LOADING PUBLIC ASSISTANCE FINANCIAL DATA ===")
        offset = 0
        while offset < max_records:
            df = fetch_public_assistance_data(page_size, offset)
            if df.empty:
                break
            for record in df.to_dict(orient='records'):
                insert_public_assistance_project(cursor, record)
                public_assistance_loaded += 1
            if len(df) < page_size:
                break
            offset += page_size
            time.sleep(1)  # Be nice to the API
        
        print(f"\n📈 ETL Summary:")
        print(f"   Basic declarations: {records_loaded} records")
        print(f"   Financial projects: {public_assistance_loaded} records")
        
        # 3. Run your star schema transformations
        print("\n=== BUILDING STAR SCHEMA ===")
        populate_dim_date(cursor)
        run_star_schema_transformations(cursor)

def quick_test():
    """Quick test to verify the financial endpoint works"""
    print("🔍 Quick testing financial data endpoint...")
    df = fetch_public_assistance_data(3, 0)
    if not df.empty and 'projectAmount' in df.columns:
        print(f"✅ Financial data working! Sample amounts: {df['projectAmount'].tolist()}")
        return True
    else:
        print("❌ Financial data not working")
        return False

if __name__ == "__main__":
    time.sleep(5)
    
    # Quick test first
    if quick_test():
        print("\n🎯 Starting full ETL pipeline...")
        run_etl_pipeline()
    else:
        print("❌ ETL aborted - financial data endpoint not working")