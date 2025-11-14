--- Initializing Disaster Analytics Database Schema ---

-- 1. Raw Staging Table: FEMA Disaster Declarations Summaries
DROP TABLE IF EXISTS declarations CASCADE;
CREATE TABLE declarations (
    disaster_number INTEGER PRIMARY KEY,
    declaration_type VARCHAR(50),
    declaration_date TIMESTAMP,
    incidentbegindate VARCHAR(100),
    state VARCHAR(20),
    state_name VARCHAR(50),
    county_name VARCHAR(100),
    incident_type VARCHAR(100),
    project_amount NUMERIC(18,2) DEFAULT 0,
    declaration_title VARCHAR(255),
    fy_declared INTEGER,
    ih_program_declared BOOLEAN DEFAULT FALSE,
    ia_program_declared BOOLEAN DEFAULT FALSE,
    pa_program_declared BOOLEAN DEFAULT FALSE,
    hm_program_declared BOOLEAN DEFAULT FALSE,
    pw_number VARCHAR(50),
    application_title VARCHAR(255),
    applicant_id VARCHAR(50),
    damage_category_code VARCHAR(10),
    damage_category_descrip VARCHAR(255),
    project_status VARCHAR(50),
    project_process_step VARCHAR(100),
    federal_share_obligated NUMERIC(18,2),
    total_obligated NUMERIC(18,2),
    last_obligation_date TIMESTAMP,
    first_obligation_date TIMESTAMP,
    mitigation_amount NUMERIC(18,2),
    gm_project_id VARCHAR(50),
    gm_applicant_id VARCHAR(50),
    last_refresh TIMESTAMP WITH TIME ZONE,
    hash_value VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. Raw Staging Table: Public Assistance Projects
DROP TABLE IF EXISTS public_assistance_projects CASCADE;
CREATE TABLE public_assistance_projects (
    disaster_number INTEGER,
    declaration_date TIMESTAMP,
    incident_type VARCHAR(100),
    pw_number VARCHAR(50),
    application_title VARCHAR(255),
    applicant_id VARCHAR(50),
    damage_category_code VARCHAR(10),
    damage_category_descrip VARCHAR(255),
    project_status VARCHAR(50),
    project_process_step VARCHAR(100),
    project_size VARCHAR(50),
    county VARCHAR(100),
    county_code VARCHAR(10),
    state_abbreviation VARCHAR(10),
    state_number_code VARCHAR(10),
    project_amount NUMERIC(18, 2),
    federal_share_obligated NUMERIC(18, 2),
    total_obligated NUMERIC(18, 2),
    last_obligation_date TIMESTAMP,
    first_obligation_date TIMESTAMP,
    mitigation_amount NUMERIC(18, 2),
    gm_project_id VARCHAR(50),
    gm_applicant_id VARCHAR(50),
    last_refresh TIMESTAMP WITH TIME ZONE,
    hash_value VARCHAR(64),
    PRIMARY KEY (disaster_number, pw_number)
);

-- Indexes (with DROP statements to avoid duplicates)
DROP INDEX IF EXISTS idx_declarations_date;
DROP INDEX IF EXISTS idx_public_assistance_disaster;
DROP INDEX IF EXISTS idx_projects_last_refresh;
DROP INDEX IF EXISTS idx_metrics_disaster_date;
DROP INDEX IF EXISTS idx_samples_amount;

CREATE INDEX idx_declarations_date ON declarations (declaration_date);
CREATE INDEX idx_public_assistance_disaster ON public_assistance_projects (disaster_number);
CREATE INDEX idx_projects_last_refresh ON public_assistance_projects (last_refresh);

-- 3. Power BI Optimized Tables
DROP TABLE IF EXISTS fact_disaster_metrics CASCADE;
CREATE TABLE fact_disaster_metrics (
    fact_key SERIAL PRIMARY KEY,
    disaster_key INTEGER,
    location_key INTEGER,
    date_key INTEGER,
    total_projects INTEGER,
    total_funding NUMERIC(18,2),
    avg_project_amount NUMERIC(18,2),
    max_project_amount NUMERIC(18,2),
    small_projects INTEGER,
    medium_projects INTEGER,
    large_projects INTEGER,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

DROP TABLE IF EXISTS fact_project_samples CASCADE;
CREATE TABLE fact_project_samples (
    project_id VARCHAR(100) PRIMARY KEY,
    disaster_key INTEGER,
    location_key INTEGER,
    date_key INTEGER,
    project_amount NUMERIC(18,2),
    damage_category VARCHAR(100),
    project_size VARCHAR(50),
    is_large_project BOOLEAN,
    amount_category VARCHAR(20),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Performance indexes for Power BI queries
CREATE INDEX idx_metrics_disaster_date ON fact_disaster_metrics (disaster_key, date_key);
CREATE INDEX idx_metrics_location_date ON fact_disaster_metrics (location_key, date_key);
CREATE INDEX idx_samples_amount ON fact_project_samples (project_amount DESC);
CREATE INDEX idx_samples_disaster ON fact_project_samples (disaster_key);

-- 4. ETL Control Table for Incremental Processing
DROP TABLE IF EXISTS etl_control CASCADE;
CREATE TABLE etl_control (
    control_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) UNIQUE,
    last_run_timestamp TIMESTAMP WITH TIME ZONE,
    last_offset INTEGER DEFAULT 0,
    records_processed INTEGER DEFAULT 0,
    status VARCHAR(50) DEFAULT 'IDLE'
);

INSERT INTO etl_control (process_name, last_run_timestamp) VALUES 
('public_assistance_projects', NOW() - INTERVAL '1 day'),
('declarations', NOW() - INTERVAL '1 day')
ON CONFLICT (process_name) DO NOTHING;

-- 5. Dimension Tables
DROP TABLE IF EXISTS dim_disaster CASCADE;
CREATE TABLE dim_disaster (
    disaster_key SERIAL PRIMARY KEY,
    disaster_number INTEGER UNIQUE,
    declaration_type VARCHAR(255),
    incident_type VARCHAR(225),
    declaration_title VARCHAR(500)
);

DROP TABLE IF EXISTS dim_location CASCADE;
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    state VARCHAR(20) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    division VARCHAR(50),
    UNIQUE (state, country_name)
);

-- Pre-populate with US states and regions for better analytics
INSERT INTO dim_location (state, country_name, region, division) VALUES
('AL', 'USA', 'South', 'East South Central'),
('AK', 'USA', 'West', 'Pacific'),
('AZ', 'USA', 'West', 'Mountain'),
('AR', 'USA', 'South', 'West South Central'),
('CA', 'USA', 'West', 'Pacific'),
('CO', 'USA', 'West', 'Mountain'),
('CT', 'USA', 'Northeast', 'New England'),
('DE', 'USA', 'South', 'South Atlantic'),
('FL', 'USA', 'South', 'South Atlantic'),
('GA', 'USA', 'South', 'South Atlantic'),
('HI', 'USA', 'West', 'Pacific'),
('ID', 'USA', 'West', 'Mountain'),
('IL', 'USA', 'Midwest', 'East North Central'),
('IN', 'USA', 'Midwest', 'East North Central'),
('IA', 'USA', 'Midwest', 'West North Central'),
('KS', 'USA', 'Midwest', 'West North Central'),
('KY', 'USA', 'South', 'East South Central'),
('LA', 'USA', 'South', 'West South Central'),
('ME', 'USA', 'Northeast', 'New England'),
('MD', 'USA', 'South', 'South Atlantic'),
('MA', 'USA', 'Northeast', 'New England'),
('MI', 'USA', 'Midwest', 'East North Central'),
('MN', 'USA', 'Midwest', 'West North Central'),
('MS', 'USA', 'South', 'East South Central'),
('MO', 'USA', 'Midwest', 'West North Central'),
('MT', 'USA', 'West', 'Mountain'),
('NE', 'USA', 'Midwest', 'West North Central'),
('NV', 'USA', 'West', 'Mountain'),
('NH', 'USA', 'Northeast', 'New England'),
('NJ', 'USA', 'Northeast', 'Middle Atlantic'),
('NM', 'USA', 'West', 'Mountain'),
('NY', 'USA', 'Northeast', 'Middle Atlantic'),
('NC', 'USA', 'South', 'South Atlantic'),
('ND', 'USA', 'Midwest', 'West North Central'),
('OH', 'USA', 'Midwest', 'East North Central'),
('OK', 'USA', 'South', 'West South Central'),
('OR', 'USA', 'West', 'Pacific'),
('PA', 'USA', 'Northeast', 'Middle Atlantic'),
('RI', 'USA', 'Northeast', 'New England'),
('SC', 'USA', 'South', 'South Atlantic'),
('SD', 'USA', 'Midwest', 'West North Central'),
('TN', 'USA', 'South', 'East South Central'),
('TX', 'USA', 'South', 'West South Central'),
('UT', 'USA', 'West', 'Mountain'),
('VT', 'USA', 'Northeast', 'New England'),
('VA', 'USA', 'South', 'South Atlantic'),
('WA', 'USA', 'West', 'Pacific'),
('WV', 'USA', 'South', 'South Atlantic'),
('WI', 'USA', 'Midwest', 'East North Central'),
('WY', 'USA', 'West', 'Mountain'),
('DC', 'USA', 'South', 'South Atlantic'),
('PR', 'USA', 'South', 'South Atlantic'),
('VI', 'USA', 'South', 'South Atlantic'),
('GU', 'USA', 'West', 'Pacific'),
('AS', 'USA', 'West', 'Pacific'),
('MP', 'USA', 'West', 'Pacific')
ON CONFLICT (state, country_name) DO NOTHING;

DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    quarter INT, 
    day_of_week INT,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    week_of_year INT
);

INSERT INTO dim_date (date_key, full_date, year, month, month_name, day, quarter, day_of_week, day_name, is_weekend, week_of_year)
SELECT
    to_char(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend,
    EXTRACT(WEEK FROM d)::INT AS week_of_year
FROM generate_series('2000-01-01', CURRENT_DATE + INTERVAL '2 years', '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;

--- Database Schema Initialization Complete ---