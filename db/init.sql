-- File: db/init.sql
-- This script runs automatically when the PostgreSQL container starts for the first time,
-- due to the volume mapping in docker-compose.yml.

-- 1. Create the main table for FEMA Disaster Declarations
CREATE TABLE IF NOT EXISTS declarations (
    id VARCHAR(50) PRIMARY KEY, -- Unique identifier from FEMA API, used for UPSERT operations

    -- Declaration details
    declaration_type VARCHAR(10) NOT NULL, -- e.g., 'DR' (Disaster), 'EM' (Emergency), 'FM' (Fire)
    declaration_date DATE NOT NULL,
    declaration_title VARCHAR(255) NOT NULL,

    -- Disaster/Incident classification
    disaster_type VARCHAR(50), -- e.g., 'Hurricane', 'Severe Storm', 'Tornado'
    incident_type VARCHAR(50), -- e.g., 'Severe Storm', 'Coastal Storm'
    state CHAR(2) NOT NULL,    -- Two-letter state code

    -- Metadata for tracking records
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

-- 2. Create table for FEMA Public Assistance Funded Projects
CREATE TABLE IF NOT EXISTS public_assistance_projects (
    id VARCHAR(50) PRIMARY KEY,
    fema_declaration_number VARCHAR(50),
    project_amount NUMERIC,
    project_description TEXT,
    state CHAR(2),
    funding_agency VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ
);

-- 3. Add indexes for query performance

-- Index for filtering declarations by state
CREATE INDEX IF NOT EXISTS idx_declarations_state ON declarations (state);

-- Index for time-series queries on declaration_date
CREATE INDEX IF NOT EXISTS idx_declarations_date ON declarations (declaration_date DESC);

-- 4. Create a custom view for analytics (optional but recommended)
-- Provides a focused dataset for tools like Power BI or Tableau.
CREATE OR REPLACE VIEW vw_disaster_analytics AS
SELECT
    id,
    declaration_type,
    declaration_date,
    EXTRACT(YEAR FROM declaration_date) AS declaration_year,
    declaration_title,
    disaster_type,
    incident_type,
    state
FROM
    declarations
WHERE
    declaration_type IN ('DR', 'EM') -- Focus on major Disasters and Emergencies
ORDER BY
    declaration_date DESC;
