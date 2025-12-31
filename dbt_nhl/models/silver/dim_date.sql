{{
  config(
    materialized='table',
    tags=['silver', 'dimension']
  )
}}

-- Silver layer: Date dimension for time intelligence
-- Generates a complete date dimension for NHL analytics
-- Includes NHL season logic (spans calendar years Oct-June)

WITH numbers AS (
    -- Generate a sequence of numbers: 0, 1, 2, 3, ... 2499
    -- Using Snowflake's GENERATOR to create rows, then numbering them
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS n
    FROM TABLE(GENERATOR(ROWCOUNT => 2500))
),

date_spine AS (
    -- Generate dates from 2020-01-01 through ~7 years in the future
    -- Add each number as days to the start date
    SELECT
        DATEADD(DAY, n, '2020-01-01'::DATE) AS date_day
    FROM numbers
),

game_dates AS (
    -- Identify which dates have games scheduled/played
    SELECT DISTINCT
        partition_date AS game_date,
        COUNT(*) AS games_on_date
    FROM {{ ref('bronze_game_boxscore_snapshots') }}
    WHERE partition_date IS NOT NULL
    GROUP BY partition_date
)

SELECT
    -- Primary Key
    ds.date_day AS date_key,
    
    -- Date Components
    YEAR(ds.date_day) AS year,
    QUARTER(ds.date_day) AS quarter,
    MONTH(ds.date_day) AS month,
    DAY(ds.date_day) AS day,
    DAYOFWEEK(ds.date_day) AS day_of_week,  -- 0=Sunday, 6=Saturday
    DAYOFYEAR(ds.date_day) AS day_of_year,
    WEEKOFYEAR(ds.date_day) AS week_of_year,
    
    -- Date Descriptions
    TO_CHAR(ds.date_day, 'Month') AS month_name,
    TO_CHAR(ds.date_day, 'Mon') AS month_abbrev,
    TO_CHAR(ds.date_day, 'Day') AS day_name,
    TO_CHAR(ds.date_day, 'Dy') AS day_abbrev,
    
    -- NHL Season Logic
    -- NHL season starts in October and ends in June
    -- Season 20252026 means Oct 2025 - June 2026
    CASE 
        WHEN MONTH(ds.date_day) >= 10 
        THEN YEAR(ds.date_day) * 10000 + (YEAR(ds.date_day) + 1)
        ELSE (YEAR(ds.date_day) - 1) * 10000 + YEAR(ds.date_day)
    END AS nhl_season,
    
    -- Season Phase (useful for context/analysis)
    CASE 
        WHEN MONTH(ds.date_day) BETWEEN 10 AND 12 THEN 'Early Season'
        WHEN MONTH(ds.date_day) BETWEEN 1 AND 2 THEN 'Mid Season'
        WHEN MONTH(ds.date_day) BETWEEN 3 AND 4 THEN 'Late Season'
        WHEN MONTH(ds.date_day) BETWEEN 5 AND 6 THEN 'Playoffs'
        WHEN MONTH(ds.date_day) BETWEEN 7 AND 9 THEN 'Off Season'
        ELSE 'Unknown'
    END AS season_phase,
    
    -- Game Day Indicators
    CASE WHEN gd.game_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_game_day,
    COALESCE(gd.games_on_date, 0) AS games_on_date,
    
    -- Day Type Flags
    CASE WHEN DAYOFWEEK(ds.date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN ds.date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
    CASE WHEN ds.date_day = DATEADD('day', -1, CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_yesterday,
    CASE WHEN ds.date_day = DATEADD('day', 1, CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_tomorrow,
    
    -- Relative Period Flags (useful for filtering)
    CASE 
        WHEN ds.date_day >= DATEADD('day', -7, CURRENT_DATE()) 
        AND ds.date_day <= CURRENT_DATE() 
        THEN TRUE ELSE FALSE 
    END AS is_last_7_days,
    
    CASE 
        WHEN ds.date_day >= DATEADD('day', -30, CURRENT_DATE()) 
        AND ds.date_day <= CURRENT_DATE() 
        THEN TRUE ELSE FALSE 
    END AS is_last_30_days,
    
    CASE 
        WHEN YEAR(ds.date_day) = YEAR(CURRENT_DATE()) 
        AND MONTH(ds.date_day) = MONTH(CURRENT_DATE())
        THEN TRUE ELSE FALSE 
    END AS is_current_month,
    
    -- Audit Fields
    CURRENT_TIMESTAMP() AS dbt_updated_at
    
FROM date_spine ds
LEFT JOIN game_dates gd ON ds.date_day = gd.game_date
WHERE ds.date_day <= DATEADD('year', 3, CURRENT_DATE())  -- Limit to 3 years in future
