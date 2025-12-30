{{
  config(
    materialized='view',
    tags=['bronze', 'raw']
  )
}}

-- Bronze layer: Raw schedule API snapshots from S3
-- No transformations - immutable source of truth
-- Contains game schedules for date ranges

SELECT
    payload,
    s3_key,
    ingest_ts
FROM {{ source('raw_nhl', 'schedule_snapshots') }}
