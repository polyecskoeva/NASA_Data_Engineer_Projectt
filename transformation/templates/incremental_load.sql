{{

  config(

    materialized='incremental',

    unique_key='TECH_KEY_HASH',

    incremental_strategy='merge',

    merge_exclude_columns=["CREATED_TS", "CREATED_BY"],

    on_schema_change='append_new_columns',

    tags=["<source_system>", "<layer>_<source_system>"]

  )

}}

 

{% set var_key_col_list = ['<PRIMARY_KEY1>', '<PRIMARY_KEY2>'] %}

 

{% set var_data_col_list = [

    '<BUSINESS_COL_1>',

    '<BUSINESS_COL_2>',

    '<BUSINESS_COL_3>',

    'FILE_NAME'

] %}

 

{#

  TEMPLATE: Hash-Based Incremental Merge with Data Change Detection

 

  PURPOSE:

  Advanced incremental template that combines:

  - Hash-based change detection (DATA_HASH) to avoid updating unchanged records

  - Optional, but STRONGLY recommended (align with BA): date/partition-based pre-filtering when source has timestamp columns

  - MODIFIED_TS only updates on actual data changes (not on every run)

  - Idempotent, reprocessable runs for Airflow orchestration

 

 

 

  KEY FEATURES:

  - Only INSERTs new records and UPDATEs actually changed records

  - CREATED_TS/CREATED_BY are set on INSERT only (excluded from merge via config)

  - MODIFIED_TS/MODIFIED_BY only update when DATA_HASH actually changes

  - Safe for first run / full-refresh: target CTE wrapped in {% if is_incremental() %}

  - DATA_HASH is excluded from the final SELECT (internal use only)

 

 

  COLUMN GROUPS:

 

  1. BUSINESS COLUMNS:

     Listed explicitly in source_data

 

  2. HASH COLUMNS (generated in final CTE):

     - TECH_KEY_HASH (string)    — surrogate key from var_key_col_list, used as unique_key for merge

     - DATA_HASH (string)        — hash from var_data_col_list, used for change detection only,

                                   excluded from final SELECT

 

  3. METADATA COLUMNS (generated in final CTE):

     - DBT_LOADED_AT (timestamp_ltz)      — CURRENT_TIMESTAMP(), when dbt processed the row

     - DATA_INTERVAL_START (string)       — '{{ var("data_interval_start") }}', Airflow DAG interval start

     - DATA_INTERVAL_END (string)         — '{{ var("data_interval_end") }}', Airflow DAG interval end

 

  4. TECHNICAL COLUMNS (generated in final CTE):

     - JOB_ID (string)                    — '{{ var("job_id", "manual_run") }}', Airflow DAG run ID

     - CREATED_TS (timestamp_ltz)         — CURRENT_TIMESTAMP(), set on INSERT only

                                            (merge_exclude_columns prevents overwrite on UPDATE)

     - CREATED_BY (string)                — '{{ var("airflow_dag_id", "dbt") }}', set on INSERT only

     - MODIFIED_TS (timestamp_ltz)        — Controlled by CASE in incremental_logic:

                                            New row → created_ts

                                            Changed row (DATA_HASH differs) → CURRENT_TIMESTAMP()

                                            Unchanged row → target.modified_ts (filtered out anyway)

     - MODIFIED_BY (string)               — Same CASE logic as MODIFIED_TS

 

#}

 

WITH source_data AS (

    SELECT

        <PRIMARY_KEY>,

        <BUSINESS_COL_1>,

        <BUSINESS_COL_2>,

        <BUSINESS_COL_3>,

        FILE_NAME

    FROM {{ ref('<SOURCE_MODEL>') }}

 

    -- Optional but STRONGLY RECOMMENDED: Date-based pre-filtering to limit source scan (performance optimization).

    -- Determine a date column with the BA which you can use in filtering

    --

      {% if is_incremental() %}

      WHERE (

          TO_DATE(MODIFIED_TS) >= '{{ var("data_interval_start") }}'::DATE

          AND TO_DATE(MODIFIED_TS) < '{{ var("data_interval_end") }}'::DATE

       )

      OR (

          TO_DATE(CREATED_TS) >= '{{ var("data_interval_start") }}'::DATE

          AND TO_DATE(CREATED_TS) < '{{ var("data_interval_end") }}'::DATE

       )

      {% endif %}

 

),

 

final AS (

    SELECT

        src.*,

 

        -- HASH COLUMNS

        {{ dbt_utils.generate_surrogate_key(var_key_col_list) }}  AS tech_key_hash,

        {{ dbt_utils.generate_surrogate_key(var_data_col_list) }} AS data_hash,

 

        -- METADATA COLUMNS

        CURRENT_TIMESTAMP()                          AS dbt_loaded_at,

        '{{ var("data_interval_start") }}'           AS data_interval_start,

        '{{ var("data_interval_end") }}'             AS data_interval_end,

 

        -- TECHNICAL COLUMNS

        '{{ var("job_id", "manual_run") }}'          AS job_id,

        CURRENT_TIMESTAMP()                          AS created_ts,

        '{{ var("airflow_dag_id", "dbt") }}'         AS created_by,

        CURRENT_TIMESTAMP()                          AS modified_ts,

        '{{ var("airflow_dag_id", "dbt") }}'         AS modified_by

 

    FROM source_data AS src

),

 

{% if is_incremental() %}

target AS (

    SELECT

        *,

        {{ dbt_utils.generate_surrogate_key(var_data_col_list) }} AS data_hash

    FROM {{ this }}

),

{% endif %}

 

incremental_logic AS (

    SELECT

        f.* EXCLUDE (modified_ts, modified_by),

 

        {% if is_incremental() %}

            CASE

                WHEN target.tech_key_hash IS NULL     THEN f.created_ts

                WHEN target.data_hash != f.data_hash  THEN f.modified_ts

                ELSE target.modified_ts

            END AS modified_ts,

            CASE

                WHEN target.tech_key_hash IS NULL     THEN f.created_by

                WHEN target.data_hash != f.data_hash  THEN f.modified_by

                ELSE target.modified_by

            END AS modified_by

        {% else %}

            f.created_ts AS modified_ts,

            f.created_by AS modified_by

        {% endif %}

 

    FROM final AS f

 

    {% if is_incremental() %}

        LEFT JOIN target

            ON f.tech_key_hash = target.tech_key_hash

        WHERE

            target.tech_key_hash IS NULL       -- new records (INSERT)

            OR f.data_hash != target.data_hash -- changed records (UPDATE)

    {% endif %}

)

 

SELECT * EXCLUDE (data_hash)

FROM incremental_logic

 

{#

  HOW THIS WORKS:

  1. source_data: Reads from source

     Optional date filter on CREATED_TS/MODIFIED_TS scopes records to time window

     (only when source has timestamp columns, e.g., PSA → DWH).

  2. final: Adds TECH_KEY_HASH (from business key) and DATA_HASH (from all business columns),

     plus metadata and technical columns.

  3. target (incremental only): Reads existing table and recalculates DATA_HASH

     from current stored values.

  4. incremental_logic: LEFT JOINs final ↔ target on TECH_KEY_HASH.

     - Filters to only NEW rows (no match in target) or CHANGED rows (DATA_HASH differs).

     - CASE logic sets MODIFIED_TS: new → created_ts, changed → now, unchanged → skipped.

  5. MERGE operation (dbt incremental strategy):

     - Matches on TECH_KEY_HASH (unique_key).

     - INSERTs new rows, UPDATEs changed rows (excluding CREATED_TS/CREATED_BY).

     - Unchanged rows never reach the MERGE (filtered out in step 4).

  6. DATA_HASH is excluded from the final SELECT (internal use only).

 

  PERFORMANCE:

  - Optional date filter: Limits source scan to relevant time window.

  - DATA_HASH comparison: Skips unchanged rows before MERGE → reduces writes.

  - TECH_KEY_HASH: Surrogate key for efficient merge matching.

#}

...

[Message clipped]  View entire message
