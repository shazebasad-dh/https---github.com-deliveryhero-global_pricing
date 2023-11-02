-----------------------------------------------------------------------------------------------------------------------------------------------
--                NAME: cpl_perseus_match_scorecard.sql
--      INITIAL AUTHOR: Tanmoy Porel
--       CREATION DATE: 2023-09-27
--         DESCRIPTION: This script is used to create report for Perseus matching with CPL logs
--        QUERY OUTPUT: 
--               NOTES:
--             UPDATED: 2023-08-01 | Tanmoy Porel        | CLOGBI-1120  |  Creation
--             UPDATED: 2023-08-09 | Tanmoy Porel        | RASD-4668    |  Bugfix Client Frontend mismatch
--             UPDATED: 2023-08-09 | Tanmoy Porel        | CLOGBI-1141  |  Fix backfill
--             UPDATED: 2023-10-20 | Sebastian Lafaurie  |              |  Add user_agent and unique counts.
-----------------------------------------------------------------------------------------------------------------------------------------------

DECLARE next_ds DATE DEFAULT CURRENT_DATE();
DECLARE stream_look_back_days INT64 DEFAULT 3;

WITH frontend_sessions AS (
  SELECT
    global_entity_id
    , partition_date
    , session_id
    , client_id
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_tracking.perseus_sessions`
  -- {%- if not params.backfill %}
  -- WHERE partition_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY) AND '{{ next_ds }}'
  -- {%- elif params.is_backfill_chunks_enabled %}
  -- WHERE partition_date BETWEEN '{{ params.backfill_start_date }}' AND '{{ params.backfill_end_date }}'
  -- {%- endif %}
  WHERE partition_date BETWEEN DATE_SUB(next_ds, INTERVAL stream_look_back_days DAY) AND next_ds

), entities AS (
  SELECT
    e.country_name
    , p.entity_id
    , p.brand_name AS platform
  -- FROM `{{ params.project_id }}.cl.entities` e
  FROM `fulfillment-dwh-production.cl.entities` e
  LEFT JOIN UNNEST(platforms) p
-- AVAILABILITY SESSIONS --
), ava_logs AS (
  SELECT
    'AVA' AS customer_service
    , request_id
    , created_date
    , endpoint
    , entity_id
    , session.id AS session_id
    , client.id AS client_id
    ,'NULL' as user_agent
  -- FROM `{{ params.project_id }}.cl.choice_availability_sessions`
  -- {%- if not params.backfill %}
  -- WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY) AND '{{ next_ds }}'
  -- {%- elif params.is_backfill_chunks_enabled %}
  -- WHERE created_date BETWEEN '{{ params.backfill_start_date }}' AND '{{ params.backfill_end_date }}'
  -- {%- endif %}
  FROM `fulfillment-dwh-production.cl.choice_availability_sessions`
  WHERE created_date BETWEEN DATE_SUB(next_ds, INTERVAL stream_look_back_days DAY) AND next_ds
  GROUP BY 1, 2, 3, 4, 5, 6, 7
-- DPS SESSIONS --
), dps_logs AS (
  SELECT
    'DPS' AS customer_service
    , request_id
    , created_date
    , CASE endpoint
      WHEN 'singleFee' THEN 'single'
      WHEN 'multipleFee' THEN 'multi'
      ELSE endpoint
    END AS endpoint
    , entity_id
    , dps_session_id AS session_id
    , perseus_client_id AS client_id
    /*
    meta_info has two columns but user_agent is more complete.
    */
    , IFNULL(meta_info.user_agent, 'NULL') as user_agent
  -- FROM `{{ params.project_id }}.cl.dynamic_pricing_user_sessions_v2`
  -- {%- if not params.backfill %}
  -- WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY) AND '{{ next_ds }}'
  -- {%- elif params.is_backfill_chunks_enabled %}
  -- WHERE created_date BETWEEN '{{ params.backfill_start_date }}' AND '{{ params.backfill_end_date }}'
  -- {%- endif %}
  FROM `fulfillment-dwh-production.cl.dynamic_pricing_user_sessions_v2`
  WHERE created_date BETWEEN DATE_SUB(next_ds, INTERVAL stream_look_back_days DAY) AND next_ds
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
-- TES USER SESSIONS --
), tes_logs AS (
  SELECT
    'TES' AS customer_service
    , x_request_id AS request_id
    , created_date
    , endpoint -- NULL endpoints also present in TES
    , entity_id
    , customer.session_id AS session_id
    , customer.perseus_client_id AS client_id
    , 'NULL' as user_agent
  -- FROM `{{ params.project_id }}.cl.tes_user_sessions`
  -- {%- if not params.backfill %}
  -- WHERE created_date BETWEEN DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY) AND '{{ next_ds }}'
  -- {%- elif params.is_backfill_chunks_enabled %}
  -- WHERE created_date BETWEEN '{{ params.backfill_start_date }}' AND '{{ params.backfill_end_date }}'
  -- {%- endif %}
  FROM `fulfillment-dwh-production.cl.tes_user_sessions`
  WHERE created_date BETWEEN DATE_SUB(next_ds, INTERVAL stream_look_back_days DAY) AND next_ds
  GROUP BY 1, 2, 3, 4, 5, 6, 7
)
, all_logs AS (
  SELECT * FROM ava_logs
  UNION ALL
  SELECT * FROM dps_logs
  UNION ALL
  SELECT * FROM tes_logs
)
SELECT
  l.customer_service
  , CASE e.platform
    WHEN 'Yemeksepeti' THEN 'pandora'
    WHEN 'Damejidlo' THEN 'pandora'
    WHEN 'Foodpanda' THEN 'pandora'
    WHEN 'Mjam' THEN 'pandora'
    WHEN 'Foodora' THEN 'pandora'
    WHEN 'PedidosYa' THEN 'peya'
    WHEN 'OnDemandRider' THEN 'odr'
    ELSE LOWER(e.platform)
  END AS platform
  , e.country_name
  , e.platform AS platform_detailed
  , l.entity_id
  , l.endpoint
  , l.created_date
  , user_agent
  , COUNT(*) AS request_count
  --session ids
  , COUNTIF(l.session_id IN UNNEST(ARRAY(SELECT id FROM `fulfillment-dwh-production.cl._bad_dps_logs_ids`))) AS bad_session_ids
  , COUNTIF(l.session_id IS NULL) AS null_session_ids
  , COUNTIF(NOT(REGEXP_CONTAINS(l.session_id, r'[0-9-]+.\.[0-9-]+.\.[a-zA-Z0-9-]+.'))) AS requests_session_non_regex
  , COUNTIF(f.session_id IS NULL) AS no_frontend_session_matched
  , COUNT(DISTINCT l.session_id) as unique_sessions_id
  , COUNT(DISTINCT CASE WHEN f.session_id IS NULL THEN l.session_id END) as unique_no_frontend_sessions_matched
  --client ids
  , COUNTIF(l.client_id IN UNNEST(ARRAY(SELECT id FROM `fulfillment-dwh-production.cl._bad_dps_logs_ids`))) AS bad_client_ids
  , COUNTIF(l.client_id IS NULL) AS null_client_ids
  , COUNTIF(NOT(REGEXP_CONTAINS(l.client_id, r'[0-9-]+.\.[0-9-]+.\.[a-zA-Z0-9-]+.'))) AS requests_client_non_regex
  , COUNTIF(f.client_id != l.client_id OR f.client_id IS NULL) AS no_frontend_client_matched
  , COUNT(DISTINCT l.client_id) as unique_client_id
  , COUNT(DISTINCT CASE WHEN f.client_id != l.client_id OR f.client_id IS NULL THEN l.client_id END) as unique_no_frontend_client_matched
FROM all_logs l
LEFT JOIN frontend_sessions f ON l.session_id = f.session_id AND l.entity_id = f.global_entity_id AND l.created_date = f.partition_date
LEFT JOIN entities e ON l.entity_id = e.entity_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8