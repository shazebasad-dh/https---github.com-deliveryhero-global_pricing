########## DECLARE VARIABLES

DECLARE start_date_filter, end_date_filter DATE;
DECLARE backfill BOOL;
DECLARE include_countries ARRAY<STRING>;


########## SET RUN MODE
SET backfill = TRUE;

# SET END DATE 
SET end_date_filter = CURRENT_DATE();


# SET PARTITION DATE
IF backfill THEN 
    SET start_date_filter = DATE_SUB(CURRENT_DATE(), interval 7 DAY); 
ELSE
    SET start_date_filter = DATE_SUB(end_date_filter, interval 7 DAY);
END IF; 

# PRINT INPUTS
SELECT
 backfill as backfill
, start_date_filter as from_date
, end_date_filter as to_date
;



CREATE OR REPLACE TEMP TABLE staging_table AS
WITH load_session_data AS (
  SELECT
      created_date
    , country
    , country_code
    , location_based_country_code
    , entity_id
    , platform
    , brand
    , sessions.customer_status
    , sessions.perseus_client_id
    , sessions.location
    , TIMESTAMP_SECONDS(CAST(REGEXP_EXTRACT(events_ga_session_id, r'[^.]*$') AS INT64)) AS ga_session_start_at
    , (SELECT ANY_VALUE(dps_zone.timezone) FROM UNNEST(dps_zone) dps_zone) AS dps_timezone
    , ARRAY_TO_STRING(ARRAY((SELECT DISTINCT CAST(dps_zone.id AS STRING) FROM UNNEST(dps_zone) dps_zone ORDER BY 1)), ', ') AS dps_zones_id
    , ARRAY_TO_STRING(ARRAY((SELECT DISTINCT dps_zone.name FROM UNNEST(dps_zone) dps_zone ORDER BY dps_zone.name)), ', ') AS dps_zones_name
    -- Added City data
    , ARRAY_TO_STRING(ARRAY((SELECT DISTINCT CAST(dps_zone.city_id AS STRING) FROM UNNEST(dps_zone) dps_zone ORDER BY 1)), ', ') AS dps_city_id
    , ARRAY_TO_STRING(ARRAY((SELECT DISTINCT dps_zone.city_name FROM UNNEST(dps_zone) dps_zone ORDER BY dps_zone.city_name)), ', ') AS dps_city_name
    -- DPS variant for A/B/n tests
    , sessions.variant
    , sessions.experiment_id
    , sessions.is_parallel
    , events_ga_session_id AS ga_session_id
    , ga_dps_session_id AS dps_session_id
    , events

  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_ga_sessions` dps
  WHERE created_date BETWEEN start_date_filter AND end_date_filter
    AND entity_id LIKE "TB_%" OR entity_id LIKE "%EG"
    AND ga_dps_session_id IS NOT NULL
    AND sessions.experiment_id > 0
    AND sessions.perseus_client_id IS NOT NULL
    -- there are nulls as string
    AND ga_dps_session_id != 'null'
    AND sessions.perseus_client_id != 'null'
)

, session_data AS (
  SELECT
    created_date
    , country
    , country_code
    , location_based_country_code
    , entity_id
    , platform
    , brand
    , customer_status
    , perseus_client_id
    , location
    , ga_session_start_at
    , dps_timezone
    , dps_zones_id
    , dps_zones_name
    -- Added City data
    , dps_city_id
    , dps_city_name
    -- DPS variant for A/B/n tests
    , variant
    , experiment_id
    , is_parallel
    , ga_session_id
    , dps_session_id
    , IFNULL(vendor_code, "NULL") as vendor_code
    , vendor_group_id
    , vertical_parent_in_test
    , vendor_group_id_in_test
    , IF(vertical_parent_in_test = TRUE
      AND has_price_variant = TRUE
      AND vendor_price_scheme_type = 'Experiment'
      AND vendor_group_id_in_test = TRUE  -- Older logs dont have vendor_group_id so we keep the old matching logic
      , TRUE, FALSE) AS is_in_treatment
    -- For each ga_dps_session_id with events array, flatten the array to new columns
    , MAX(IF(event_action = 'shop_list.loaded', ga_session_id, NULL)) AS shop_list_session -- vendor_code is always null for multifee events, so it will not be grouped with the events below, even though it should. This will be fixed in the next CTE.
    , MAX(IF(event_action = 'shop_details.loaded', ga_session_id, NULL)) AS shop_menu_session
    , MAX(IF(event_action = 'checkout.loaded', ga_session_id, NULL)) AS checkout_session
    , MAX(IF(event_action = 'transaction', ga_session_id, NULL)) AS transaction_session
  FROM load_session_data dps
  LEFT JOIN UNNEST(events) e
  -- LEFT JOIN UNNEST(ab_tests) ab
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
-- vendor_code is always null for multifee events, so shop_list_session will not be grouped with the singlefee events, even though it should.
-- the following CTEs fix it: first separate the multifee and singlefee CTEs,
-- then take the shop_list_session value from the multifee CTE and the rest from the singlefee
)

, singlefee_sessions AS (
  SELECT * FROM session_data 
  WHERE shop_menu_session IS NOT NULL OR checkout_session IS NOT NULL OR transaction_session IS NOT NULL
)

, multifee_sessions AS (
  SELECT * FROM session_data 
  WHERE shop_menu_session IS NULL AND checkout_session IS NULL AND transaction_session IS NULL
)

, session_data_fixed_singlefee AS (
  SELECT single.* REPLACE (multi.shop_list_session AS shop_list_session)
  FROM singlefee_sessions single
  LEFT JOIN multifee_sessions multi ON single.entity_id = multi.entity_id
    AND single.ga_session_id = multi.ga_session_id
    AND single.dps_session_id = multi.dps_session_id
    AND single.experiment_id = multi.experiment_id
    AND single.variant = multi.variant
    AND (single.vendor_group_id = multi.vendor_group_id OR (single.vendor_group_id IS NULL AND multi.vendor_group_id IS NULL))
), sessions_data_clean AS (
  -- the former CTE fixed the singlefee grouping. The following CTE will union the result with the multifee CTE. the multifee CTE is still needed
  -- since we want the multifee events to be taken into account on an experiment level, regardless of the later singlefee events that are vendor specific.
  SELECT * FROM session_data_fixed_singlefee

  UNION ALL

  SELECT * FROM multifee_sessions
)

, load_final_data as (
SELECT DISTINCT
  created_date
  , CONCAT(ga_session_id, dps_session_id, entity_id, vendor_code) as _primary_key
  , country
  , country_code
  , location_based_country_code
  , entity_id
  , platform
  , brand
  , dps_session_id
  , ga_session_id
  , ga_session_start_at AS created_at
  , DATE(ga_session_start_at, dps_timezone) AS created_date_local
  , DATETIME(ga_session_start_at, dps_timezone) AS created_at_local
  , dps_zones_id
  , dps_zones_name
  , dps_city_id
  , dps_city_name
  , customer_status
  , perseus_client_id
  , variant
  , experiment_id
  , is_parallel
  -- , ab_test_variant
  , vendor_group_id
  , vendor_code
  , vertical_parent_in_test
  , vendor_group_id_in_test
  , is_in_treatment
  , IF(shop_list_session IS NOT NULL, ga_session_id, NULL) AS shop_list_no
  , IF(shop_list_session IS NOT NULL AND shop_menu_session IS NOT NULL, ga_session_id, NULL) AS list_menu
  , IF(shop_menu_session IS NOT NULL, ga_session_id, NULL) AS shop_menu_no
  , IF(checkout_session IS NOT NULL AND shop_menu_session IS NOT NULL, ga_session_id, NULL) AS menu_checkout
  , IF(checkout_session IS NOT NULL, ga_session_id, NULL) AS checkout_no
  , IF(transaction_session IS NOT NULL AND checkout_session IS NOT NULL, ga_session_id, NULL) AS checkout_transaction
  , IF(transaction_session IS NOT NULL, ga_session_id, NULL) AS transaction_no
FROM sessions_data_clean
)

select * from load_final_data;




###### UPSERT
IF backfill THEN 
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_cvr_events_treatment` 
  PARTITION BY created_date
  CLUSTER BY entity_id
  AS 
  SELECT * FROM staging_table;
ELSE
  MERGE INTO `dh-logistics-product-ops.pricing.dps_cvr_events_treatment` prd
  USING staging_table stg
    ON prd._primary_key = stg._primary_key
  WHEN MATCHED THEN
    UPDATE SET
        created_date = stg.created_date_local
        , _primary_key = stg._primary_key
        , country_code = stg.country_code
        , location_based_country_code = stg.location_based_country_code
        , entity_id = stg.entity_id
        , platform = stg.platform
        , brand = stg.brand
        , dps_session_id = stg.dps_session_id
        , ga_session_id = stg.ga_session_id
        , created_at = stg.created_at
        , created_date_local = stg.created_date_local
        , created_at_local = stg.created_at_local
        , dps_zones_id = stg.dps_zones_id
        , dps_zones_name = stg.dps_zones_name
        , dps_city_id = stg.dps_city_id
        , dps_city_name = stg.dps_city_name
        , customer_status = stg.customer_status
        , perseus_client_id = stg.perseus_client_id
        , variant = stg.variant
        , experiment_id = stg.experiment_id
        , is_parallel = stg.is_parallel
        -- , ab_test_variant = stg.ab_test_variant
        , vendor_group_id = stg.vendor_group_id
        , vendor_code = stg.vendor_code
        , vertical_parent_in_test = stg.vertical_parent_in_test
        , vendor_group_id_in_test = stg.vendor_group_id_in_test
        , is_in_treatment = stg.is_in_treatment
        , shop_list_no = stg.shop_list_no
        , list_menu = stg.list_menu
        , shop_menu_no = stg.shop_menu_no
        , menu_checkout = stg.menu_checkout
        , checkout_no = stg.checkout_no
        , checkout_transaction = stg.checkout_transaction
        , transaction_no = stg.transaction_no

  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
end if;