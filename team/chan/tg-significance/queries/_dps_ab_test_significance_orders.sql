DECLARE end_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE test_start_timestamp TIMESTAMP DEFAULT TIMESTAMP_SUB(end_timestamp, INTERVAL 120 DAY);
DECLARE lookback_period_days INT64 DEFAULT 65;

DECLARE end_date DATE DEFAULT DATE(end_timestamp);
DECLARE test_start_date_filter DATE DEFAULT DATE(test_start_timestamp);

-- DROP TABLE `dh-logistics-product-ops.pricing._tb_dps_ab_test_significance_orders`;

CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._tb_dps_ab_test_significance_orders`
-- PARTITION BY created_date
CLUSTER BY entity_id, country_code, test_name AS
WITH tests_in_scope as (
  SELECT DISTINCT
  entity_id
  , test_name
  , IF(test_end_date IS NULL, "running", "ended") as status
  FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
  -- filter either running or tests that ended less than 65 days ago
  WHERE IFNULL(test_end_date, end_timestamp) >= TIMESTAMP_SUB(end_timestamp, INTERVAL lookback_period_days DAY)
  -- filter test that started less than 7 days ago
  AND TIMESTAMP_DIFF(end_timestamp, test_start_date, DAY) > 7
  AND test_start_date >= test_start_timestamp
  -- only MENA
  -- AND (
  --   CONTAINS_SUBSTR(entity_id, "TB_")
  --   OR 
  --   entity_id = "HS_SA"
  -- )
  AND misconfigured = FALSE
)

, load_orders as (
  SELECT
    country_code
    , entity_id
    , test_name
    , target_group
    , treatment
    , variant
    , created_date
    , is_own_delivery
    , platform_order_code
    , IF(is_own_delivery, dps_delivery_fee_local, 0) AS dps_delivery_fee_local
    , IF(is_own_delivery, delivery_fee_local, 0) AS delivery_fee_local
    , gfv_local
    , travel_time
    , delivery_distance
    , mean_delay
    , delivery_costs_local
    , revenue_local
    , mov_customer_fee_local
    , service_fee_local
    , profit_local
    , commission_local

  FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` o
  WHERE created_date >= test_start_date_filter
  AND variant != "Original"
  AND variant iS NOT NULL
  AND is_sent
)

, filter_to_test_in_scope as (
  SELECT
    country_code
    , o.entity_id
    , o.test_name
    , variant
    , created_date
    , platform_order_code
    , target_group
    , treatment
    -- convert all columns to float because the python libraries we use in the significance scripts can only handle floats
    , SAFE_CAST(dps_delivery_fee_local AS FLOAT64) AS dps_delivery_fee_local
    , SAFE_CAST(delivery_fee_local AS FLOAT64) AS delivery_fee_local
    , SAFE_CAST(gfv_local AS FLOAT64) AS gfv_local
    , SAFE_CAST(travel_time AS FLOAT64) AS travel_time
    , SAFE_CAST(delivery_distance AS FLOAT64) AS delivery_distance
    , SAFE_CAST(mean_delay AS FLOAT64) AS fleet_delay
    , SAFE_CAST(delivery_costs_local AS FLOAT64) AS delivery_costs_local
    , SAFE_CAST(revenue_local AS FLOAT64) AS revenue_local
    , SAFE_CAST(profit_local AS FLOAT64) AS profit_local
    , SAFE_CAST(commission_local AS FLOAT64) AS commission_local
    , SAFE_CAST(mov_customer_fee_local AS FLOAT64) AS mov_customer_fee_local
    , SAFE_CAST(service_fee_local AS FLOAT64) AS service_fee_local
    , t.status
  FROM load_orders o
  INNER JOIN tests_in_scope t
      ON o.entity_id = t.entity_id
      AND o.test_name = t.test_name
)



############ some processing

, varians_per_test as (
  SELECT 
  entity_id
  , test_name
  , COUNT(DISTINCT variant) as n_variants_in_test
  FROM filter_to_test_in_scope
  GROUP BY 1,2
)


, add_columns as (
  SELECT f.*
    , n_variants_in_test
  FROM filter_to_test_in_scope f
  INNER JOIN varians_per_test
    USING(entity_id, test_name)
)


SELECT 
country_code
, entity_id
, test_name
, n_variants_in_test
, variant
, target_group
, treatment
, status
, ARRAY_AGG(dps_delivery_fee_local IGNORE NULLS) AS dps_delivery_fee_local
, ARRAY_AGG(delivery_fee_local IGNORE NULLS)  AS delivery_fee_local 
, ARRAY_AGG(gfv_local IGNORE NULLS)  AS gfv_local
, ARRAY_AGG(travel_time IGNORE NULLS)  AS travel_time
, ARRAY_AGG(delivery_distance IGNORE NULLS)  AS delivery_distance
, ARRAY_AGG(fleet_delay IGNORE NULLS)  AS fleet_delay
, ARRAY_AGG(delivery_costs_local IGNORE NULLS)  AS delivery_costs_local
, ARRAY_AGG(revenue_local IGNORE NULLS)  AS revenue_local
, ARRAY_AGG(profit_local IGNORE NULLS)  AS profit_local
, ARRAY_AGG(commission_local IGNORE NULLS)  AS commission_local
, ARRAY_AGG(mov_customer_fee_local IGNORE NULLS)  AS mov_customer_fee_local
, ARRAY_AGG(service_fee_local IGNORE NULLS) AS service_fee_local
FROM add_columns
GROUP BY 1,2,3,4,5,6,7,8;