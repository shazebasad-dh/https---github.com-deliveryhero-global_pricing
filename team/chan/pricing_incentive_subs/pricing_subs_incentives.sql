########################################## INPUTS

########## DECLARE VARIABLES

DECLARE from_date_filter, to_date_filter DATE;
DECLARE backfill BOOL;


########## SET RUN MODE
SET backfill = TRUE;

# SET END DATE
SET to_date_filter = CURRENT_DATE();

# SET PARTITION DATE
IF backfill THEN
SET from_date_filter = DATE_SUB("2024-01-01", interval 0 DAY);
ELSE
SET from_date_filter = DATE_TRUNC(DATE_SUB(to_date_filter, interval 1 MONTH), MONTH);
END IF;
##########################################

##########################################

CREATE TEMP FUNCTION GENERATE_DPS_INCENTIVE_ID(assignment_type STRING, assignment_id NUMERIC)
RETURNS STRING
AS (
CONCAT("dynamic-pricing:", ARRAY_TO_STRING(SPLIT(LOWER(assignment_type), " "), "-"), ":", CAST(assignment_id AS STRING))
);
##########################################

-- CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.pricing_subs_incentives_data`
-- AS
WITH load_dps_data AS (

  SELECT
    entity_id
    , ge.management_entity
    , ge.brand_name
    , platform_order_code
    , created_date_local
    , DATE_TRUNC(created_date_local, month) AS month
    , order_placed_at_local
    , dps_travel_time_fee_local
    , dps_surge_fee_local
    --- basket value discount are not discount except for subs. I want this column to reflect non-discount basket value discounts. 
    , IF(has_subscription_discount, 0, dps_basket_value_fee_local) AS dps_basket_value_fee_local
    , IF(has_subscription_discount, dps_delivery_fee_local + ABS(dps_basket_value_fee_local), dps_delivery_fee_local) as base_delivery_fee
    , delivery_fee_local
    , exchange_rate
    , vertical_type
    , has_subscription
    , assignment_id
    , partial_assignments.subscription_id
    , scheme_id
    , dps_session_id
    , ARRAY_AGG(
      STRUCT(
      CASE
        WHEN has_subscription_discount THEN GENERATE_DPS_INCENTIVE_ID("subscription", assignment_id)
        WHEN vendor_price_scheme_type = "Campaign" THEN GENERATE_DPS_INCENTIVE_ID(vendor_price_scheme_type, assignment_id)
        WHEN has_new_customer_condition THEN GENERATE_DPS_INCENTIVE_ID(vendor_price_scheme_type, assignment_id)
        ELSE NULL
      END AS customer_incentive_id

      , "discount" AS incentive_level_category
      , "delivery_fee" AS incentive_level_type
      , "DPS" as channel_subchannel
      , IF(has_subscription_discount, TRUE, FALSE) as is_membership_only

      , CASE
        WHEN has_subscription_discount THEN ABS(dps_basket_value_fee_local)
        ELSE ABS(dps_incentive_discount_local)
      END AS total_incentives_local
      -- assume all is on DH until solution is found
      , CASE
        WHEN has_subscription_discount THEN ABS(dps_basket_value_fee_local)
        ELSE ABS(dps_incentive_discount_local)
      END AS dh_contribution_local

      , 0 as vendor_contribution_local
      )
    ) as dps_incentive


  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps
  LEFT JOIN `fulfillment-dwh-production.curated_data_shared_coredata.global_entities` ge
    ON dps.entity_id = ge.global_entity_id
  WHERE created_date BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    AND is_sent
    AND is_own_delivery
    AND dps_delivery_fee_local is not null
    AND created_date_local BETWEEN from_date_filter AND to_date_filter
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

, load_bima_incentives AS (
  SELECT
    order_id
    , global_entity_id
    , ARRAY(
        SELECT
          STRUCT(x.customer_incentive_id
          , x.incentive_level_category
          , x.incentive_level_type
          , x.channel_subchannel
          , x.is_membership_only
          , x.total_incentives_lc
          , x.dh_contribution_lc 
          , x.vendor_contribution_lc
          )
        FROM UNNEST(incentive_spend) x
        WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
          AND (
            LOWER(x.incentive_level_type) LIKE "%delivery%"
            OR ( x.deliveryfee_inc_lc > 0 OR  x.total_incentives_lc > 0)
          )
    ) AS bima_incentive_spend
    
    , EXISTS(
        SELECT x
        FROM UNNEST(incentive_spend) x
        WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
    ) AS bima_has_dps_incentive

  FROM `fulfillment-dwh-production.curated_data_shared_mkt.bima_incentives_reporting`
  WHERE created_date BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
  --- only delivery fee incentives
  AND EXISTS(
    SELECT x FROM UNNEST(incentive_spend) x
    WHERE LOWER(x.customer_incentive_id) LIKE "%dynamic-pricing%"
      AND (
        LOWER(x.incentive_level_type) LIKE "%delivery%"
        OR ( x.deliveryfee_inc_lc > 0 OR  x.total_incentives_lc > 0)
      )
  )
)

, load_subs_data AS (
  SELECT
    global_entity_id
    , order_id
    , ARRAY(
      SELECT
        STRUCT(
            x.customer_incentive_id
            , NULL as incentive_level_category
            , "delivery_fee" as incentive_level_type 
            , NULL AS channel_subchannel
            , TRUE AS is_membership_only
            , x.total_value_local
            , x.dh_funded_value_local
            , x.vendor_funded_value_local
        )
      FROM UNNEST(benefits) x
      WHERE LOWER(x.category) LIKE "%delivery%"
    ) as subs_benefits
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.subscription_benefit_transactions`
  WHERE DATE(transaction_timestamp_local) BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
)


############# JOIN ALL

, join_all AS (
SELECT
dps.* EXCEPT(has_subscription)
, bima.* EXCEPT(global_entity_id, order_id, bima_has_dps_incentive)
, IFNULL(bima_has_dps_incentive, FALSE) as bima_has_dps_incentive
, subs.* EXCEPT(global_entity_id, order_id)
, GREATEST(has_subscription, subs.order_id IS NOT NULL) AS has_subscription
, CASE
    WHEN ARRAY_LENGTH(dps_incentive) > 0  THEN TRUE
    WHEN bima.order_id IS NOT NULL THEN TRUE
    WHEN ARRAY_LENGTH(subs_benefits) > 0 THEN TRUE
    ELSE FALSE
  END AS has_delivery_fee_incentive

FROM load_dps_data dps

LEFT JOIN load_bima_incentives bima
  ON dps.entity_id = bima.global_entity_id
  AND dps.platform_order_code = bima.order_id

LEFT JOIN load_subs_data subs
  ON dps.entity_id = subs.global_entity_id
  AND dps.platform_order_code = subs.order_id
)

-- , dps_incentives_status AS (
-- SELECT
-- entity_id
-- , management_entity
-- , dps_incentive_reason
-- , COUNT(IF(dps_incentive_discount_local IS NOT NULL, platform_order_code, NULL)) as orders_with_dps_incentive_discount_local
-- , COUNT(IF(dps_standard_fee_local IS NOT NULL, platform_order_code, NULL)) as orders_with_dps_standard_fee_local
-- , COUNT(IF(dps_basket_value_fee_local IS NOT NULL, platform_order_code, NULL)) as orders_with_basket_value_discount
-- , COUNT(platform_order_code) as n_orders
-- , COUNT(IF(dps_incentive_amount IS NOT NULL, platform_order_code, NULL)) as orders_with_incentive_amount
-- FROM load_dps_data
-- WHERE has_dps_incentive
-- GROUP BY 1,2,3
-- )

-- , aggregate_incentives_data_q4 AS (
-- SELECT
-- management_entity
-- , entity_id
-- , has_subscription
-- , has_delivery_fee_incentive
-- , has_dps_incentive
-- , COUNT(platform_order_code) as n_orders
-- FROM join_all
-- WHERE created_date_local BETWEEN "2023-10-01" AND "2023-12-31"
-- GROUP BY 1,2,3,4,5
-- )

SELECT *
FROM join_all
WHERE TRUE 
AND platform_order_code = "q4bf-w10a"
AND entity_id = "FP_TW"
LIMIT 100