-- BEGIN

------------------- INPUTS
  -- BASIC FILTERS
    DECLARE VAR_entity_id STRING DEFAULT 'TB_AE'; -- @$GLOBAL_ENTITY_ID;
    DECLARE VAR_country_code STRING DEFAULT 'ae'; -- @$VAR_COUNTRY_CODE;
    DECLARE VAR_start_date DATE DEFAULT '2023-12-01'; -- @$START_DATE;
    DECLARE VAR_end_date DATE DEFAULT '2023-12-16'; -- @$END_DATE;


  -- GEO FILTERS
    DECLARE VAR_zone_id DEFAULT CAST([] AS ARRAY<INT64>); -- CAST(@$ZONE_ID AS AS ARRAY<INT64>)
    DECLARE VAR_zone_name DEFAULT CAST([] AS ARRAY<STRING>);  -- CAST(@$ZONE_NAME AS AS ARRAY<STRING>)
    DECLARE VAR_city_name DEFAULT CAST(["Dubai"] AS ARRAY<STRING>); -- values in lower case preferably CAST(@$CITY_NAME AS AS ARRAY<STRING>)

  -- DPS FILTERS
    DECLARE VAR_included_variants DEFAULT CAST(['Original', 'Control'] AS ARRAY<STRING>); -- CAST(@$INCLUDED_VARIANTS AS AS ARRAY<STRING>);
    DECLARE VAR_vendor_price_scheme_type DEFAULT CAST(['Experiment', 'Automatic scheme'] AS ARRAY<STRING>); -- CAST(@$VENDOR_PRICING_SCHEME_TYPE AS ARRAY<STRING>);
    DECLARE VAR_assignment_id DEFAULT CAST([] AS ARRAY<INT64>); -- CAST(@$ASSIGNMENT_ID AS ARRAY<INT64>;
    DECLARE VAR_scheme_id DEFAULT CAST([1510] AS ARRAY<INT64>); -- CAST(@$SCHEME_ID AS ARRAY<INT64>;


  -- Vendor filters
    DECLARE VAR_vertical_type DEFAULT CAST([] AS ARRAY<STRING>); -- CAST(@$VERTICAL_TYPE AS ARRAY<STRING>);
    DECLARE VAR_chain_id DEFAULT CAST([] AS ARRAY<STRING>); -- CAST(@$CHAIN_ID AS ARRAY<STRING>);
    DECLARE VAR_chain_name DEFAULT CAST([] AS ARRAY<STRING>); -- CAST(@$CHAIN_NAME AS ARRAY<STRING>);
    -- Tags / Marketing Tags / Key Account filters MUST BE passed as vendor_ids as those filters are not available in dps_sessions_mapped_to_orders
    DECLARE VAR_vendor_list DEFAULT CAST([] AS ARRAY<STRING>); -- CAST(@$VENDOR_IDS AS ARRAY<STRING>);

  -- time conditions
    DECLARE VAR_hour_from INT64 DEFAULT NULL; 
    DECLARE VAR_hour_to INT64 DEFAULT NULL;
    DECLARE VAR_days_of_week DEFAULT CAST([] as ARRAY<STRING>); --Must be sent in Capital letter, e.g, "Monday"  -- CAST(@$DAYS_OF_WEEK AS ARRAY<STRING>);

  -- customer area conditions
    DECLARE VAR_customer_areas DEFAULT CAST([] AS ARRAY<INT64>);


  -- input share
    DECLARE VAR_share_per_tier DEFAULT CAST([20.0, 40.0, 60.0, 80.0, 100.0] AS ARRAY<FLOAT64>); -- CAST(@$SHARE_PER_TIER AS ARRAY<FLOAT64>);
    DECLARE VAR_mov_per_tier DEFAULT CAST([20.0, 40.0, 60.0, 80.0, 100.0] AS ARRAY<FLOAT64>); -- CAST(@$MOV_PER_TIER AS ARRAY<FLOAT64>);
-------------------

------------------- QUERY
  CREATE OR REPLACE TEMP TABLE orders_for_given_vendor_list AS
  WITH 
   area_conditions AS (
    SELECT
      area_configs.country_code
      , area_configs.area_id
      , history.polygon
    FROM UNNEST(VAR_customer_areas) var_area_id
    INNER JOIN `fulfillment-dwh-production.cl.pricing_customer_area_versions` area_configs
      ON VAR_country_code = area_configs.country_code
      AND var_area_id = area_configs.area_id
    CROSS JOIN UNNEST(area_configs.customer_area_history) history
    WHERE history.active_to IS NULL -- get the active area config
  )

  SELECT
    dps.entity_id
    , dps.platform_order_code
    , dps.dps_travel_time
    , dps.dps_minimum_order_value_local
    , gfv_local
    , mov_customer_fee_local
    , ROW_NUMBER() OVER(PARTITION BY dps.entity_id ORDER BY dps.dps_travel_time ASC) / COUNT(*) OVER () * 100 AS tt_percentile -- this adds fake quartiles ties in travel time. Known limitation of this implementation. Pending replacement with PERCENT_RANK() OVER (ORDER BY o.dps_travel_time ASC) * 100 tt_percentile
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps
  LEFT JOIN area_conditions ac ON TRUE -- left join on true instead of cross join means that we retain orders even if no conditions exist
  WHERE
    -- date filters
      -- filter by utc date (dps_sessions_mapped_to_orders partition field)
      -- we use 15 days lookback to make sure we don't miss orders due to timezones
      dps.created_date BETWEEN
        IFNULL(VAR_start_date, DATE_SUB(CURRENT_DATE, INTERVAL 15 DAY))
        AND IFNULL(VAR_end_date, CURRENT_DATE) -- we perform this filter first since the dps table is partitioned by order_placed_at
      -- local date (displayed in the DPS UI)
      AND dps.created_date_local BETWEEN VAR_start_date
        AND IFNULL(VAR_end_date, CURRENT_DATE)

    --- geographic filters
      AND dps.entity_id = VAR_entity_id
      AND IF(ARRAY_LENGTH(VAR_zone_id) > 0, dps.zone_id IN UNNEST(VAR_zone_id), TRUE)
      AND IF(ARRAY_LENGTH(VAR_zone_name) > 0, dps.zone_name IN UNNEST(VAR_zone_name), TRUE)
      AND IF(ARRAY_LENGTH(VAR_city_name) > 0, dps.city_name IN UNNEST(VAR_city_name), TRUE)

    -- DPS filters
      AND IF(ARRAY_LENGTH(VAR_included_variants) > 0, dps.test_variant IN UNNEST(VAR_included_variants) OR dps.test_variant IS NULL, TRUE)
      AND IF(ARRAY_LENGTH(VAR_vendor_price_scheme_type) > 0, dps.vendor_price_scheme_type IN UNNEST(VAR_vendor_price_scheme_type), TRUE)
      AND IF(ARRAY_LENGTH(VAR_assignment_id) > 0, CAST(dps.assignment_id AS INT64) IN UNNEST(VAR_assignment_id), TRUE)
      AND IF(ARRAY_LENGTH(VAR_scheme_id) > 0, CAST(dps.scheme_id AS INT64) IN UNNEST(VAR_scheme_id), TRUE)

    -- time conditions
      AND EXTRACT(HOUR FROM order_placed_at_local) BETWEEN IFNULL(VAR_hour_from,0) AND IFNULL(VAR_hour_to,24)
      AND IF(ARRAY_LENGTH(VAR_days_of_week) > 0, FORMAT_DATE("%A", created_date_local) IN UNNEST(VAR_days_of_week), TRUE)

    -- Vendor filters
      AND IF(ARRAY_LENGTH(VAR_vertical_type) > 0, dps.vertical_type IN UNNEST(VAR_vertical_type), TRUE)
      AND IF(ARRAY_LENGTH(VAR_vendor_list) > 0, dps.vendor_id IN UNNEST(VAR_vendor_list), TRUE)
      AND IF(ARRAY_LENGTH(VAR_chain_name) > 0, dps.chain_name IN UNNEST(VAR_chain_name), TRUE)
      AND IF(ARRAY_LENGTH(VAR_chain_id) > 0, dps.chain_id IN UNNEST(VAR_chain_id), TRUE)

    -- customer location condition
      AND CASE
        WHEN ac.area_id IS NOT NULL THEN -- area_configs is never null, even when empty
          ST_COVERS(ac.polygon, dps.customer_location)
        ELSE TRUE -- if no area condition, set to TRUE
      END
    -- General filters
      -- successfully delivered orders only
        AND dps.is_sent = TRUE
      -- DBDF requires non-null travel time fees 
        AND dps_travel_time_fee_local IS NOT NULL
  ;

  WITH tiers AS ( -- allows us to join the two arrays with shares and new MOVs passed in as variables
  SELECT
    ROW_NUMBER() OVER (ORDER BY upper_tt_percentile) AS tier
    , IFNULL(LAG(upper_tt_percentile) OVER (ORDER BY upper_tt_percentile), 0) AS lower_tt_percentile
    , upper_tt_percentile
    , mov_per_tier
    FROM UNNEST(VAR_share_per_tier) upper_tt_percentile WITH OFFSET o1
    LEFT JOIN UNNEST(VAR_mov_per_tier) mov_per_tier WITH OFFSET o2 ON o1 = o2
  )
  SELECT
    tiers.tier
    , tiers.upper_tt_percentile - tiers.lower_tt_percentile AS share
    , tiers.upper_tt_percentile AS cum_share
    , MAX(o.dps_travel_time) AS dps_travel_time_decimal
    , FORMAT_TIME('%M:%S', TIME(TIMESTAMP_SECONDS(CAST(MAX(o.dps_travel_time) * 60 AS INT64)))) AS dps_travel_time_formatted
    , ROUND(AVG(o.dps_minimum_order_value_local), 2) AS current_average_mov
    , ROUND(AVG(o.gfv_local), 2) AS current_average_gfv_local
    , COUNT(o.platform_order_code) AS num_orders
    -- we assume user who paid small basket fee would continue to do so, so they're not at "risk"
    , SUM(IF(o.gfv_local < tiers.mov_per_tier AND mov_customer_fee_local = 0, 1, 0)) AS lost_orders
    , SUM(COUNT(o.platform_order_code)) OVER (ORDER BY tiers.tier) AS total_orders
  FROM tiers
  LEFT JOIN orders_for_given_vendor_list o ON o.tt_percentile > tiers.lower_tt_percentile AND o.tt_percentile <= tiers.upper_tt_percentile
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
  ;
-------------------