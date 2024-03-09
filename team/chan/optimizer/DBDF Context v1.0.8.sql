BEGIN
DECLARE VAR_entity_id STRING DEFAULT 'FP_SG'; -- @$GLOBAL_ENTITY_ID;
DECLARE VAR_country_code STRING DEFAULT 'sg';
DECLARE VAR_start_date DATE DEFAULT '2023-11-15'; -- @$START_DATE;
DECLARE VAR_end_date DATE DEFAULT '2023-11-30'; -- @$END_DATE;
DECLARE VAR_included_variants DEFAULT CAST(['Original', 'Control'] AS ARRAY<STRING>); -- CAST(@$INCLUDED_VARIANTS AS AS ARRAY<STRING>);
DECLARE VAR_vendor_price_scheme_type DEFAULT CAST(['Experiment', 'Automatic scheme', 'Manual'] AS ARRAY<STRING>); -- CAST(@$VENDOR_PRICING_SCHEME_TYPE AS ARRAY<STRING>);
-- If getting context by ASA:
DECLARE VAR_assignment_id INT64 DEFAULT 946; -- @$ASSIGNMENT_ID;
DECLARE VAR_vgpc_id INT64 DEFAULT 15335; -- optional, if we context for a specific conditional scheme in an ASA
-- If getting context by vendor list:
DECLARE VAR_vendor_list DEFAULT CAST(['z1bt', 'm5pa', 'gvoj', 's4uq'] AS ARRAY<STRING>); -- CAST(@$VENDOR_IDS AS ARRAY<STRING>);
DECLARE VAR_share_per_tier DEFAULT CAST([20.0, 40.0, 60.0, 80.0, 100.0] AS ARRAY<FLOAT64>); -- CAST(@$SHARE_PER_TIER AS ARRAY<FLOAT64>);
DECLARE VAR_customer_condition_config DEFAULT STRUCT( -- optional, allows us to filter orders by customer condition for the vendor list
  CAST(NULL AS STRING) AS type
  , CAST(NULL AS BOOL) AS value
  , CAST(NULL AS INT64) AS orders_number_less_than
  , CAST(NULL AS INT64) AS days_since_first_order_less_than
  , CAST(NULL AS STRING) AS counting_method
);
DECLARE VAR_schedule_config DEFAULT STRUCT( -- optional, allows us to filter orders by time condition for the vendor list
  CAST(NULL AS TIMESTAMP) AS start_at
  , CAST(NULL AS TIMESTAMP) AS end_at
  , CAST(NULL AS ARRAY<STRING>) AS active_days
  , CAST(NULL AS TIMESTAMP) AS recurrence_end_at
  , CAST(NULL AS STRING) AS recurrence
  , CAST(NULL AS BOOL) AS is_all_day
  , CAST(NULL AS STRING) AS timezone
);
DECLARE VAR_customer_areas DEFAULT CAST([] AS ARRAY<INT64>);

CREATE TEMP FUNCTION CHECK_CUSTOMER_CONDITION (
  vertical_type STRING
  , is_default_scheme BOOL
  , customer_condition_config STRUCT<
    description STRING
    , orders_number_less_than INT64
    , days_since_first_order_less_than INT64
    , counting_method STRING
  >
  , order_count_total NUMERIC
  , order_count_vertical NUMERIC
  , order_count_qcommerce NUMERIC
  , days_since_first_order FLOAT64
  , days_since_first_vertical_order FLOAT64
  , days_since_first_qcommerce_order FLOAT64
) RETURNS BOOL AS (
  -- just because the order count or first order date doens't match between DPS and the other IDs, it *doesn't* mean we accidentally granted free delivery.
  -- With multiple FDNC, the first 10 deliveries might be free for example. In this case, if we flag an order as order number 5 when it was in fact number 6, we didn't do anything wrong by granting FDNC. It's only at order number 10 that it's a problem.
  -- Therefore, we compare the order counts and first order dates reported by each of the identifiers to the maximum order count and max first order date in the ASA or Campaign configuration.
  CASE
    WHEN
      is_default_scheme IS TRUE 
      OR (
        customer_condition_config.description IS NULL
        AND customer_condition_config.orders_number_less_than IS NULL
        AND customer_condition_config.days_since_first_order_less_than IS NULL
      )
      THEN TRUE -- Base case standard scheme / no condition check needed
    -- new to darkstore vertical conditions:
    WHEN (
      customer_condition_config.counting_method = 'VENDOR_VERTICAL' -- string used to identify new to vertical conditions
      AND vertical_type = 'darkstores' -- darkstores new customer conditions apply only to this vertical
      AND customer_condition_config.orders_number_less_than IS NOT NULL
      AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
     ) THEN
        order_count_vertical <= customer_condition_config.orders_number_less_than
        AND days_since_first_vertical_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (
      customer_condition_config.counting_method = 'VENDOR_VERTICAL'
      AND vertical_type = 'darkstores'
      AND customer_condition_config.orders_number_less_than IS NOT NULL
     ) THEN
        order_count_vertical <= customer_condition_config.orders_number_less_than
    WHEN (
      customer_condition_config.counting_method = 'VENDOR_VERTICAL'
      AND vertical_type = 'darkstores'
      AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
     ) THEN
        days_since_first_vertical_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (
      customer_condition_config.counting_method = 'VENDOR_VERTICAL'
      AND vertical_type = 'darkstores'
      AND customer_condition_config.description = 'New'
     ) THEN
        order_count_vertical = 0 OR order_count_vertical IS NULL
    -- new to qcommerce vertical conditions:
    WHEN (
      customer_condition_config.counting_method = 'QCOMMERCE'
      AND vertical_type != 'restaurants' -- qcommerce orders are all orders not in restaurants or courier. Darkstore orders are a special qcommerce case handled above.
      AND vertical_type NOT LIKE 'courier%'
      AND customer_condition_config.orders_number_less_than IS NOT NULL
      AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
     ) THEN
        order_count_qcommerce <= customer_condition_config.orders_number_less_than
        AND days_since_first_qcommerce_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (
      customer_condition_config.counting_method = 'QCOMMERCE'
      AND vertical_type != 'restaurants'
      AND vertical_type NOT LIKE 'courier%'
      AND customer_condition_config.orders_number_less_than IS NOT NULL
     ) THEN
        order_count_qcommerce <= customer_condition_config.orders_number_less_than
    WHEN (
      customer_condition_config.counting_method = 'QCOMMERCE'
      AND vertical_type != 'restaurants'
      AND vertical_type NOT LIKE 'courier%'
      AND customer_condition_config.days_since_first_order_less_than IS NOT NULL
     ) THEN
        days_since_first_qcommerce_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (
      customer_condition_config.counting_method = 'QCOMMERCE'
      AND vertical_type != 'restaurants'
      AND vertical_type NOT LIKE 'courier%'
      AND customer_condition_config.description = 'New'
     ) THEN
        order_count_qcommerce = 0 OR order_count_qcommerce IS NULL
    -- regular new customer conditions:
    WHEN (customer_condition_config.orders_number_less_than IS NOT NULL AND customer_condition_config.days_since_first_order_less_than IS NOT NULL) THEN
      order_count_total <= customer_condition_config.orders_number_less_than
      AND days_since_first_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (customer_condition_config.orders_number_less_than IS NOT NULL) THEN
      order_count_total <= customer_condition_config.orders_number_less_than
    WHEN (customer_condition_config.days_since_first_order_less_than IS NOT NULL) THEN
      days_since_first_order <= customer_condition_config.days_since_first_order_less_than
    WHEN (customer_condition_config.description = 'New') THEN -- description can be New, Existing, or NULL
      order_count_total <= 1 OR order_count_total IS NULL -- It's the first order
  END
);

IF VAR_assignment_id IS NULL THEN -- if no assignment ID is passed in, we optimize based on vendor list and conditions. Added in LOGDPO-616.
  CREATE OR REPLACE TEMP TABLE orders_for_given_vendor_list AS
  WITH day_of_week_mapper AS (
    -- this formatting below is condensed to be more legible than our coding guidelines suggest:
    SELECT 1 AS weekday_id, "SUNDAY" AS weekday_name, DATE("1905-01-01") AS arbitrary_date UNION ALL
    SELECT 2, "MONDAY", DATE("1905-01-02") UNION ALL
    SELECT 3, "TUESDAY", DATE("1905-01-03") UNION ALL
    SELECT 4, "WEDNESDAY", DATE("1905-01-04") UNION ALL
    SELECT 5, "THURSDAY", DATE("1905-01-05") UNION ALL
    SELECT 6, "FRIDAY", DATE("1905-01-06") UNION ALL
    SELECT 7, "SATURDAY", DATE("1905-01-07")
  ), time_conditions AS (
    SELECT
      VAR_schedule_config.start_at
      , VAR_schedule_config.end_at
      , VAR_schedule_config.recurrence
      , VAR_schedule_config.recurrence_end_at
      , VAR_schedule_config.is_all_day
      , TIMESTAMP_DIFF(VAR_schedule_config.end_at, VAR_schedule_config.start_at, MILLISECOND) AS scheme_duration_ms
      , CASE
          WHEN VAR_schedule_config.recurrence = 'DAILY' THEN ARRAY(SELECT weekday_name FROM day_of_week_mapper)
          WHEN VAR_schedule_config.recurrence = 'WEEKLY' THEN [FORMAT_DATE('%A', DATETIME(VAR_schedule_config.start_at))]
        ELSE VAR_schedule_config.active_days
      END AS active_days
  ), time_conditions_flattened AS (
    SELECT
      * EXCEPT (active_days)
      , DATETIME(mapper.arbitrary_date, EXTRACT(TIME FROM tc.start_at)) AS _arbitrary_recurrence_starts
      , DATETIME_ADD(
        DATETIME(mapper.arbitrary_date, EXTRACT(TIME FROM tc.start_at))
        , INTERVAL TIMESTAMP_DIFF(tc.end_at, tc.start_at, MILLISECOND) MILLISECOND
      ) AS _arbitrary_recurrence_ends
    FROM time_conditions tc
    LEFT JOIN UNNEST(active_days) recurrence_flattened -- left join keeps null/empty arrays
    LEFT JOIN day_of_week_mapper mapper ON recurrence_flattened = mapper.weekday_name
  ), area_conditions AS (
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
    , dps.dps_travel_time_fee_local
    , ROW_NUMBER() OVER(PARTITION BY dps.entity_id ORDER BY dps.dps_travel_time ASC) / COUNT(*) OVER () * 100 AS tt_percentile -- this adds fake quartiles ties in travel time. Known limitation of this implementation. Pending replacement with PERCENT_RANK() OVER (ORDER BY o.dps_travel_time ASC) * 100 tt_percentile
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps
  LEFT JOIN area_conditions ac ON TRUE -- left join on true instead of cross join means that we retain orders even if no conditions exist
  LEFT JOIN time_conditions_flattened tc ON TRUE
  LEFT JOIN day_of_week_mapper mapper ON mapper.weekday_id = EXTRACT(DAYOFWEEK FROM dps.order_placed_at)
  WHERE
    -- filter by utc date (dps_sessions_mapped_to_orders partition field)
    -- we use 15 days lookback to make sure we don't miss orders due to timezones
    dps.created_date BETWEEN
      IFNULL(VAR_start_date, DATE_SUB(CURRENT_DATE, INTERVAL 15 DAY))
      AND IFNULL(VAR_end_date, CURRENT_DATE) -- we perform this filter first since the dps table is partitioned by order_placed_at
    -- local date (displayed in the DPS UI)
    AND dps.created_date_local BETWEEN
      IFNULL(VAR_start_date, DATE_SUB(CURRENT_DATE, INTERVAL 2 WEEK))
      AND IFNULL(VAR_end_date, CURRENT_DATE)
    -- entity_id and vendor list
    AND dps.vendor_id IN UNNEST(VAR_vendor_list)
    AND dps.entity_id = VAR_entity_id
    -- variants
    AND IF(ARRAY_LENGTH(VAR_included_variants) > 0, dps.test_variant IN UNNEST(VAR_included_variants) OR dps.test_variant IS NULL, TRUE)
    -- assignment_type
    AND IF(ARRAY_LENGTH(VAR_vendor_price_scheme_type) > 0, dps.vendor_price_scheme_type IN UNNEST(VAR_vendor_price_scheme_type), TRUE)
    -- successfully delivered orders only
    AND dps.is_sent = TRUE
    -- new customer condition
    AND CHECK_CUSTOMER_CONDITION(
      dps.vertical_type
      , NULL -- is_default_scheme not available since we aren't joining ASA data
      , STRUCT(
        VAR_customer_condition_config.type
        , VAR_customer_condition_config.orders_number_less_than
        , VAR_customer_condition_config.days_since_first_order_less_than
        , VAR_customer_condition_config.counting_method
      )
      , IFNULL(dps.customer_total_orders, 0)
      , IFNULL(dps.customer_total_orders_darkstores, 0)
      , IFNULL(dps.customer_total_orders_quick_commerce, 0)
      , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_date, dps.order_placed_at), SECOND) / 86400 -- 86400 seconds in one day
      , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_darkstores_date, dps.order_placed_at), SECOND) / 86400
      , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_quick_commerce_date, dps.order_placed_at), SECOND) / 86400
    ) = TRUE
    -- time condition
    AND CASE
      WHEN (tc.start_at IS NOT NULL AND tc.recurrence IS NULL) THEN
        dps.order_placed_at BETWEEN tc.start_at AND tc.end_at -- if there is no recurrence, calculate this
      WHEN (tc.start_at IS NOT NULL AND tc.recurrence IS NOT NULL) THEN -- if there is recurrence, calculate below
        dps.order_placed_at BETWEEN tc.start_at AND tc.end_at
        OR (dps.order_placed_at >= tc.start_at
          AND IF(tc.recurrence_end_at IS NOT NULL, dps.order_placed_at <= tc.recurrence_end_at, TRUE)
          AND (
            DATETIME(DATE(mapper.arbitrary_date), EXTRACT(TIME FROM dps.order_placed_at))
            BETWEEN tc._arbitrary_recurrence_starts AND tc._arbitrary_recurrence_ends
            OR DATETIME_ADD(DATETIME(DATE(mapper.arbitrary_date), EXTRACT(TIME FROM dps.order_placed_at)), INTERVAL 1 WEEK)
            BETWEEN tc._arbitrary_recurrence_starts AND tc._arbitrary_recurrence_ends
          )
        )
      ELSE TRUE -- if there is no time condition, set to TRUE
    END
    -- customer location condition
    AND CASE
      WHEN ac.area_id IS NOT NULL THEN -- area_configs is never null, even when empty
        ST_COVERS(ac.polygon, dps.customer_location)
      ELSE TRUE -- if no area condition, set to TRUE
    END
  QUALIFY ROW_NUMBER() OVER(PARTITION BY dps.entity_id, dps.platform_order_code ORDER BY tc.start_at DESC, ac.area_id) = 1 -- in the case an order meets multiple time conditions due to overlapping durations, or falls into multiple area conditions, we take only one order
  ;
  WITH tiers AS ( -- allows us to join the two arrays with shares and new MOVs passed in as variables
    SELECT
      ROW_NUMBER() OVER () AS tier -- UNNEST retains order of the array
      , IFNULL(LAG(upper_tt_percentile) OVER (ORDER BY upper_tt_percentile), 0) AS lower_tt_percentile
      , upper_tt_percentile
    FROM UNNEST(VAR_share_per_tier) upper_tt_percentile
  )
  SELECT
    tiers.tier
    , tiers.upper_tt_percentile - tiers.lower_tt_percentile AS share
    , tiers.upper_tt_percentile AS cum_share
    , MAX(o.dps_travel_time) AS dps_travel_time_decimal
    , FORMAT_TIME('%M:%S', TIME(TIMESTAMP_SECONDS(CAST(MAX(o.dps_travel_time) * 60 AS INT64)))) AS dps_travel_time_formatted
    , ROUND(AVG(o.dps_travel_time_fee_local), 2) AS current_average_travel_time_fee_local
    , COUNT(o.platform_order_code) AS num_orders
    , SUM(COUNT(o.platform_order_code)) OVER () AS total_orders
  FROM tiers
  LEFT JOIN orders_for_given_vendor_list o ON o.tt_percentile > tiers.lower_tt_percentile AND o.tt_percentile <= tiers.upper_tt_percentile
  GROUP BY 1, 2, 3
  ORDER BY 1, 2, 3
  ;
ELSE -- if we get an assignment ID, we optimize based on scheme priority and ASA conditions
  CREATE OR REPLACE TEMP TABLE asa_configs AS (
    SELECT
      entity_id
      , asa_id
      , sorted_assigned_vendor_ids
      , assigned_vendors_count
      , n_schemes > 1 AS do_conditions_exist
      , apc.asa_price_config_id AS vendor_group_price_config_id
      , apc.priority AS scheme_priority
      , apc.is_default_scheme
      , apc.customer_condition_id
      , apc.customer_condition_config
      , apc.schedule_id
      , apc.schedule_config
      , apc.area_configs
      , IFNULL(LAG(ttc.travel_time_threshold, 1) OVER (PARTITION BY apc.priority ORDER BY ttc.travel_time_threshold ASC NULLS LAST), 0) AS travel_time_threshold_start
      , ttc.travel_time_threshold AS travel_time_threshold_end
      , ttc.travel_time_fee
    FROM `fulfillment-dwh-production.cl.pricing_asa_full_configuration_versions`
    CROSS JOIN UNNEST(asa_price_config) apc
    CROSS JOIN UNNEST(apc.scheme_component_configs.travel_time_config) ttc
    WHERE
      asa_id = VAR_assignment_id
      AND entity_id = VAR_entity_id
      AND active_to IS NULL
  )
  ;
  ASSERT (SELECT assigned_vendors_count > 0 FROM asa_configs LIMIT 1) AS 'ERROR: ASA returned no assigned vendors.' -- Catches an error we seem to have in our pipeline where some ASA configs show 0 assigned vendors.
  ;
  CREATE OR REPLACE TEMP TABLE orders_for_given_vgpc_id AS
  WITH day_of_week_mapper AS (
    -- this formatting below is condensed to be more legible than our coding guidelines suggest:
    SELECT 1 AS weekday_id, "SUNDAY" AS weekday_name, DATE("1905-01-01") AS arbitrary_date UNION ALL
    SELECT 2, "MONDAY", DATE("1905-01-02") UNION ALL
    SELECT 3, "TUESDAY", DATE("1905-01-03") UNION ALL
    SELECT 4, "WEDNESDAY", DATE("1905-01-04") UNION ALL
    SELECT 5, "THURSDAY", DATE("1905-01-05") UNION ALL
    SELECT 6, "FRIDAY", DATE("1905-01-06") UNION ALL
    SELECT 7, "SATURDAY", DATE("1905-01-07")
  ), conditions AS (
    SELECT
      *
      , TIMESTAMP_DIFF(schedule_config.end_at, schedule_config.start_at, MILLISECOND) AS scheme_duration_ms
      , CASE
          WHEN schedule_config.recurrence = 'DAILY' THEN ARRAY(SELECT weekday_name FROM day_of_week_mapper)
          WHEN schedule_config.recurrence = 'WEEKLY' THEN [FORMAT_DATE('%A', DATETIME(schedule_config.start_at))]
        ELSE schedule_config.active_days
      END AS active_days
    FROM asa_configs
  ), conditions_flattened AS (
    SELECT
      * EXCEPT (active_days)
      , DATETIME(mapper.arbitrary_date, EXTRACT(TIME FROM schedule_config.start_at)) AS _arbitrary_recurrence_starts
      , DATETIME_ADD(
        DATETIME(mapper.arbitrary_date, EXTRACT(TIME FROM schedule_config.start_at))
        , INTERVAL TIMESTAMP_DIFF(schedule_config.end_at, schedule_config.start_at, MILLISECOND) MILLISECOND
      ) AS _arbitrary_recurrence_ends
    FROM conditions
    LEFT JOIN UNNEST(active_days) recurrence_flattened -- left join keeps null/empty arrays
    LEFT JOIN day_of_week_mapper mapper ON recurrence_flattened = mapper.weekday_name
  ), orders_with_all_conditions AS ( -- Here we figure out what scheme the order SHOULD be in based on our other IDs
    SELECT
      dps.entity_id
      , dps.platform_order_code
      , dps.order_placed_at
      , dps.dps_travel_time
      , dps.dps_travel_time_fee_local
      , c.asa_id
      , c.vendor_group_price_config_id
      , c.scheme_priority
      , c.is_default_scheme
      , c.travel_time_threshold_start
      , c.travel_time_threshold_end
      , c.travel_time_fee
      , c.customer_condition_id
      , c.customer_condition_config
      , c.schedule_id
      , c.schedule_config
      , c._arbitrary_recurrence_starts
      , c._arbitrary_recurrence_ends
      , area.area_id
      -- New customer condition
      , CHECK_CUSTOMER_CONDITION(
        dps.vertical_type
        , c.is_default_scheme
        , c.customer_condition_config
        , IFNULL(dps.customer_total_orders, 0)
        , IFNULL(dps.customer_total_orders_darkstores, 0)
        , IFNULL(dps.customer_total_orders_quick_commerce, 0)
        , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_date, dps.order_placed_at), SECOND) / 86400 -- 86400 seconds in one day
        , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_darkstores_date, dps.order_placed_at), SECOND) / 86400
        , DATETIME_DIFF(dps.order_placed_at, IFNULL(dps.customer_first_order_quick_commerce_date, dps.order_placed_at), SECOND) / 86400
      ) AS is_matching_customer_condition
      -- Customer location condition
      , CASE
          WHEN c.is_default_scheme IS TRUE THEN NULL -- Base case standard scheme / no condition check needed
          WHEN area.area_id IS NOT NULL THEN -- area_configs is never null, even when empty
            ST_COVERS(area.polygon, dps.customer_location)
        END AS is_matching_location_condition
      -- Time condition
      , CASE
          WHEN c.is_default_scheme IS TRUE THEN NULL -- Base case standard scheme / no condition check needed
          WHEN (c.schedule_id IS NOT NULL AND c.schedule_config.recurrence IS NULL) THEN -- First check if time condition exists
            dps.order_placed_at BETWEEN c.schedule_config.start_at AND c.schedule_config.end_at -- if there is no recurrence, calculate this
          WHEN (c.schedule_id IS NOT NULL AND c.schedule_config.recurrence IS NOT NULL) THEN -- if there is recurrence, calculate below
            dps.order_placed_at BETWEEN c.schedule_config.start_at AND c.schedule_config.end_at
            OR (
              dps.order_placed_at >= c.schedule_config.start_at
              AND IF(c.schedule_config.recurrence_end_at IS NOT NULL, dps.order_placed_at <= c.schedule_config.recurrence_end_at, TRUE)
              AND (
                DATETIME(DATE(mapper.arbitrary_date), EXTRACT(TIME FROM dps.order_placed_at))
                BETWEEN c._arbitrary_recurrence_starts AND c._arbitrary_recurrence_ends
                OR DATETIME_ADD(DATETIME(DATE(mapper.arbitrary_date), EXTRACT(TIME FROM dps.order_placed_at)), INTERVAL 1 WEEK)
                BETWEEN c._arbitrary_recurrence_starts AND c._arbitrary_recurrence_ends
              )
            )
        END AS is_matching_time_condition
      FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps
      LEFT JOIN day_of_week_mapper mapper ON mapper.weekday_id = EXTRACT(DAYOFWEEK FROM dps.order_placed_at)
      CROSS JOIN conditions_flattened c
      LEFT JOIN UNNEST(c.area_configs) area
      WHERE
        -- filter by utc date (dps_sessions_mapped_to_orders partition field)
        -- we use 15 days lookback to make sure we don't miss orders due to timezones
        dps.created_date BETWEEN
          IFNULL(VAR_start_date, DATE_SUB(CURRENT_DATE, INTERVAL 15 DAY))
          AND IFNULL(VAR_end_date, CURRENT_DATE) -- we perform this filter first since the dps table is partitioned by order_placed_at
        -- local date (displayed in the DPS UI)
        AND dps.created_date_local BETWEEN
          IFNULL(VAR_start_date, DATE_SUB(CURRENT_DATE, INTERVAL 14 DAY))
          AND IFNULL(VAR_end_date, CURRENT_DATE)
        -- entity_id and vendor list
        AND dps.entity_id = VAR_entity_id
        AND dps.vendor_id IN UNNEST(c.sorted_assigned_vendor_ids)
        -- variants
        AND IF(ARRAY_LENGTH(VAR_included_variants) > 0, dps.test_variant IN UNNEST(VAR_included_variants) OR dps.test_variant IS NULL, TRUE)
        -- assignment_type
        AND IF(ARRAY_LENGTH(VAR_vendor_price_scheme_type) > 0, dps.vendor_price_scheme_type IN UNNEST(VAR_vendor_price_scheme_type), TRUE)
        -- successfully delivered orders only
        AND dps.is_sent = TRUE
        -- filter only travel time tiers that could apply to the order
        AND (dps.travel_time <= c.travel_time_threshold_end OR c.travel_time_threshold_end IS NULL)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY dps.entity_id, dps.platform_order_code, c.vendor_group_price_config_id, c._arbitrary_recurrence_starts, area.area_id ORDER BY c.travel_time_threshold_end ASC NULLS LAST) = 1 -- only get the corresponding travel time row for each order. Also handles cases where an order might meet multiple time or location conditions.
  ), orders_with_min_priority_condition AS (
    SELECT
      entity_id
      , platform_order_code
      , dps_travel_time
      , dps_travel_time_fee_local
      , travel_time_threshold_start
      , travel_time_threshold_end
      , travel_time_fee
      , asa_id
      , vendor_group_price_config_id
      , scheme_priority
      , is_default_scheme
    FROM orders_with_all_conditions
    WHERE -- only look at the priorities with a matching condition
      (area_id IS NULL OR (area_id IS NOT NULL AND is_matching_location_condition))
      AND (schedule_id IS NULL OR (schedule_id IS NOT NULL AND is_matching_time_condition))
      AND (customer_condition_id IS NULL OR (customer_condition_id IS NOT NULL AND is_matching_customer_condition))
    QUALIFY MIN(scheme_priority) OVER(PARTITION BY platform_order_code) = scheme_priority -- gets only the rows with the lowest priority scheme
      AND ROW_NUMBER() OVER(PARTITION BY platform_order_code ORDER BY _arbitrary_recurrence_starts DESC, area_id) = 1 -- if there are more than one condition that are true, we select one order among them
  )
  SELECT *
  FROM orders_with_min_priority_condition
  WHERE IF(VAR_vgpc_id IS NULL, is_default_scheme, vendor_group_price_config_id = VAR_vgpc_id)
  ;
  WITH percents AS (
    SELECT
      asa_configs.travel_time_threshold_start
      , asa_configs.travel_time_threshold_end
      , asa_configs.travel_time_fee
      , COUNT(orders.platform_order_code) AS num_orders
      , SAFE_DIVIDE(SUM(orders.dps_travel_time_fee_local), COUNT(orders.platform_order_code)) AS current_average_travel_time_fee_local
      , SAFE_DIVIDE(COUNT(orders.platform_order_code), SUM(COUNT(orders.platform_order_code)) OVER()) * 100 AS share
    FROM asa_configs -- we rejoin this table to add the missing travel time tiers with no orders associated in them
    LEFT JOIN orders_for_given_vgpc_id orders ON asa_configs.travel_time_threshold_start = orders.travel_time_threshold_start
    WHERE IF(VAR_vgpc_id IS NULL, asa_configs.is_default_scheme, asa_configs.vendor_group_price_config_id = VAR_vgpc_id)
    GROUP BY 1, 2, 3
  ), order_share AS (
    SELECT
      ROW_NUMBER() OVER(ORDER BY travel_time_threshold_start) AS tier
      , SUM(share) OVER(ORDER BY travel_time_threshold_start) AS cum_share
      , percents.*
      , SUM(num_orders) OVER() AS total_orders
    FROM percents
  )
  -- Formatting to match the DBDF optimizer output
  SELECT
    tier
    , share
    , cum_share
    , travel_time_threshold_end AS dps_travel_time_decimal
    , FORMAT_TIME("%H:%M:%S", TIME(TIMESTAMP_SECONDS(CAST(ROUND(travel_time_threshold_end * 60) AS INT64)))) AS dps_travel_time_formatted
    , travel_time_fee AS configured_travel_time_fee
    , current_average_travel_time_fee_local
    , num_orders AS orders
    , total_orders
  FROM order_share
  ORDER BY 1
  ;
END IF
;
END
