CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.hexagon_data`
PARTITION BY created_date
CLUSTER BY global_entity_id, city_name
OPTIONS (partition_expiration_days = NULL) AS (
  WITH  
  dps_entities AS (
    SELECT
      global_entity_id,
      global_entity_id AS entity_id,
      country_code
    FROM `fulfillment-dwh-production.dl.dynamic_pricing_global_configuration`
  )
  , points AS (
    SELECT
      dps.entity_id,
      ci.name AS city_name,
      -- Get all H3 indexes within a geographic polygon
      jslibs.h3.ST_H3_POLYFILLFROMGEOG(
        -- Union all the zone shapes of a city into a single multipolygon
        ST_UNION_AGG(zo.shape),
        -- Define the H3 resolution
        9
      ) AS area
    FROM `fulfillment-dwh-production.cl.countries` co
    INNER JOIN dps_entities dps USING (country_code)
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(ci.zones) zo
    WHERE TRUE
      AND ci.is_active
      AND zo.is_active
    GROUP BY 1, 2
  )
  , hexagons AS (
    SELECT
      entity_id,
      city_name,
      h3,
      -- Get the center of each h3
      `carto-os.carto.H3_CENTER`(h3) AS center,
      -- -- Create a circular polygon around h3
      -- ST_BUFFERWITHTOLERANCE(
      --   -- Get the center of each h3
      --   `carto-os.carto.H3_CENTER`(h3),
      --   -- Define the radius size: 1000m ~ 5.7x h3 resolution 9 edge size (https://h3geo.org/docs/core-library/restable/)
      --   1000,
      --   50
      -- ) AS polygon_1km
    FROM points, UNNEST(area) h3
  )
  , orders_data AS (
    SELECT
      o.global_entity_id,
      o.order_id,
      o.delivery_location.h3_level_9_index AS h3,
      o.value.order.gbv_local AS gbv,
      dps.delivery_costs_local AS cpo,
      o.value.charges.delivery_fee_local AS df,
      o.value.charges.delivery_fee_local + o.value.charges.service_fee_customer_local + o.value.charges.mov_customer_fee_local + o.value.charges.delivery_priority_fee_local AS cf,
      o.value.charges.delivery_fee_local + o.value.charges.service_fee_customer_local + o.value.commission.commission_local + o.value.charges.mov_customer_fee_local + o.value.charges.delivery_priority_fee_local + o.value.charges.joker_vendor_fee_local AS revenue,
      o.value.incentives.voucher_dh_local + o.value.incentives.discount_dh_local AS incentives,
      ST_GEOGPOINT(o.delivery_location.longitude, o.delivery_location.latitude) AS delivery_coordinates
    FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders` o
    INNER JOIN dps_entities e USING (global_entity_id)
    INNER JOIN `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps ON dps.entity_id = o.global_entity_id AND dps.platform_order_code = o.order_id
    WHERE DATE(o.partition_date_local) BETWEEN CURRENT_DATE() - 30 AND CURRENT_DATE() - 3
      AND o.is_successful
      AND NOT o.is_qcommerce
      AND o.is_delivery_by_dh
      AND dps.delivery_costs_local + o.value.charges.delivery_fee_local + o.value.charges.service_fee_customer_local + o.value.commission.commission_local + o.value.charges.mov_customer_fee_local + o.value.charges.delivery_priority_fee_local + o.value.charges.joker_vendor_fee_local IS NOT NULL
  )
  , hexagon_data AS (
    SELECT
      o.global_entity_id,
      o.order_id,
      h.city_name,
      h.h3,
      h.center,
      h.h3 = o.h3 AS hex_order,
      o.gbv,
      o.df,
      o.cf,
      o.revenue,
      o.cpo,
      o.incentives,
      o.revenue - o.cpo AS gpo,
      o.revenue - o.cpo - o.incentives AS flgpo,
      -- h.polygon_1km,
      (1000 - ST_DISTANCE(h.center, o.delivery_coordinates)) / 1000 AS order_weight,
      ST_DISTANCE(h.center, o.delivery_coordinates) AS distance
    FROM orders_data o
    CROSS JOIN hexagons h
    -- Define the radius size to be considered: 1000m ~ 5.7x h3 resolution 9 edge size (https://h3geo.org/docs/core-library/restable/)
    WHERE ST_DISTANCE(h.center, o.delivery_coordinates) <= 1000
  )
  SELECT
    global_entity_id,
    city_name,
    h3,
    ST_X(center) AS longitude,
    ST_Y(center) AS latitude,
    CURRENT_DATE AS created_date,
    -- Orders
    SUM(1) AS orders_considered,
    SUM(CASE WHEN hex_order THEN 1 END) AS hex_orders,
    SUM(order_weight) AS order_weight,
    -- AFV
    SAFE_DIVIDE(SUM(gbv * order_weight), SUM(order_weight)) AS weighted_afv,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN gbv END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_afv,
    -- Delivery Fee
    SAFE_DIVIDE(SUM(df * order_weight), SUM(order_weight)) AS weighted_df,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN df END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_df,
    SAFE_DIVIDE(SUM(df * order_weight), SUM(gbv * order_weight)) AS weighted_df_of_gbv_share,
    -- Customer Fees
    SAFE_DIVIDE(SUM(cf * order_weight), SUM(order_weight)) AS weighted_cf,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN cf END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_cf,
    SAFE_DIVIDE(SUM(cf * order_weight), SUM(gbv * order_weight)) AS weighted_cf_of_gbv_share,
    -- Revenue
    SAFE_DIVIDE(SUM(revenue * order_weight), SUM(order_weight)) AS weighted_revenue,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN revenue END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_revenue,
    SAFE_DIVIDE(SUM(revenue * order_weight), SUM(gbv * order_weight)) AS weighted_revenue_of_gbv_share,
    -- CPO
    SAFE_DIVIDE(SUM(cpo * order_weight), SUM(order_weight)) AS weighted_cpo,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN cpo END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_cpo,
    SAFE_DIVIDE(SUM(cpo * order_weight), SUM(gbv * order_weight)) AS weighted_cpo_of_gbv_share,
    -- Gross Profit
    SAFE_DIVIDE(SUM(gpo * order_weight), SUM(order_weight)) AS weighted_gpo,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN gpo END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_gpo,
    SAFE_DIVIDE(SUM(gpo * order_weight), SUM(gbv * order_weight)) AS weighted_gpo_of_gbv_share,
    -- Incentives
    SAFE_DIVIDE(SUM(incentives * order_weight), SUM(order_weight)) AS weighted_incentives,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN incentives END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_incentives,
    SAFE_DIVIDE(SUM(incentives * order_weight), SUM(gbv * order_weight)) AS weighted_incentive_of_gbv_share,
    -- Net Profit
    SAFE_DIVIDE(SUM(flgpo * order_weight), SUM(order_weight)) AS weighted_flgpo,
    SAFE_DIVIDE(SUM(CASE WHEN hex_order THEN flgpo END), SUM(CASE WHEN hex_order THEN 1 END)) AS hex_flgpo,
    SAFE_DIVIDE(SUM(flgpo * order_weight), SUM(gbv * order_weight)) AS weighted_flgpo_of_gbv_share
  FROM hexagon_data
  GROUP BY 1, 2, 3, 4, 5
)
