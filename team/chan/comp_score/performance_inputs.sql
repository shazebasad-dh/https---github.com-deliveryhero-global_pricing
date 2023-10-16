  DECLARE from_date_filter, to_date_filter, filter_month DATE;
  SET from_date_filter = "2023-03-01";
  SET to_date_filter = "2024-09-01";


  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compscore_performance_inputs`
  AS

  ############################################ TRANSACTIONAL DATA WITH SUBS
    WITH countries as (
    select distinct
    segment as region
    , management_entity
    , country_name
    , lower(country_code) as country_code
    , global_entity_id as entity_id
    from `fulfillment-dwh-production.curated_data_shared_central_dwh.global_entities`
    )


    , load_cdwh as (
    SELECT
    order_id as platform_order_code
    , DATE(placed_at_local) as created_date_local
    , global_entity_id as entity_id
    , IFNULL(value.service_fee_eur,0) AS service_fee_eur
    , IFNULL(value.delivery_fee_eur,0) AS delivery_fee_eur
    , IFNULL(value.mov_customer_fee_eur,0) AS small_basket_fee_eur
    , value.gbv_eur as gfv_eur
    , value.gmv_eur
    , value.mov_eur
    , value.commission_eur
    , is_qcommerce

    FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders`
    WHERE placed_at_local BETWEEN from_date_filter AND to_date_filter
    AND is_sent
    AND is_own_delivery
    )


    , load_dps as (
    SELECT
    platform_order_code
    , entity_id
    , delivery_costs_eur
    , has_subscription
    , exchange_rate
    , has_subscription_discount AS has_subscription_and_discount

    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders`
    WHERE created_date BETWEEN from_date_filter AND to_date_filter
    AND delivery_costs_eur IS NOT NULL
    AND entity_id NOT IN (
    "HS_BH"
    )
    )

    , load_tpro as (
    SELECT
    /*
    TRUE IF user is subscriber and received benefits
    */
    CAST(order_id AS string) as platform_order_code
    , is_tpro_order as tb_has_subscription_and_discount
    , app_version
    , lower(country_Code) as country_code
    FROM `tlb-data-prod.data_platform.fct_order_info`
    WHERE order_date >= date_sub(from_date_filter, interval 2 day)
    AND order_date <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_hs as (
    SELECT
    /*
    is_user_subscribed -> only if user is subscriber
    is_subscribed -> user subscriber + benefits
    */
    CAST(platform_order_code AS STRING) platform_order_code
    , rdf_offer_applied AS is_rdf_order
    , rdf_offer_restaurant_max_charge AS rdf_vendor_contribution
    , OD_delivery_fee - IFNULL(delivery_fee_discount,0) - IFNULL(subscribed_discount_amount,0) as loaded_delivery_fee
    , is_subscribed as hs_has_subscription_and_discount
    , is_user_subscribed as hs_has_subscription
    FROM `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders`
    WHERE operation_day >= date_sub(from_date_filter, interval 2 day)
    AND operation_day <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)


    )

    , load_peya as (
    SELECT
    CAST(order_id AS STRING) as platform_order_code
    , LOWER(country.country_code) AS country_code
    , is_user_plus as peya_has_subscription
    , has_plus_shipping_cost_discount as peya_has_subscription_and_discount
    , shipping_amount as peya_delivery_fee
    FROM `peya-bi-tools-pro.il_core.fact_orders`
    WHERE registered_date >= date_sub(from_date_filter, interval 2 day)
    AND registered_date <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_pd_apac as (
    SELECT
    global_entity_id as entity_id
    , order_code as platform_order_code
    , is_subscriber_order as fp_has_subscription
    , is_subscription_benefit_order as fp_has_subscription_and_benefits
    FROM `fulfillment-dwh-production.pandata_report.regional_apac_pd_orders_agg_sb_subscriptions`
    WHERE created_date_utc >= date_sub(from_date_filter, interval 2 day)
    AND created_date_utc <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )

    , load_pd_eu as (
    SELECT
    global_entity_id as entity_id
    , order_code as platform_order_code
    , is_subscriber_order as fd_has_subscription
    FROM `fulfillment-dwh-production.pandata_report.regional_eu__pd_orders_agg_sb_subscriptions`
    WHERE created_date_utc >= date_sub(from_date_filter, interval 2 day)
    AND created_date_utc <= DATE_ADD(to_date_filter, INTERVAL 2 DAY)
    )


    , join_tables as (
    SELECT
    c.*
    , cdwh.* EXCEPT(entity_id)
    , dps.delivery_costs_eur

    , CASE
    WHEN hs.is_rdf_order = 1 THEN SAFE_DIVIDE(loaded_delivery_fee,exchange_rate)
    ELSE delivery_fee_eur
    END AS loaded_delivery_fee

    , CASE
    WHEN hs.is_rdf_order = 1 THEN SAFE_DIVIDE(rdf_vendor_contribution,exchange_rate)
    ELSE 0
    END AS vendor_funded_deals_amount

    /*
    THIS FIELD IDENTIFY WETHER THE USER IS A SUBSCRIBER OR NOT!
    As of Q2/2023, for talabat we only know if an order made by susbscription
    users got benefits.
    */
    , CASE
    WHEN has_subscription THEN TRUE
    WHEN hs_has_subscription THEN TRUE
    WHEN peya_has_subscription = 1 THEN TRUE
    WHEN apac.fp_has_subscription THEN TRUE
    WHEN eu.fd_has_subscription THEN TRUE

    WHEN tb_has_subscription_and_discount THEN TRUE
    ELSE has_subscription
    END AS has_subscription
    /*
    Field to use when user is a sub and got benefits
    As of Q2/2023, for Foodora (Europe countries) we only know if an order made by susbscription
    users got benefits.
    */
    , CASE
    WHEN has_subscription_and_discount THEN TRUE
    WHEN hs_has_subscription_and_discount THEN TRUE
    WHEN peya_has_subscription_and_discount = 1 THEN TRUE
    WHEN fp_has_subscription_and_benefits THEN TRUE
    WHEN tb_has_subscription_and_discount = TRUE THEN TRUE
    WHEN fd_has_subscription THEN TRUE
    ELSE has_subscription_and_discount
    END AS has_subscription_and_discount

    FROM load_cdwh cdwh

    INNER JOIN load_dps dps
    ON cdwh.entity_id = dps.entity_id
    AND cdwh.platform_order_code = dps.platform_order_code

    LEFT JOIN countries c
    ON cdwh.entity_id = c.entity_id

    LEFT JOIN load_peya py
    ON c.country_code = py.country_code
    AND cdwh.platform_order_code = py.platform_order_code


    LEFT JOIN load_tpro tb
    ON c.country_code = tb.country_code
    AND cdwh.platform_order_code = tb.platform_order_code

    LEFT JOIN load_hs hs
    ON cdwh.entity_id = "HS_SA"
    AND cdwh.platform_order_code = hs.platform_order_code

    LEFT JOIN load_pd_apac apac
    ON cdwh.entity_id = apac.entity_id
    AND cdwh.platform_order_code = apac.platform_order_code

    LEFT JOIN load_pd_eu eu
    ON cdwh.entity_id = eu.entity_id
    AND cdwh.platform_order_code = eu.platform_order_code
    )

    , add_calculated_fields AS (


    SELECT *

    , service_fee_eur + small_basket_fee_eur as non_df_fees

    --- nominal
    , service_fee_eur + delivery_fee_eur + small_basket_fee_eur as customer_fee_eur
    , service_fee_eur + delivery_fee_eur + small_basket_fee_eur + commission_eur as take_in
    , service_fee_eur + delivery_fee_eur + small_basket_fee_eur + commission_eur - delivery_costs_eur as profit

    --- Loaded
    , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur as loaded_customer_fee_eur
    , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur + vendor_funded_deals_amount + commission_eur as loaded_take_in
    , service_fee_eur + loaded_delivery_fee + small_basket_fee_eur + vendor_funded_deals_amount + commission_eur - delivery_costs_eur as loaded_profit

    FROM join_tables
    )

  ############################################

  ############################################ Aggregate for compliance score purposes

    , aggregate_quarter_performance AS (
      SELECT  
      entity_id
      , management_entity
      /*
      Prev quarter performance is used for the current one
      */
      , DATE_ADD(DATE_TRUNC(created_date_local, QUARTER), INTERVAL 1 QUARTER) AS quarter
      , SAFE_DIVIDE(sum(loaded_customer_fee_eur), SUM(gfv_eur)) as cf_over_afv
      , SAFE_DIVIDE(sum(mov_eur), sum(gfv_eur)) as mov_over_afv
      , SAFE_DIVIDE(sum(commission_eur), sum(gfv_eur)) as comm_rate

      FROM add_calculated_fields
      WHERE TRUE
      AND has_subscription = FALSE
      AND is_qcommerce = FALSE
      GROUP BY 1,2,3
    )

    , get_median as (
      SELECT *
      , PERCENTILE_CONT(mov_over_afv, 0.5) OVER() AS median_mov_over_afv
      , PERCENTILE_CONT(comm_rate, 0.5) OVER() AS median_com_rate
      FROM aggregate_quarter_performance
    )

    , add_market_status as (
      SELECT *
      , IFNULL(cf_over_afv < 0.07, TRUE) AS has_low_cf_over_afv
      , IFNULL(mov_over_afv < median_mov_over_afv,TRUE) as has_low_mov_over_afv
      , comm_rate > median_com_rate AS has_high_comm_rate
      FROM get_median
    )

  ############################################ 


    SELECT *
    FROM add_market_status