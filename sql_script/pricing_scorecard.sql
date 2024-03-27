######################### LOGS #########################
  -- Martin Fourcade run the query on 13 Mar. 2024 with the following changes:
    -- Backfilled 1 Year data.
    -- Added affordability index and returned to previous query on served population.
    -- Profit calculated directly from DPS sessions mapped to orders.
    -- Query will run monthly without backfilling. 
  -- Martin Fourcade: 15 Mar. 2024:
    -- Changed scores calculations
    -- Fixed bug for duplicated data for some entity-months combinations
    -- Backfilled the table
  -- MF: 26 Mar. 2024:
    -- Back to previous calculations for populations CTE.
    -- Added pme and MOV/Main Dish KPI.
    -- Backfilled data for all of the entities.
    -- Corrected Score calculations taking into account only non null values.

######################### INPUTS

  ########### DECLARE VARIABLES
  DECLARE start_date_filter, end_date_filter DATE;
  DECLARE backfill BOOL;


  ######## SET RUN MODE
  SET backfill = TRUE;

  ######## SET END DATE
  SET end_date_filter = CURRENT_DATE();

  ######## SET PARTITION DATE
  IF backfill THEN
      SET start_date_filter = DATE_SUB("2023-01-01", interval 0 DAY);
  ELSE
      SET start_date_filter = DATE_TRUNC(DATE_SUB(end_date_filter, interval 2 MONTH), MONTH);
  END IF;
#####################################

##################################### STAGING TABLE 


  CREATE TEMP TABLE staging_table AS (
    with
      categories AS (
      SELECT
      entity_id,
        date AS month,
        country_categorisation,
      FROM `logistics-data-storage-staging.long_term_pricing.market_archetypes`
        WHERE date >= DATE_SUB(start_date_filter, INTERVAL 2 DAY) 
          AND date < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
      )

,population AS ( ##### With this way of calculating the fields, the underlying data works fine, otherwise it breaks due to duplicated rows in the upstream tables
SELECT
    entity_id,
    cp.month,
    (cp.total_population) AS total_population,
    (cp.active_zones_population) AS active_zones_population,
    (cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) AS addressable_population,
    (cp.active_zones_population / cp.total_population) AS active_zones_population_share,
    (i.internet_access_perc) AS internet_access_perc,
    (a.population_ages_15_64_perc) AS population_ages_15_64_perc,
    (cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) / (cp.total_population) AS addressable_population_perc
FROM `logistics-data-storage-staging.temp_pricing.country_served_population` cp
LEFT JOIN `logistics-data-storage-staging.temp_pricing.internet_access_share` i USING (country_code)
LEFT JOIN `logistics-data-storage-staging.temp_pricing.active_population_share` a USING (country_code)
WHERE
    month BETWEEN start_date_filter AND end_date_filter
    AND dh_country_code <> 't5' -- TR restaurant fleet is t3, dmart fleet is t5

)

,total_orders AS (
SELECT
    o.global_entity_id AS entity_id,
    date_trunc(o.partition_date_local, MONTH) AS month,
    COUNT(DISTINCT order_id) AS orders
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders` o
  WHERE 
    o.partition_date_local >=  DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND end_date_filter < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
    AND o.is_successful
    AND o.vertical_type IN ('restaurants', 'coffee', 'restaurant', 'street_food') 
  GROUP BY 1,2
)

,main_dish as (
  SELECT
  global_entity_id entity_id,
  report_month month,
  SAFE_DIVIDE(SUM(avg_unit_price*confirmed_orders),SUM(confirmed_orders)) AS main_dish
  FROM
    `fulfillment-dwh-production.curated_data_shared_intl_markets.affordable_supply_index` a
  WHERE
    index_class = 'Index_OK'
    AND report_month BETWEEN start_date_filter AND end_date_filter
  GROUP BY
    1,
    2
)

,kpi AS (
  SELECT
    o.region,
    o.entity_id,
    o.country_code,
    date_trunc(created_date_local, MONTH) month,
    CASE WHEN c.country_categorisation in ('Strong Leadership', 'Leadership', 'Very strong leadership') THEN "Leadership"
    WHEN c.country_categorisation in ('Head to Head', 'Lagging') THEN "Challenger" END AS market_archetype,
    p.addressable_population,
    p.total_population,
    p.active_zones_population,
    p.active_zones_population_share,
    t.orders orders,
    r.share_of_responses,
    m.main_dish,
    pme.pme,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN platform_order_code END) AS od_restaurant_orders, #IS OD filter may be removed
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN platform_order_code END)/t.orders AS od_rest_orders_share,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN analytical_customer_id END) od_restaurant_users,
    COUNT(DISTINCT analytical_customer_id )/p.addressable_population user_penetration,
    COUNT(DISTINCT CASE WHEN IFNULL(profit_eur, 0) >= 0 THEN platform_order_code END) AS profitable_orders,
    COUNT(DISTINCT CASE WHEN IFNULL(profit_eur, 0) >= 0 THEN platform_order_code END)/COUNT(DISTINCT platform_order_code) AS profitable_orders_share,
    SUM(gfv_eur)/COUNT(DISTINCT platform_order_code) afv_eur,
    SUM(delivery_fee_eur)/COUNT(DISTINCT platform_order_code) df_eur,
    SUM(dps_delivery_fee_eur)/COUNT(DISTINCT platform_order_code) dps_df_eur,
    SUM(dps_surge_fee_eur)/COUNT(DISTINCT platform_order_code) surge_fee_eur,
    SUM(service_fee_eur)/COUNT(DISTINCT platform_order_code) sf_eur,
    SUM(mov_customer_fee_eur)/COUNT(DISTINCT platform_order_code) sbf_eur,
    SAFE_DIVIDE(SUM(IFNULL(delivery_fee_eur,0) + IFNULL(service_fee_eur,0) + IFNULL(mov_customer_fee_eur,0)+ IFNULL(priority_fee_eur,0)),COUNT(DISTINCT platform_order_code)) cf_eur,
    SUM(dps_minimum_order_value_eur)/COUNT(DISTINCT platform_order_code) mov_eur,
    SUM(commission_eur)/COUNT(DISTINCT platform_order_code) comm_eur,
    SUM(joker_vendor_fee_eur)/COUNT(DISTINCT platform_order_code) joker_eur,
    SUM(delivery_costs_eur)/COUNT(DISTINCT platform_order_code) cpo_eur,
    SUM(discount_dh_eur)/COUNT(DISTINCT platform_order_code) + sum(voucher_dh_eur)/COUNT(DISTINCT platform_order_code) incentives_dh_eur,
    COUNT( DISTINCT cASe when dps_delivery_fee_local=0 and vendor_price_scheme_type != "Campaign" AND NOT has_new_customer_condition THEN platform_order_code END) AS free_df_scheme_orders,
    COUNT( DISTINCT cASe when dps_delivery_fee_local=0 and vendor_price_scheme_type != "Campaign" AND NOT has_new_customer_condition THEN platform_order_code END)/COUNT(DISTINCT platform_order_code) AS free_df_scheme_share,
    COUNT( DISTINCT cASe when vendor_price_scheme_type = "Campaign" THEN platform_order_code END) AS campaign_orders,
    COUNT( DISTINCT cASe when vendor_price_scheme_type = "Campaign" THEN platform_order_code END)/COUNT(DISTINCT platform_order_code) AS campaign_orders_share,
    CASE WHEN region ="Americas" and o.entity_id in ("PY_CL", "PY_DO","PY_EC", "PY_SV", "PY_CR","PY_GT") THEN avg(weighted_df_diff_ubereats)
    WHEN region="Americas" and o.entity_id not in ("PY_CL", "PY_DO","PY_EC", "PY_SV", "PY_CR","PY_GT") THEN avg(weighted_df_diff_rappi)
    WHEN region="Europe" THEN avg(weighted_df_diff_vs_comp)
    WHEN region="Asia" THEN avg(weighted_df_diff_grab)
    ELSE null END AS weighted_df_diff_vs_comp,
    SAFE_DIVIDE(SUM(CASE WHEN pme.pme IS NULL THEN NULL WHEN delivery_fee_local < pme.pme THEN 1 ELSE 0 END),SUM(1)) share_below_pme,
    SAFE_DIVIDE(avg(dps_minimum_order_value_local),main_dish) AS mov_main_dish

    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
    LEFT JOIN categories c
    on o.entity_id = c.entity_id and date_trunc(created_date_local, month) = c.month
    LEFT JOIN population p on o.entity_id = p.entity_id and date_trunc(created_date_local, month) = p.month
    LEFT JOIN total_orders t on o.entity_id = t.entity_id and date_trunc(created_date_local, month)= t.month
    LEFT JOIN main_dish m on m.entity_id = o.entity_id and m.month = date_trunc(created_date_local, month)
    LEFT JOIN `logistics-data-storage-staging.temp_pricing.latam_competiton` lat on o.entity_id = lat.country_code and date_trunc(created_date_local, month) = lat.month
    LEFT JOIN `logistics-data-storage-staging.temp_pricing.europe_competiton` eu on o.country_code = eu.country_code and date_trunc(created_date_local, month) = eu.month
    LEFT JOIN `logistics-data-storage-staging.temp_pricing.apac_competiton` ap on o.country_code = ap.country_code and date_trunc(created_date_local, month) = ap.month
    LEFT JOIN `logistics-data-storage-staging.temp_pricing.churn_responses` r on o.entity_id = r.entity_id and date_trunc(created_date_local, month) = r.month
    LEFT JOIN `logistics-data-storage-staging.long_term_pricing.point_of_marginal_expensiveness` pme
    ON o.country_code = pme.country_code AND DATE_TRUNC(o.created_date_local, month) = pme.month
    WHERE  o.created_date_local BETWEEN start_date_filter AND end_date_filter
    AND o.created_date>= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND is_sent
    -- AND (o.vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants')
    -- OR  o.vendor_vertical_parent is null)
    AND o.vertical_type IN ('restaurants', 'restaurant', 'coffee', 'street_food')
    AND o.entity_id != "FP_DE" 

    GROUP BY ALL
    ORDER BY 1,4 desc,2
)

,pm AS (
    SELECT
    p.entity_id,
    date_trunc(created_date_local, MONTH) AS month,
    ### QTY ###
    COUNT(platform_order_code) AS order_qty,
      COUNT(CASE WHEN order_price_mechanisms.is_dbdf THEN platform_order_code END) AS dbdf_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_fleet_delay THEN platform_order_code END) AS surge_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_bASket_value_deal THEN platform_order_code END) AS basket_value_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_service_fee THEN platform_order_code END) AS service_fee_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_small_order_fee THEN platform_order_code END) AS small_order_fee_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_dbmov THEN platform_order_code END) AS variable_mov_orders,
      COUNT(CASE WHEN order_price_mechanisms.is_surge_mov THEN platform_order_code END) AS surge_mov_orders,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code END) AS tod_orders,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code END) AS fdnc_orders,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code END) AS customer_location_orders,
      COUNT(CASE WHEN only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" THEN platform_order_code END) AS flat_mov_orders,
      COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_COUNT >= 4 THEN platform_order_code END) AS multiple_pm_orders,
      COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_COUNT = 3 THEN platform_order_code END) AS triple_pm_orders,
      COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_COUNT = 2 THEN platform_order_code END) AS double_pm_orders,
      COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_COUNT = 1 THEN platform_order_code END) AS single_pm_orders,
      ### SHARES #### 
      COUNT(CASE WHEN order_price_mechanisms.is_dbdf THEN platform_order_code END)/COUNT(platform_order_code) AS dbdf_share,
      COUNT(CASE WHEN order_price_mechanisms.is_fleet_delay THEN platform_order_code END)/COUNT(platform_order_code) AS surge_share,
      COUNT(CASE WHEN order_price_mechanisms.is_bASket_value_deal THEN platform_order_code END)/COUNT(platform_order_code) AS basket_value_share,
      COUNT(CASE WHEN order_price_mechanisms.is_service_fee THEN platform_order_code END)/COUNT(platform_order_code) AS service_fee_share,
      COUNT(CASE WHEN order_price_mechanisms.is_small_order_fee THEN platform_order_code END)/COUNT(platform_order_code) AS small_order_fee_share,
      COUNT(CASE WHEN order_price_mechanisms.is_dbmov THEN platform_order_code END) / COUNT(platform_order_code) AS variable_mov_share,
      COUNT(CASE WHEN order_price_mechanisms.is_surge_mov THEN platform_order_code END)/COUNT(platform_order_code) AS surge_mov_share,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code END)/COUNT(platform_order_code) AS tod_share,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code END)/COUNT(platform_order_code) AS fdnc_share,
      COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code END)/COUNT(platform_order_code) AS customer_location_share,
      COUNT(CASE WHEN vendor_price_scheme_type='Experiment' THEN platform_order_code END) AS test_orders, #cambiar de ordenes a usuarios y vendedores expuestos al test
      COUNT(CASE when vendor_price_scheme_type='Experiment' THEN platform_order_code END)/COUNT(platform_order_code) AS test_orders_share
    
    FROM `fulfillment-dwh-production.cl._dps_orders_with_pricing_mechanism` p
    WHERE  created_date_local  BETWEEN start_date_filter AND end_date_filter
    AND p.created_date>= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND is_own_delivery IS TRUE
    AND entity_id is not null 
    AND region is not null
    AND entity_id != 'FP_DE'
    AND vertical_type IN ('restaurants', 'restaurant', 'coffee', 'street_food') 
    GROUP BY ALL
)

,tests AS (
    SELECT
      entity_id,
      date(date_trunc(test_start_date, MONTH)) month,
      COUNT(DISTINCT CASE WHEN is_test_config_good = TRUE THEN test_name ELSE null END) AS test_all_valid_tests,#porcentaje de tiempo de test activos
      COUNT(DISTINCT test_name) AS test_all_entities_count_cum #cantidad distinta de test activos
    FROM `fulfillment-dwh-production.cl._dps_experiment_configuration_versions` t
    WHERE TRUE
    AND date(t.test_start_date) >= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND date(t.test_start_date) < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
    AND parent_vertical_flags.is_restaurant
    GROUP BY ALL
)

,avg_kpi AS (
  SELECT
    kpi.entity_id,
    kpi.month,
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_region,SUM(orders) OVER agg_region) AS profitable_orders_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders)OVER agg_region,SUM(orders) OVER agg_region) AS od_rest_orders_reg_threshold,
    SAFE_DIVIDE(SUM(weighted_df_diff_vs_comp*orders)OVER agg_region,SUM(orders) OVER agg_region) AS avg_diff_comp_reg_threshold,
    SAFE_DIVIDE(SUM(share_of_responses*orders)OVER agg_region,SUM(orders) OVER agg_region) AS churn_responses_reg_threshold,
    SAFE_DIVIDE(SUM(share_below_pme*orders)OVER agg_region,SUM(orders) OVER agg_region) AS share_below_pme_reg_threshold,
    SAFE_DIVIDE(SUM(mov_main_dish*orders)OVER agg_region,SUM(orders) OVER agg_region) AS mov_main_dish_reg_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_region,SUM(total_population) OVER agg_region)  AS active_zones_pop_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_region,SUM(addressable_population) OVER agg_region) AS user_penetration_reg_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders) OVER agg_region,SUM(orders) OVER agg_region)  AS free_df_orders_reg_threshold,
    SAFE_DIVIDE(SUM(campaign_orders) OVER agg_region,SUM(orders) OVER agg_region)  AS campaign_orders_reg_threshold,
    AVG(afv_eur) OVER agg_region AS afv_reg_threshold,
    SAFE_DIVIDE(SUM(df_eur) OVER agg_region, SUM(afv_eur) OVER agg_region) AS df_afv_reg_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END) OVER agg_region ,SUM(df_eur) OVER agg_region)  AS surge_df_reg_threshold,
    SAFE_DIVIDE(SUM(sf_eur) OVER agg_region,SUM(CASE WHEN cf_eur>0 THEN cf_eur else 0 END) OVER agg_region)  AS sf_cf_reg_threshold,
    SAFE_DIVIDE(SUM(mov_eur) OVER agg_region,SUM(CASE WHEN afv_eur>0 THEN afv_eur else 0 END) OVER agg_region) AS mov_afv_reg_threshold,
    SAFE_DIVIDE(SUM(sbf_eur) OVER agg_region,SUM(cf_eur) OVER agg_region)  AS sbf_cf_reg_threshold,
    AVG(od_restaurant_users) OVER agg_region AS active_users_reg_threshold, 
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_region,SUM(od_restaurant_users) OVER agg_region) AS frequency_reg_threshold,
    SAFE_DIVIDE(SUM(test_all_valid_tests)OVER agg_region,SUM(test_all_entities_count_cum) OVER agg_region)  AS valid_tests_reg_threshold,
    SAFE_DIVIDE(SUM(test_orders) OVER agg_region, SUM(orders) OVER agg_region) AS total_tests_reg_threshold,
   
    ##Market archetype thresholds ##
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_archetype,SUM(orders) OVER agg_archetype) AS profitable_orders_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_archetype,SUM(orders) OVER agg_archetype) AS od_rest_orders_ma_threshold,
    SAFE_DIVIDE(SUM(weighted_df_diff_vs_comp*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) AS avg_diff_comp_ma_threshold,
    SAFE_DIVIDE(SUM(share_of_responses*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) AS churn_responses_ma_threshold,
    SAFE_DIVIDE(SUM(share_below_pme*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) AS share_below_pme_ma_threshold,
    SAFE_DIVIDE(SUM(mov_main_dish*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) AS mov_main_dish_ma_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_archetype,SUM(total_population) OVER agg_archetype) AS  active_zones_pop_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_archetype,SUM(addressable_population) OVER agg_archetype)  AS user_penetration_ma_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders)  OVER agg_archetype,SUM(orders) OVER agg_archetype) AS free_df_orders_ma_threshold,
    SAFE_DIVIDE(SUM(campaign_orders)  OVER agg_archetype,SUM(orders)  OVER agg_archetype) AS campaign_orders_ma_threshold,
    AVG(afv_eur) OVER agg_archetype AS afv_ma_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END)  OVER agg_archetype,SUM(df_eur) OVER agg_archetype)  AS surge_df_ma_threshold,
    SAFE_DIVIDE(SUM(df_eur)  OVER agg_archetype,SUM(afv_eur) OVER agg_archetype) AS df_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sf_eur)  OVER agg_archetype,SUM(CASE WHEN cf_eur>0 THEN cf_eur else 0 END) OVER agg_archetype) AS sf_cf_ma_threshold,
    SAFE_DIVIDE(SUM(mov_eur)  OVER agg_archetype,SUM(CASE WHEN afv_eur>0 THEN afv_eur else 0 END) OVER agg_archetype) AS mov_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sbf_eur)  OVER agg_archetype,SUM(CASE WHEN cf_eur>0 THEN cf_eur else 0 END) OVER agg_archetype) AS sbf_cf_ma_threshold,
    AVG(od_restaurant_users) OVER agg_archetype AS active_users_ma_threshold, 
    SAFE_DIVIDE(SUM(od_restaurant_orders)OVER agg_archetype,SUM(od_restaurant_users) OVER agg_archetype)  AS frequency_ma_threshold,
    SAFE_DIVIDE(SUM(test_all_valid_tests) OVER agg_archetype,SUM(test_all_entities_count_cum) OVER agg_archetype) AS valid_tests_ma_threshold,
     SAFE_DIVIDE(SUM(test_orders) OVER agg_archetype, SUM(orders) OVER agg_archetype) AS total_tests_ma_threshold,
    from kpi
    left join tests t on kpi.month = t.month and kpi.entity_id = t.entity_id
    LEFT JOIN pm on kpi.month = pm.month and kpi.entity_id = pm.entity_id
     WINDOW agg_region AS (PARTITION BY kpi.region, kpi.month)
   , agg_archetype AS (PARTITION BY kpi.market_archetype, kpi.month)
)

,kpi_score AS (
  SELECT
   kpi.*,
   ## Region scores
   ### Profitability
   CASE WHEN profitable_orders_share >= profitable_orders_reg_threshold THEN 1 WHEN profitable_orders_share IS NOT NULL THEN 0 END AS profitable_orders_score,
   CASE WHEN free_df_scheme_share <= free_df_orders_reg_threshold THEN 1 WHEN free_df_scheme_share IS NOT NULL THEN 0 END AS free_df_scheme_score,
   CASE WHEN campaign_orders_share <= campaign_orders_reg_threshold THEN 1 WHEN campaign_orders_share IS NOT NULL THEN 0 END AS campaign_orders_score,
   CASE WHEN SAFE_DIVIDE(mov_eur, afv_eur) >= mov_afv_reg_threshold THEN 1 WHEN afv_eur IS NOT NULL THEN 0 END AS mov_afv_score,
   CASE WHEN SAFE_DIVIDE(surge_fee_eur, df_eur) <= surge_df_reg_threshold THEN 1 WHEN df_eur IS NOT NULL THEN 0 END AS surge_df_score,
   CASE WHEN SAFE_DIVIDE(sbf_eur, cf_eur) >= sbf_cf_reg_threshold THEN 1 WHEN cf_eur IS NOT NULL THEN 0 END AS sbf_cf_score,
   CASE WHEN SAFE_DIVIDE(sf_eur, cf_eur) >= sf_cf_reg_threshold THEN 1 WHEN cf_eur IS NOT NULL THEN 0 END AS sf_cf_score,
   CASE WHEN afv_eur >= afv_reg_threshold THEN 1 WHEN afv_eur IS NOT NULL THEN 0 END AS afv_eur_score,
   ### Affordability
   CASE WHEN weighted_df_diff_vs_comp <= avg_diff_comp_reg_threshold THEN 1 WHEN weighted_df_diff_vs_comp IS NOT NULL THEN 0 END AS diff_df_comp_score,
   CASE WHEN share_of_responses <= churn_responses_reg_threshold THEN 1 WHEN share_of_responses IS NOT NULL THEN 0 END AS churn_responses_score,
   CASE WHEN share_below_pme >= share_below_pme_reg_threshold THEN 1 WHEN share_below_pme IS NOT NULL THEN 0 END AS share_below_pme_score,
   CASE WHEN mov_main_dish >= mov_main_dish_reg_threshold THEN 1 WHEN mov_main_dish IS NOT NULL THEN 0 END AS mov_main_dish_score,
   ### Penetration
   CASE WHEN od_rest_orders_share>= od_rest_orders_reg_threshold THEN 1 WHEN od_rest_orders_share IS NOT NULL THEN 0 END AS od_rest_orders_score,
   CASE WHEN active_zones_population_share >= active_zones_pop_reg_threshold THEN 1 WHEN active_zones_population_share IS NOT NULL THEN 0 END AS active_zones_score,
   CASE WHEN user_penetration >= user_penetration_reg_threshold THEN 1 WHEN user_penetration IS NOT NULL THEN 0 END AS user_penetration_score,
   CASE WHEN SAFE_DIVIDE(od_restaurant_orders, od_restaurant_users) >= frequency_reg_threshold THEN 1 WHEN od_restaurant_users IS NOT NULL THEN 0 END AS frequency_score,
   CASE WHEN od_restaurant_users >= active_users_reg_threshold THEN 1 WHEN od_restaurant_users IS NOT NULL THEN 0 END AS active_users_score,
   ### Experimentation
   CASE WHEN SAFE_DIVIDE(test_all_valid_tests, test_all_entities_count_cum) >= valid_tests_reg_threshold THEN 1 WHEN test_all_entities_count_cum IS NOT NULL THEN 0 END AS valid_tests_score,
   CASE WHEN test_orders_share >= total_tests_reg_threshold THEN 1 WHEN test_all_entities_count_cum IS NOT NULL THEN 0 END AS total_tests_score,
   ## Market archetypes scores
   ### Profitability
   CASE WHEN profitable_orders_share >= profitable_orders_ma_threshold THEN 1 WHEN profitable_orders_share IS NOT NULL THEN 0 END AS profitable_orders_ma_score,
   CASE WHEN free_df_scheme_share <= free_df_orders_ma_threshold THEN 1 WHEN free_df_scheme_share IS NOT NULL THEN 0 END AS free_df_scheme_ma_score,
   CASE WHEN campaign_orders_share <= campaign_orders_ma_threshold THEN 1 WHEN campaign_orders_share IS NOT NULL THEN 0 END AS campaign_orders_ma_score,
   CASE WHEN SAFE_DIVIDE(mov_eur, afv_eur) >= mov_afv_ma_threshold THEN 1 WHEN afv_eur IS NOT NULL THEN 0 END AS mov_afv_ma_score,
   CASE WHEN SAFE_DIVIDE(surge_fee_eur, df_eur) <= surge_df_ma_threshold THEN 1 WHEN df_eur IS NOT NULL THEN 0 END AS surge_df_ma_score,
   CASE WHEN SAFE_DIVIDE(sbf_eur, cf_eur) >= sbf_cf_ma_threshold THEN 1 WHEN cf_eur IS NOT NULL THEN 0 END AS sbf_cf_ma_score,
   CASE WHEN SAFE_DIVIDE(sf_eur, cf_eur) >= sf_cf_ma_threshold THEN 1 WHEN cf_eur IS NOT NULL THEN 0 END AS sf_cf_ma_score,
   CASE WHEN afv_eur >= afv_ma_threshold THEN 1 WHEN afv_eur IS NOT NULL THEN 0 END AS afv_eur_ma_score,
   ### Affordability
   CASE WHEN weighted_df_diff_vs_comp <= avg_diff_comp_ma_threshold THEN 1 WHEN weighted_df_diff_vs_comp IS NOT NULL THEN 0 END AS diff_df_comp_ma_score,
   CASE WHEN share_of_responses <= churn_responses_ma_threshold THEN 1 WHEN share_of_responses IS NOT NULL THEN 0 END AS churn_responses_ma_score,
   CASE WHEN share_below_pme >= share_below_pme_ma_threshold THEN 1 WHEN share_below_pme IS NOT NULL THEN 0 END AS share_below_pme_ma_score,
   CASE WHEN mov_main_dish >= mov_main_dish_ma_threshold THEN 1 WHEN mov_main_dish IS NOT NULL THEN 0 END AS mov_main_dish_ma_score,
   ### Penetration
   CASE WHEN od_rest_orders_share >= od_rest_orders_ma_threshold THEN 1 WHEN od_rest_orders_share IS NOT NULL THEN 0 END AS od_rest_orders_ma_score,
   CASE WHEN active_zones_population_share >= active_zones_pop_ma_threshold THEN 1 WHEN active_zones_population_share IS NOT NULL THEN 0 END AS active_zones_ma_score,
   CASE WHEN user_penetration >= user_penetration_ma_threshold THEN 1 WHEN user_penetration IS NOT NULL THEN 0 END AS user_penetration_ma_score,
   CASE WHEN SAFE_DIVIDE(od_restaurant_orders, od_restaurant_users) >= frequency_ma_threshold THEN 1 WHEN od_restaurant_users IS NOT NULL THEN 0 END AS frequency_ma_score,
   CASE WHEN od_restaurant_users >= active_users_ma_threshold THEN 1 WHEN od_restaurant_users IS NOT NULL THEN 0 END AS active_users_ma_score,
   ### Experimentation
   CASE WHEN SAFE_DIVIDE(test_all_valid_tests, test_all_entities_count_cum) >= valid_tests_ma_threshold THEN 1 WHEN test_all_entities_count_cum IS NOT NULL THEN 0 END AS valid_tests_ma_score,
   CASE WHEN test_orders_share >= total_tests_ma_threshold THEN 1 WHEN test_all_entities_count_cum IS NOT NULL THEN 0 END AS total_tests_ma_score,
   ## Sophistication scores
   CASE WHEN dbdf_share >= 0.5 THEN 1 WHEN dbdf_share IS NOT NULL THEN 0 END AS dbdf_score,
   CASE WHEN surge_share >= 0.5 THEN 1 WHEN surge_share IS NOT NULL THEN 0 END AS surge_score,
   CASE WHEN service_fee_share >= 0.5 THEN 1 WHEN service_fee_share IS NOT NULL THEN 0 END AS service_fee_score,
   CASE WHEN basket_value_share >= 0.5 THEN 1 WHEN basket_value_share IS NOT NULL THEN 0 END AS basket_value_score,
   CASE WHEN small_order_fee_share >= 0.5 THEN 1 WHEN small_order_fee_share IS NOT NULL THEN 0 END AS small_order_fee_score,
   CASE WHEN variable_mov_share >= 0.5 THEN 1 WHEN variable_mov_share IS NOT NULL THEN 0 END AS variable_mov_score,
   CASE WHEN surge_mov_share >= 0.5 THEN 1 WHEN surge_mov_share IS NOT NULL THEN 0 END AS surge_mov_score,
   CASE WHEN tod_share >= 0.5 THEN 1 WHEN tod_share IS NOT NULL THEN 0 END AS tod_score,
   CASE WHEN fdnc_share >= 0.5 THEN 1 WHEN fdnc_share IS NOT NULL THEN 0 END AS fdnc_score,
   CASE WHEN customer_location_share >= 0.5 THEN 1 WHEN customer_location_share IS NOT NULL THEN 0 END AS customer_location_score,
   ## Thresholds
   avg_kpi.* EXCEPT(entity_id, month)
  FROM kpi
  LEFT JOIN avg_kpi USING (entity_id, month)
  LEFT JOIN tests t USING (month, entity_id)
  LEFT JOIN pm USING (month, entity_id)
)

SELECT
    kpi.*,
    -- Todas las columnas de la primera subconsulta
    pm.* except(entity_id, month),  -- Todas las columnas de la segunda subconsulta
    --penetration.* except(entity_id, month),  -- Todas las columnas de la tercera subconsulta
    tests.* except(entity_id, month),  -- Todas las columnas de la cuarta subconsulta
    ### SCORES #####
  --  (profitable_orders_score+ free_df_scheme_score + campaign_orders_score + mov_afv_score +surge_df_score +sbf_cf_score +sf_cf_score)/7 as profitability_score,
  --  (ifnull(diff_df_comp_score,0) + ifnull(churn_responses_score,0) + ifnull(share_below_pme_score,0) + ifnull(mov_main_dish_score,0))/4 as affordability_score,
  -- (od_rest_orders_score+frequency_score+user_penetration_score)/3 as penetration_score,
  -- (valid_tests_score+total_tests_score)/2 experimentation_score,
  -- (profitable_orders_ma_score + free_df_scheme_ma_score + campaign_orders_ma_score+ mov_afv_ma_score+ surge_df_ma_score+ sbf_cf_ma_score+ sf_cf_ma_score)/7 as profitability_ma_score,
  -- (ifnull(diff_df_comp_ma_score,0) + ifnull(churn_responses_ma_score,0) + ifnull(share_below_pme_ma_score,0) + ifnull(mov_main_dish_ma_score,0))/4 as affordability_ma_score,
  -- (od_rest_orders_ma_score + frequency_ma_score + user_penetration_ma_score)/3 as penetration_ma_score,
  -- (valid_tests_ma_score + total_tests_ma_score)/2 experimentation_ma_score,
  -- (dbdf_score + surge_score + service_fee_score + basket_value_score + small_order_fee_score + variable_mov_score + surge_mov_score + tod_score + fdnc_score+ customer_location_score)/10 as sophistication_score

    ### Regional Dimension Scores
    SAFE_DIVIDE((IFNULL(profitable_orders_score,0) + IFNULL(free_df_scheme_score,0) + IFNULL(campaign_orders_score,0) + IFNULL(mov_afv_score,0) + IFNULL(surge_df_score,0) + IFNULL(sbf_cf_score,0) + IFNULL(sf_cf_score,0)),
    (IF(profitable_orders_score IS NOT NULL,1,0) + IF(free_df_scheme_score IS NOT NULL,1,0) + IF(campaign_orders_score IS NOT NULL,1,0) + IF(mov_afv_score IS NOT NULL,1,0) + IF(surge_df_score IS NOT NULL,1,0) + IF(sbf_cf_score IS NOT NULL,1,0) + IF(sf_cf_score IS NOT NULL,1,0))) AS profitability_score,
    SAFE_DIVIDE((IFNULL(diff_df_comp_score,0) + IFNULL(churn_responses_score,0) + IFNULL(share_below_pme_score,0) + IFNULL(mov_main_dish_score,0)),
    IF(diff_df_comp_score IS NOT NULL,1,0) + IF(churn_responses_score IS NOT NULL,1,0) + IF(share_below_pme_score IS NOT NULL,1,0)+ IF(mov_main_dish_score IS NOT NULL,1,0)) AS affordability_score,
    SAFE_DIVIDE((IFNULL(od_rest_orders_score,0) + IFNULL(frequency_score,0) + IFNULL(user_penetration_score,0)),
    (IF(od_rest_orders_score IS NOT NULL,1,0) + IF(frequency_score IS NOT NULL,1,0) + IF(user_penetration_score IS NOT NULL,1,0))) AS penetration_score,
    SAFE_DIVIDE((IFNULL(valid_tests_score,0) + IFNULL(total_tests_score,0)),
    (IF(valid_tests_score IS NOT NULL,1,0) + IF(total_tests_score IS NOT NULL,1,0))) AS experimentation_score,
    
    ### Market Archetype Dimension Scores
    SAFE_DIVIDE((IFNULL(profitable_orders_ma_score,0) + IFNULL(free_df_scheme_ma_score,0) + IFNULL(campaign_orders_ma_score,0) + IFNULL(mov_afv_ma_score,0) + IFNULL(surge_df_ma_score,0) + IFNULL(sbf_cf_ma_score,0) + IFNULL(sf_cf_ma_score,0)),
    (IF(profitable_orders_ma_score IS NOT NULL,1,0) + IF(free_df_scheme_ma_score IS NOT NULL,1,0) + IF(campaign_orders_ma_score IS NOT NULL,1,0) + IF(mov_afv_ma_score IS NOT NULL,1,0) + IF(surge_df_ma_score IS NOT NULL,1,0) + IF(sbf_cf_ma_score IS NOT NULL,1,0) + IF(sf_cf_ma_score IS NOT NULL,1,0))) AS profitability_ma_score,
    SAFE_DIVIDE((IFNULL(diff_df_comp_ma_score,0) + IFNULL(churn_responses_ma_score,0) + IFNULL(share_below_pme_ma_score,0) + IFNULL(mov_main_dish_ma_score,0)),
    (IF(diff_df_comp_ma_score IS NOT NULL,1,0) + IF(churn_responses_ma_score IS NOT NULL,1,0) + IF(share_below_pme_ma_score IS NOT NULL,1,0) + IF(mov_main_dish_ma_score IS NOT NULL,1,0))) AS affordability_ma_score,
    SAFE_DIVIDE((IFNULL(od_rest_orders_ma_score,0) + IFNULL(frequency_ma_score,0) + IFNULL(user_penetration_ma_score,0)),
    (IF(od_rest_orders_ma_score IS NOT NULL,1,0) + IF(frequency_ma_score IS NOT NULL,1,0) + IF(user_penetration_ma_score IS NOT NULL,1,0))) AS penetration_ma_score,
    SAFE_DIVIDE((IFNULL(valid_tests_ma_score,0) + IFNULL(total_tests_ma_score,0)),
    (IF(valid_tests_ma_score IS NOT NULL,1,0) + IF(total_tests_ma_score IS NOT NULL,1,0))) AS experimentation_ma_score,
    SAFE_DIVIDE((dbdf_score + surge_score + service_fee_score + basket_value_score + small_order_fee_score + variable_mov_score + surge_mov_score + tod_score + fdnc_score + customer_location_score),
    (IF(dbdf_score IS NOT NULL,1,0) + IF(surge_score IS NOT NULL,1,0) + IF(service_fee_score IS NOT NULL,1,0) + IF(basket_value_score IS NOT NULL,1,0) + IF(small_order_fee_score IS NOT NULL,1,0) + IF(variable_mov_score IS NOT NULL,1,0) + IF(surge_mov_score IS NOT NULL,1,0) + IF(tod_score IS NOT NULL,1,0) + IF(fdnc_score IS NOT NULL,1,0) + IF(customer_location_score IS NOT NULL,1,0))) AS sophistication_score
  FROM
  kpi_score AS kpi
  LEFT JOIN pm USING(entity_id, month)
  LEFT JOIN tests USING(entity_id, month)
  Order by 1,2 asc, 4 desc
);

###################################

################################### UPSERT
  IF backfill THEN
      CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_scorecard`
      PARTITION by month
      OPTIONS(
        partition_expiration_days=null,
        require_partition_filter=false
      )
      AS
      SELECT * FROM staging_table;
    ELSE
      MERGE INTO `logistics-data-storage-staging.long_term_pricing.pricing_scorecard` s
      USING staging_table stg
      ON prd.entity_id = stg.entity_id
      AND prd.month = stg.month
      WHEN MATCHED THEN
        UPDATE SET
        region = stg.region
        ,entity_id = stg.entity_id
        ,country_code = stg.country_code
        ,month = stg.month
        ,market_archetype = stg.market_archetype
        ,addressable_population = stg.addressable_population
        ,total_population = stg.total_population
        ,active_zones_population = stg.active_zones_population
        ,active_zones_population_share = stg.active_zones_population_share
        ,orders = stg.orders
        ,share_of_responses = stg.share_of_responses
        ,main_dish = stg.main_dish
        ,pme = stg.pme
        ,od_restaurant_orders = stg.od_restaurant_orders
        ,od_rest_orders_share = stg.od_rest_orders_share
        ,od_restaurant_users = stg.od_restaurant_users
        ,user_penetration = stg.user_penetration
        ,profitable_orders = stg.profitable_orders
        ,profitable_orders_share = stg.profitable_orders_share
        ,afv_eur = stg.afv_eur
        ,df_eur = stg.df_eur
        ,dps_df_eur = stg.dps_df_eur
        ,surge_fee_eur = stg.surge_fee_eur
        ,sf_eur = stg.sf_eur
        ,sbf_eur = stg.sbf_eur
        ,cf_eur = stg.cf_eur
        ,mov_eur = stg.mov_eur
        ,comm_eur = stg.comm_eur
        ,joker_eur = stg.joker_eur
        ,cpo_eur = stg.cpo_eur
        ,incentives_dh_eur = stg.incentives_dh_eur
        ,free_df_scheme_orders = stg.free_df_scheme_orders
        ,free_df_scheme_share = stg.free_df_scheme_share
        ,campaign_orders = stg.campaign_orders
        ,campaign_orders_share = stg.campaign_orders_share
        ,weighted_df_diff_vs_comp = stg.weighted_df_diff_vs_comp
        ,share_below_pme = stg.share_below_pme
        ,mov_main_dish = stg.mov_main_dish
        ,profitable_orders_score = stg.profitable_orders_score
        ,free_df_scheme_score = stg.free_df_scheme_score
        ,campaign_orders_score = stg.campaign_orders_score
        ,mov_afv_score = stg.mov_afv_score
        ,surge_df_score = stg.surge_df_score
        ,sbf_cf_score = stg.sbf_cf_score
        ,sf_cf_score = stg.sf_cf_score
        ,afv_eur_score = stg.afv_eur_score
        ,diff_df_comp_score = stg.diff_df_comp_score
        ,churn_responses_score = stg.churn_responses_score
        ,share_below_pme_score = stg.share_below_pme_score
        ,mov_main_dish_score = stg.mov_main_dish_score
        ,od_rest_orders_score = stg.od_rest_orders_score
        ,active_zones_score = stg.active_zones_score
        ,user_penetration_score = stg.user_penetration_score
        ,frequency_score = stg.frequency_score
        ,active_users_score = stg.active_users_score
        ,valid_tests_score = stg.valid_tests_score
        ,total_tests_score = stg.total_tests_score
        ,profitable_orders_ma_score = stg.profitable_orders_ma_score
        ,od_rest_orders_ma_score = stg.od_rest_orders_ma_score
        ,diff_df_comp_ma_score = stg.diff_df_comp_ma_score
        ,churn_responses_ma_score = stg.churn_responses_ma_score
        ,share_below_pme_ma_score = stg.share_below_pme_ma_score
        ,mov_main_dish_ma_score = stg.mov_main_dish_ma_score
        ,active_zones_ma_score = stg.active_zones_ma_score
        ,user_penetration_ma_score = stg.user_penetration_ma_score
        ,free_df_scheme_ma_score = stg.free_df_scheme_ma_score
        ,campaign_orders_ma_score = stg.campaign_orders_ma_score
        ,afv_eur_ma_score = stg.afv_eur_ma_score
        ,surge_df_ma_score = stg.surge_df_ma_score
        ,sf_cf_ma_score = stg.sf_cf_ma_score
        ,mov_afv_ma_score = stg.mov_afv_ma_score
        ,sbf_cf_ma_score = stg.sbf_cf_ma_score
        ,active_users_ma_score = stg.active_users_ma_score
        ,frequency_ma_score = stg.frequency_ma_score
        ,valid_tests_ma_score = stg.valid_tests_ma_score
        ,total_tests_ma_score = stg.total_tests_ma_score
        ,dbdf_score = stg.dbdf_score
        ,surge_score = stg.surge_score
        ,service_fee_score = stg.service_fee_score
        ,basket_value_score = stg.basket_value_score
        ,small_order_fee_score = stg.small_order_fee_score
        ,variable_mov_score = stg.variable_mov_score
        ,surge_mov_score = stg.surge_mov_score
        ,tod_score = stg.tod_score
        ,fdnc_score = stg.fdnc_score
        ,customer_location_score = stg.customer_location_score
        ,profitable_orders_reg_threshold = stg.profitable_orders_reg_threshold
        ,od_rest_orders_reg_threshold = stg.od_rest_orders_reg_threshold
        ,avg_diff_comp_reg_threshold = stg.avg_diff_comp_reg_threshold
        ,churn_responses_reg_threshold = stg.churn_responses_reg_threshold
        ,share_below_pme_reg_threshold = stg.share_below_pme_reg_threshold
        ,mov_main_dish_reg_threshold = stg.mov_main_dish_reg_threshold
        ,active_zones_pop_reg_threshold = stg.active_zones_pop_reg_threshold
        ,user_penetration_reg_threshold = stg.user_penetration_reg_threshold
        ,free_df_orders_reg_threshold = stg.free_df_orders_reg_threshold
        ,campaign_orders_reg_threshold = stg.campaign_orders_reg_threshold
        ,afv_reg_threshold = stg.afv_reg_threshold
        ,df_afv_reg_threshold = stg.df_afv_reg_threshold
        ,surge_df_reg_threshold = stg.surge_df_reg_threshold
        ,sf_cf_reg_threshold = stg.sf_cf_reg_threshold
        ,mov_afv_reg_threshold = stg.mov_afv_reg_threshold
        ,sbf_cf_reg_threshold = stg.sbf_cf_reg_threshold
        ,active_users_reg_threshold = stg.active_users_reg_threshold
        ,frequency_reg_threshold = stg.frequency_reg_threshold
        ,valid_tests_reg_threshold = stg.valid_tests_reg_threshold
        ,total_tests_reg_threshold = stg.total_tests_reg_threshold
        ,profitable_orders_ma_threshold = stg.profitable_orders_ma_threshold
        ,od_rest_orders_ma_threshold = stg.od_rest_orders_ma_threshold
        ,avg_diff_comp_ma_threshold = stg.avg_diff_comp_ma_threshold
        ,churn_responses_ma_threshold = stg.churn_responses_ma_threshold
        ,share_below_pme_ma_threshold = stg.share_below_pme_ma_threshold
        ,mov_main_dish_ma_threshold = stg.mov_main_dish_ma_threshold
        ,active_zones_pop_ma_threshold = stg.active_zones_pop_ma_threshold
        ,user_penetration_ma_threshold = stg.user_penetration_ma_threshold
        ,free_df_orders_ma_threshold = stg.free_df_orders_ma_threshold
        ,campaign_orders_ma_threshold = stg.campaign_orders_ma_threshold
        ,afv_ma_threshold = stg.afv_ma_threshold
        ,surge_df_ma_threshold = stg.surge_df_ma_threshold
        ,df_afv_ma_threshold = stg.df_afv_ma_threshold
        ,sf_cf_ma_threshold = stg.sf_cf_ma_threshold
        ,mov_afv_ma_threshold = stg.mov_afv_ma_threshold
        ,sbf_cf_ma_threshold = stg.sbf_cf_ma_threshold
        ,active_users_ma_threshold = stg.active_users_ma_threshold
        ,frequency_ma_threshold = stg.frequency_ma_threshold
        ,valid_tests_ma_threshold = stg.valid_tests_ma_threshold
        ,total_tests_ma_threshold = stg.total_tests_ma_threshold
        ,order_qty = stg.order_qty
        ,dbdf_orders = stg.dbdf_orders
        ,surge_orders = stg.surge_orders
        ,basket_value_orders = stg.basket_value_orders
        ,service_fee_orders = stg.service_fee_orders
        ,small_order_fee_orders = stg.small_order_fee_orders
        ,variable_mov_orders = stg.variable_mov_orders
        ,surge_mov_orders = stg.surge_mov_orders
        ,tod_orders = stg.tod_orders
        ,fdnc_orders = stg.fdnc_orders
        ,customer_location_orders = stg.customer_location_orders
        ,flat_mov_orders = stg.flat_mov_orders
        ,multiple_pm_orders = stg.multiple_pm_orders
        ,triple_pm_orders = stg.triple_pm_orders
        ,double_pm_orders = stg.double_pm_orders
        ,single_pm_orders = stg.single_pm_orders
        ,dbdf_share = stg.dbdf_share
        ,surge_share = stg.surge_share
        ,basket_value_share = stg.basket_value_share
        ,service_fee_share = stg.service_fee_share
        ,small_order_fee_share = stg.small_order_fee_share
        ,variable_mov_share = stg.variable_mov_share
        ,surge_mov_share = stg.surge_mov_share
        ,tod_share = stg.tod_share
        ,fdnc_share = stg.fdnc_share
        ,customer_location_share = stg.customer_location_share
        ,test_orders = stg.test_orders
        ,test_orders_share = stg.test_orders_share
        ,test_all_valid_tests = stg.test_all_valid_tests
        ,test_all_entities_count_cum = stg.test_all_entities_count_cum
        ,profitability_score = stg.profitability_score
        ,affordability_score = stg.affordability_score
        ,penetration_score = stg.penetration_score
        ,experimentation_score = stg.experimentation_score
        ,profitability_ma_score = stg.profitability_ma_score
        ,affordability_ma_score = stg.affordability_ma_score
        ,penetration_ma_score = stg.penetration_ma_score
        ,experimentation_ma_score = stg.experimentation_ma_score
        ,sophistication_score = stg.sophistication_score

        WHEN NOT MATCHED THEN
          INSERT ROW
        ;
      END IF;
#########################################
