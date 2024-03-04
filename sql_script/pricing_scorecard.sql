with
categories AS (
  SELECT
    entity_id,
    date AS month,
    country_categorisation,
  FROM `logistics-data-storage-staging.long_term_pricing.market_archetypes`
)
, population AS (
  SELECT
    CASE WHEN cp.dh_country_code = 't5' THEN 'tr' ELSE cp.dh_country_code END AS dh_country_code,
    cp.country_code,
    cp.month,
    cp.total_population,
    active_zones_population,
    (cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) AS addressable_population,
    (cp.active_zones_population / cp.total_population) AS active_zones_population_share,
    (i.internet_access_perc) AS internet_access_perc,
    (a.population_ages_15_64_perc) AS population_ages_15_64_perc,
    (cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) / (cp.total_population) AS addressable_population_perc
  FROM `logistics-data-storage-staging.temp_pricing.country_served_population` cp
  LEFT JOIN `logistics-data-storage-staging.temp_pricing.internet_access_share` i USING (country_code)
  LEFT JOIN `logistics-data-storage-staging.temp_pricing.active_population_share` a USING (country_code)
  WHERE month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND month < DATE_TRUNC(CURRENT_DATE(), MONTH)
)
, total_orders AS (
  SELECT
    o.entity_id,
    DATE_TRUNC(created_date, MONTH) month,
    COUNT(DISTINCT platform_order_code) AS orders
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
  WHERE created_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND created_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND is_sent
    AND (vertical_type IN ('restaurants', 'coffee', 'home_based_kitchen', 'street_food', 'confectionery') AND vertical_type NOT IN ('courier', 'courier_business'))
  GROUP BY 1,2
)
, kpi AS (
  SELECT
    o.region,
    o.entity_id,
    o.country_code,
    DATE_TRUNC(created_date, MONTH) month,
    CASE
      WHEN c.country_categorisation IN ('Strong Leadership', 'Leadership', 'Very strong leadership') THEN "Leadership"
      WHEN c.country_categorisation IN ('Head to Head', 'Lagging') THEN "Challenger"
    END AS market_archetype,
    p.addressable_population,
    p.total_population,
    p.active_zones_population,
    p.active_zones_population_share,
    t.orders total_orders,
    COUNT(DISTINCT platform_order_code) AS orders,
    COUNT(DISTINCT analytical_customer_id) users,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN analytical_customer_id END) od_restaurant_users,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN analytical_customer_id END) / p.addressable_population user_penetration,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN platform_order_code END) od_restaurant_orders,
    COUNT(DISTINCT CASE WHEN is_own_delivery THEN platform_order_code END) / t.orders AS od_rest_orders_share,
    COUNT(DISTINCT CASE WHEN IFNULL((delivery_fee_eur + service_fee_eur + mov_customer_fee_eur + commission_eur) - delivery_costs_eur, 0) >= 0 THEN platform_order_code END) AS profitable_orders, ## cambiar profit local
    COUNT(DISTINCT CASE WHEN IFNULL(profit_local, 0) >= 0 THEN platform_order_code END) AS profitable_orders_II, ## cambiar profit local
    COUNT(DISTINCT CASE WHEN IFNULL((delivery_fee_eur + service_fee_eur + mov_customer_fee_eur + commission_eur) - delivery_costs_eur, 0) >= 0 THEN platform_order_code END)/COUNT(DISTINCT platform_order_code) AS profitable_orders_share,
    SUM(gfv_eur) / COUNT(DISTINCT platform_order_code) afv_eur,
    SUM(delivery_fee_eur) / COUNT(DISTINCT platform_order_code) df_eur,
    SUM(dps_delivery_fee_eur) / COUNT(DISTINCT platform_order_code) dps_df_eur,
    SUM(dps_surge_fee_eur) / COUNT(DISTINCT platform_order_code) surge_fee_eur,
    SUM(service_fee_eur) / COUNT(DISTINCT platform_order_code) sf_eur,
    SUM(mov_customer_fee_eur) / COUNT(DISTINCT platform_order_code) sbf_eur,
    SAFE_DIVIDE(SUM(delivery_fee_eur + service_fee_eur + mov_customer_fee_eur), COUNT(DISTINCT platform_order_code)) cf_eur,
    SUM(dps_minimum_order_value_eur) / COUNT(DISTINCT platform_order_code) mov_eur,
    SUM(commission_eur) / COUNT(DISTINCT platform_order_code) comm_eur,
    SUM(joker_vendor_fee_eur) / COUNT(DISTINCT platform_order_code) joker_eur,
    SUM(delivery_costs_eur) / COUNT(DISTINCT platform_order_code) cpo_eur,
    SUM(discount_dh_eur) / COUNT(DISTINCT platform_order_code) + SUM(voucher_dh_eur) / COUNT(DISTINCT platform_order_code) incentives_dh_eur,
    COUNT(DISTINCT CASE WHEN dps_delivery_fee_local = 0 AND vendor_price_scheme_type != "Campaign" THEN platform_order_code END) AS free_df_scheme_orders, ## has new customer condition, ver como meter esto
    COUNT(DISTINCT CASE WHEN dps_delivery_fee_local = 0 AND vendor_price_scheme_type != "Campaign" THEN platform_order_code END) / COUNT(DISTINCT platform_order_code) AS free_df_scheme_share,
    COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" THEN platform_order_code END) AS campaign_orders,
    COUNT(DISTINCT CASE WHEN vendor_price_scheme_type = "Campaign" THEN platform_order_code END) / COUNT(DISTINCT platform_order_code) AS campaign_orders_share
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
  LEFT JOIN categories c ON DATE_TRUNC(created_date, month) = c.month AND o.entity_id = c.entity_id
  LEFT JOIN population p ON DATE_TRUNC(created_date, month) = p.month AND o.country_code = p.dh_country_code
  LEFT JOIN total_orders t ON DATE_TRUNC(created_date, month) = t.month AND o.entity_id = t.entity_id
  WHERE created_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND created_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND is_sent
    AND o.entity_id != "FP_DE"
    AND (vertical_type IN ('restaurants', 'coffee', 'home_based_kitchen', 'street_food', 'confectionery') AND vertical_type NOT IN ('courier', 'courier_business'))
  GROUP BY 1,2,3,4,5,6,7,8,9,10
)
, pm AS (
  SELECT
    p.entity_id,
    DATE_TRUNC(created_date_local, MONTH) AS month,
    ### QTY ###
    COUNT(platform_order_code) AS order_qty,
    COUNT(CASE WHEN order_price_mechanisms.is_dbdf THEN platform_order_code END) AS dbdf_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_fleet_delay THEN platform_order_code END) AS surge_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_basket_value_deal THEN platform_order_code END) AS basket_value_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_service_fee THEN platform_order_code END) AS service_fee_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_small_order_fee THEN platform_order_code END) AS small_order_fee_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_dbmov THEN platform_order_code END) AS variable_mov_orders,
    COUNT(CASE WHEN order_price_mechanisms.is_surge_mov THEN platform_order_code END) AS surge_mov_orders,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code END) AS tod_orders,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code END) AS fdnc_orders,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code END) AS customer_location_orders,
    COUNT(CASE WHEN only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" THEN platform_order_code END) AS flat_mov_orders,
    COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_count >= 4 THEN platform_order_code END) AS multiple_pm_orders,
    COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_count = 3 THEN platform_order_code END) AS triple_pm_orders,
    COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_count = 2 THEN platform_order_code END) AS double_pm_orders,
    COUNT(CASE WHEN price_mechanism_fields.exposed_price_mechanism_count = 1 THEN platform_order_code END) AS single_pm_orders,
    ### SHARES ####
    COUNT(CASE WHEN order_price_mechanisms.is_dbdf THEN platform_order_code END) / COUNT(platform_order_code) AS dbdf_share,
    COUNT(CASE WHEN order_price_mechanisms.is_fleet_delay THEN platform_order_code END) / COUNT(platform_order_code) AS surge_share,
    COUNT(CASE WHEN order_price_mechanisms.is_basket_value_deal THEN platform_order_code END) / COUNT(platform_order_code) AS basket_value_share,
    COUNT(CASE WHEN order_price_mechanisms.is_service_fee THEN platform_order_code END) / COUNT(platform_order_code) AS service_fee_share,
    COUNT(CASE WHEN order_price_mechanisms.is_small_order_fee THEN platform_order_code END) / COUNT(platform_order_code) AS small_order_fee_share,
    COUNT(CASE WHEN order_price_mechanisms.is_dbmov THEN platform_order_code END) / COUNT(platform_order_code) AS variable_mov_share,
    COUNT(CASE WHEN order_price_mechanisms.is_surge_mov THEN platform_order_code END) / COUNT(platform_order_code) AS surge_mov_share,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_time_condition THEN platform_order_code END) / COUNT(platform_order_code) AS tod_share,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_condition THEN platform_order_code END) / COUNT(platform_order_code) AS fdnc_share,
    COUNT(CASE WHEN vendor_price_mechanisms.vendor_has_customer_area THEN platform_order_code END) / COUNT(platform_order_code) AS customer_location_share,
    COUNT(CASE WHEN vendor_price_scheme_type = 'Experiment' THEN platform_order_code END) AS test_orders, #cambiar de ordenes a usuarios y vendedores expuestos al test
    COUNT(CASE WHEN vendor_price_scheme_type = 'Experiment' THEN platform_order_code END) / COUNT(platform_order_code) AS test_orders_share
  FROM `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
  where created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND created_date_local < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND is_own_delivery IS TRUE
    AND entity_id is not null
    AND region is not null
    AND entity_id != 'FP_DE'
    AND (vertical_type IN ('restaurants', 'coffee', 'home_based_kitchen', 'street_food', 'confectionery') AND vertical_type NOT IN ('courier', 'courier_business'))
  GROUP BY 1,2
)
, tests AS (
  SELECT
    entity_id,
    DATE(DATE_TRUNC(test_start_date, MONTH)) month,
    COUNT(DISTINCT CASE WHEN is_test_config_good THEN test_name END) AS all_valid_tests_count,#porcentaje de tiempo de test activos
    COUNT(DISTINCT CONCAT(entity_id,entity_id)) AS all_tests_count #cantidad distinta de test activos
  FROM `fulfillment-dwh-production.cl._dps_experiment_configuration_versions` t
  WHERE TRUE
    AND DATE(t.test_start_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND DATE(t.test_start_date) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    AND parent_vertical_flags.is_restaurant # does this consider tests with no parent vertical filter?
  GROUP BY 1,2
)
, avg_kpi AS (
  SELECT
    kpi.entity_id,
    kpi.month,
    ## Region thresholds ##
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_region,SUM(orders) OVER agg_region) AS profitable_orders_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_region,SUM(total_orders) OVER agg_region) AS od_rest_orders_reg_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_region,SUM(total_population) OVER agg_region) AS active_zones_pop_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_region,SUM(addressable_population) OVER agg_region) AS user_penetration_reg_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders) OVER agg_region,SUM(orders) OVER agg_region) AS free_df_orders_reg_threshold,
    SAFE_DIVIDE(SUM(campaign_orders) OVER agg_region,SUM(orders) OVER agg_region) AS campaign_orders_reg_threshold,
    AVG(afv_eur) OVER agg_region AS afv_reg_threshold,
    SAFE_DIVIDE(SUM(df_eur) OVER agg_region, SUM(afv_eur) OVER agg_region) AS df_afv_reg_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END) OVER agg_region ,SUM(df_eur) OVER agg_region) AS surge_df_reg_threshold,
    SAFE_DIVIDE(SUM(sf_eur) OVER agg_region, SUM(CASE WHEN cf_eur >0 THEN cf_eur ELSE 0 END) OVER agg_region) AS sf_cf_reg_threshold,
    SAFE_DIVIDE(SUM(mov_eur) OVER agg_region, SUM(CASE WHEN afv_eur >0 THEN afv_eur ELSE 0 END) OVER agg_region) AS mov_afv_reg_threshold,
    SAFE_DIVIDE(SUM(sbf_eur) OVER agg_region, SUM(cf_eur) OVER agg_region) AS sbf_cf_reg_threshold,
    AVG(od_restaurant_users) OVER agg_region AS active_users_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_region, SUM(od_restaurant_users) OVER agg_region) AS frequency_reg_threshold,
    SAFE_DIVIDE(SUM(all_valid_tests_count) OVER agg_region, SUM(all_tests_count) OVER agg_region) AS valid_tests_reg_threshold,
    SAFE_DIVIDE(SUM(test_orders) OVER agg_region, SUM(orders) OVER agg_region) AS test_orders_share_reg_threshold, -- renamed from total_tests_reg_threshold
    ## Market archetype thresholds ##
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_entity, SUM(orders) OVER agg_entity) AS profitable_orders_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_entity, SUM(total_orders) OVER agg_entity) AS od_rest_orders_ma_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_entity, SUM(total_population) OVER agg_entity) AS  active_zones_pop_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_entity, SUM(addressable_population) OVER agg_entity) AS user_penetration_ma_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders) OVER agg_entity, SUM(orders) OVER agg_entity) AS free_df_orders_ma_threshold,
    SAFE_DIVIDE(SUM(campaign_orders) OVER agg_entity, SUM(orders) OVER agg_entity) AS campaign_orders_ma_threshold,
    AVG(afv_eur) OVER agg_entity AS afv_ma_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END) OVER agg_entity, SUM(df_eur) OVER agg_entity) AS surge_df_ma_threshold,
    SAFE_DIVIDE(SUM(df_eur) OVER agg_entity, SUM(afv_eur) OVER agg_entity) AS df_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sf_eur) OVER agg_entity, SUM( CASE WHEN cf_eur > 0 THEN cf_eur else 0 END) OVER agg_entity) AS sf_cf_ma_threshold,
    SAFE_DIVIDE(SUM(mov_eur) OVER agg_entity, SUM(CASE WHEN afv_eur > 0 THEN afv_eur else 0 END) OVER agg_entity) AS mov_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sbf_eur) OVER agg_entity, SUM(CASE WHEN cf_eur > 0 THEN cf_eur else 0 END) OVER agg_entity) AS sbf_cf_ma_threshold,
    AVG(od_restaurant_users) OVER agg_entity AS active_users_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_entity, SUM(od_restaurant_users) OVER agg_entity) AS frequency_ma_threshold,
    SAFE_DIVIDE(SUM(all_valid_tests_count) OVER agg_entity, SUM(all_tests_count) OVER agg_entity) AS valid_tests_ma_threshold,
    SAFE_DIVIDE(SUM(test_orders) OVER agg_entity, SUM(orders) OVER agg_entity) AS test_orders_share_ma_threshold,
  FROM kpi
  LEFT JOIN tests t ON kpi.month = t.month AND kpi.entity_id = t.entity_id
  LEFT JOIN pm ON kpi.month = pm.month AND kpi.entity_id = pm.entity_id
  WINDOW agg_region AS (PARTITION BY kpi.region, kpi.month)
    , agg_entity AS (PARTITION BY kpi.market_archetype, kpi.month)
)
, kpi_score AS (
  SELECT
    kpi.*,
    ## Region scores ##
    ### Profitability ###
    CASE WHEN profitable_orders_share >= profitable_orders_reg_threshold THEN 1 else 0 END AS profitable_orders_score,
    CASE WHEN free_df_scheme_share <= free_df_orders_reg_threshold THEN 1 else 0 END AS free_df_scheme_score,
    CASE WHEN campaign_orders_share <= campaign_orders_reg_threshold THEN 1 else 0 END AS campaign_orders_score,
    CASE WHEN SAFE_DIVIDE(mov_eur,afv_eur) >= mov_afv_reg_threshold THEN 1 else 0 END AS mov_afv_score,
    CASE WHEN SAFE_DIVIDE(surge_fee_eur,df_eur) <= surge_df_reg_threshold THEN 1 else 0 END AS surge_df_score,
    CASE WHEN SAFE_DIVIDE(sbf_eur,cf_eur) >= sbf_cf_reg_threshold THEN 1 else 0 END AS sbf_cf_score,
    CASE WHEN SAFE_DIVIDE(sf_eur,cf_eur) >= sf_cf_reg_threshold THEN 1 else 0 END AS sf_cf_score,
    CASE WHEN afv_eur >= afv_reg_threshold THEN 1 else 0 END AS afv_eur_score,
    ### Penetration ###
    CASE WHEN od_rest_orders_share>= od_rest_orders_reg_threshold THEN 1 else 0 END AS od_rest_orders_score,
    CASE WHEN active_zones_population_share >= active_zones_pop_reg_threshold THEN 1 else 0 END AS active_zones_score,
    CASE WHEN user_penetration >= user_penetration_reg_threshold THEN 1 else 0 END AS user_penetration_score,
    CASE WHEN SAFE_DIVIDE(od_restaurant_orders,od_restaurant_users) >= frequency_reg_threshold THEN 1 else 0 END AS frequency_score,
    CASE WHEN od_restaurant_users >= active_users_reg_threshold THEN 1 else 0 END AS active_users_score,
    ### Experimentation ###
    CASE WHEN SAFE_DIVIDE(all_valid_tests_count,all_tests_count) >= valid_tests_reg_threshold THEN 1 else 0 END AS valid_tests_score,
    CASE WHEN SAFE_DIVIDE(test_orders,orders) >= test_orders_share_reg_threshold THEN 1 else 0 END AS test_orders_share_score, -- renamed from total_tests_score
    ## Market archetypes scores ##
    ### Profitability ###
    CASE WHEN profitable_orders_share >= profitable_orders_ma_threshold THEN 1 else 0 END AS profitable_orders_ma_score,
    CASE WHEN od_rest_orders_share>= od_rest_orders_ma_threshold THEN 1 else 0 END AS od_rest_orders_ma_score,
    CASE WHEN active_zones_population_share >= active_zones_pop_ma_threshold THEN 1 else 0 END AS active_zones_ma_score,
    CASE WHEN user_penetration >= user_penetration_ma_threshold THEN 1 else 0 END AS user_penetration_ma_score,
    CASE WHEN free_df_scheme_share <= free_df_orders_ma_threshold THEN 1 else 0 END AS free_df_scheme_ma_score,
    CASE WHEN campaign_orders_share <= campaign_orders_ma_threshold THEN 1 else 0 END AS campaign_orders_ma_score,
    CASE WHEN afv_eur >= afv_ma_threshold THEN 1 else 0 END AS afv_eur_ma_score,
    CASE WHEN SAFE_DIVIDE(surge_fee_eur,df_eur) <= surge_df_ma_threshold THEN 1 else 0 END AS surge_df_ma_score,
    CASE WHEN SAFE_DIVIDE(sf_eur,cf_eur) >= sf_cf_ma_threshold THEN 1 else 0 END AS sf_cf_ma_score,
    CASE WHEN SAFE_DIVIDE(mov_eur,afv_eur) >= mov_afv_ma_threshold THEN 1 else 0 END AS mov_afv_ma_score,
    CASE WHEN SAFE_DIVIDE(sbf_eur,cf_eur) >= sbf_cf_ma_threshold THEN 1 else 0 END AS sbf_cf_ma_score,
    ### Penetration ###
    CASE WHEN od_restaurant_users >= active_users_ma_threshold THEN 1 else 0 END AS active_users_ma_score,
    CASE WHEN SAFE_DIVIDE(od_restaurant_orders,od_restaurant_users) >= frequency_ma_threshold THEN 1 else 0 END AS frequency_ma_score,
    ### Experimentation ###
    CASE WHEN SAFE_DIVIDE(all_valid_tests_count,all_tests_count) >= valid_tests_ma_threshold THEN 1 else 0 END AS valid_tests_ma_score,
    CASE WHEN SAFE_DIVIDE(test_orders,orders) >= test_orders_share_ma_threshold THEN 1 else 0 END AS test_orders_share_ma_score, -- renamed from total_tests_ma_score
    ### Sophistication scores ###
    CASE WHEN dbdf_share >= 0.5 THEN 1 else 0 END AS dbdf_score,
    CASE WHEN surge_share >= 0.5 THEN 1 else 0 END AS surge_score,
    CASE WHEN service_fee_share >= 0.5 THEN 1 else 0 END AS service_fee_score,
    CASE WHEN basket_value_share >= 0.5 THEN 1 else 0 END AS basket_value_score,
    CASE WHEN small_order_fee_share >= 0.5 THEN 1 else 0 END AS small_order_fee_score,
    CASE WHEN variable_mov_share >= 0.5 THEN 1 else 0 END AS variable_mov_score,
    CASE WHEN surge_mov_share >= 0.5 THEN 1 else 0 END AS surge_mov_score,
    CASE WHEN tod_share >= 0.5 THEN 1 else 0 END AS tod_score,
    CASE WHEN fdnc_share >= 0.5 THEN 1 else 0 END AS fdnc_score,
    CASE WHEN customer_location_share >= 0.5 THEN 1 else 0 END AS customer_location_score,
    ## Thresholds ##
    avg_kpi.* EXCEPT(entity_id, month),
  FROM kpi
  LEFT JOIN avg_kpi USING(entity_id, month)
  LEFT JOIN tests t USING(month, entity_id)
  LEFT JOIN pm USING(month, entity_id)
)
SELECT
  kpi.*,
  -- Todas las columnas de la primera subconsulta
  pm.* EXCEPT(entity_id, month), -- Todas las columnas de la segunda subconsulta
  -- penetration.* EXCEPT(entity_id, month), -- Todas las columnas de la tercera subconsulta
  tests.* EXCEPT(entity_id, month),  -- Todas las columnas de la cuarta subconsulta
  (profitable_orders_score + free_df_scheme_score + campaign_orders_score + mov_afv_score + surge_df_score + sbf_cf_score + sf_cf_score) / 7 AS profitability_score,
  (od_rest_orders_score + frequency_score + user_penetration_score) / 3 AS penetration_score,
  (valid_tests_score + test_orders_share_score) / 2 experimentation_score,
  (profitable_orders_ma_score + free_df_scheme_ma_score + campaign_orders_ma_score + mov_afv_ma_score + surge_df_ma_score + sbf_cf_ma_score + sf_cf_ma_score) / 7 AS profitability_ma_score,
  (od_rest_orders_ma_score + frequency_ma_score + user_penetration_ma_score) / 3 AS penetration_ma_score,
  (valid_tests_ma_score + test_orders_share_score) / 2 experimentation_ma_score,
  (dbdf_score + surge_score + service_fee_score + basket_value_score + small_order_fee_score + variable_mov_score + surge_mov_score + tod_score + fdnc_score + customer_location_score) / 10 AS sophistication_score
FROM kpi_score AS kpi
LEFT JOIN pm USING (entity_id, month)
LEFT JOIN tests USING (entity_id, month)
ORDER BY 1,2,4 DESC
