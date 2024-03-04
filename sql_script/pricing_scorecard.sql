with
dates AS (
  SELECT date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH), DATE_TRUNC(CURRENT_DATE(), MONTH))) AS date
),
entities AS (
  SELECT DISTINCT global_entity_id entity_id, date
  FROM `fulfillment-dwh-production.curated_data_shared_intl_markets.competition_strategy_framework_monthly_categorisations`
  CROSS JOIN dates
),
categories as (
SELECT
  entity_id,
  date as month,
  ifnull(
    ifnull(country_categorisation,
      last_value(country_categorisation ignore nulls) over (partition by entity_id order by date))
      , last_value(country_categorisation ignore nulls) over (partition by entity_id order by date desc)
      ) country_categorisation,
FROM entities
LEFT JOIN `fulfillment-dwh-production.curated_data_shared_intl_markets.competition_strategy_framework_monthly_categorisations` i
  ON global_entity_id = entity_id
    AND report_month = date
ORDER BY 1,2),

population as (
SELECT
    ANY_VALUE(cp.dh_country_code) AS dh_country_code,
    cp.month,
    cp.country_code,
    ANY_VALUE(cp.total_population) AS total_population,
    MAX(cp.active_zones_population) AS active_zones_population,
    MAX(cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) AS addressable_population,
    ANY_VALUE(cp.active_zones_population / cp.total_population) AS active_zones_population_share,
    ANY_VALUE(i.internet_access_perc) AS internet_access_perc,
    ANY_VALUE(a.population_ages_15_64_perc) AS population_ages_15_64_perc,
    MAX(cp.active_zones_population * i.internet_access_perc * a.population_ages_15_64_perc) / ANY_VALUE(cp.total_population) AS addressable_population_perc
FROM `logistics-data-storage-staging.temp_pricing.country_served_population` cp
LEFT JOIN `logistics-data-storage-staging.temp_pricing.internet_access_share` i USING (country_code)
LEFT JOIN `logistics-data-storage-staging.temp_pricing.active_population_share` a USING (country_code)
WHERE
    month >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH) 
    AND month < DATE_TRUNC(CURRENT_DATE(), MONTH)
GROUP BY 2, 3
ORDER BY 1, 2
),
total_orders as (
  SELECT
  o.entity_id,
  date_trunc(created_date, MONTH) month,
  count(distinct platform_order_code) as orders
  FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
WHERE created_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH) 
  and created_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
  and is_sent
  and vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants')
  --and region is not null
  GROUP by 1,2--,3
  ORDER By 1 asc ,2 desc
),
kpi as (
  SELECT
    o.region,
    o.entity_id,
    o.country_code,
    date_trunc(created_date, MONTH) month,
    CASE WHEN c.country_categorisation in ('Strong Leadership', 'Leadership', 'Very strong leadership') then "Leadership"
    WHEN c.country_categorisation in ('Head to Head', 'Lagging') then "Challenger" end as market_archetype,
    p.addressable_population,
    p.total_population,
    p.active_zones_population,
    p.active_zones_population_share,
    t.orders total_orders,
    count(distinct platform_order_code) as orders,
    count(distinct analytical_customer_id) users,
    count(distinct case when (vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants') OR vendor_vertical_parent is null) and is_own_delivery then analytical_customer_id end) od_restaurant_users,
    count(distinct case when (vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants') OR vendor_vertical_parent is null) and is_own_delivery then analytical_customer_id end)/p.addressable_population user_penetration,
    count(distinct case when (vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants') OR vendor_vertical_parent is null) and is_own_delivery then platform_order_code end) od_restaurant_orders,
    count(distinct case when (vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants') OR vendor_vertical_parent is null) and is_own_delivery then platform_order_code end)/t.orders as od_rest_orders_share,
    count( distinct case when ifnull((delivery_fee_eur+service_fee_eur+mov_customer_fee_eur+commission_eur) - delivery_costs_eur, 0) >= 0 then platform_order_code end) as profitable_orders, ## cambiar profit local
    count( distinct case when ifnull(profit_local, 0) >= 0 then platform_order_code end) as profitable_orders_II, ## cambiar profit local
    count( distinct case when ifnull((delivery_fee_eur+service_fee_eur+mov_customer_fee_eur+commission_eur) - delivery_costs_eur, 0) >= 0 then platform_order_code end)/count(distinct platform_order_code) as profitable_orders_share,
    sum(gfv_eur)/count(distinct platform_order_code) afv_eur,
    sum(delivery_fee_eur)/count(distinct platform_order_code) df_eur,
    sum(dps_delivery_fee_eur)/count(distinct platform_order_code) dps_df_eur,
    sum(dps_surge_fee_eur)/count(distinct platform_order_code) surge_fee_eur,
    sum(service_fee_eur)/count(distinct platform_order_code) sf_eur,
    sum(mov_customer_fee_eur)/count(distinct platform_order_code) sbf_eur,
    safe_divide(sum(delivery_fee_eur+service_fee_eur+mov_customer_fee_eur),count(distinct platform_order_code)) cf_eur,
    sum(dps_minimum_order_value_eur)/count(distinct platform_order_code) mov_eur,
    sum(commission_eur)/count(distinct platform_order_code) comm_eur,
    sum(joker_vendor_fee_eur)/count(distinct platform_order_code) joker_eur,
    sum(delivery_costs_eur)/count(distinct platform_order_code) cpo_eur,
    sum(discount_dh_eur)/count(distinct platform_order_code) + sum(voucher_dh_eur)/count(distinct platform_order_code) incentives_dh_eur,
    count( distinct case when dps_delivery_fee_local=0 and vendor_price_scheme_type != "Campaign" then platform_order_code end) as            
    free_df_scheme_orders, ## has new customer condition, ver como meter esto
    count( distinct case when dps_delivery_fee_local=0 and vendor_price_scheme_type != "Campaign" then platform_order_code end)/count(distinct platform_order_code) as free_df_scheme_share,
    count( distinct case when vendor_price_scheme_type = "Campaign" then platform_order_code end) as campaign_orders,
    count( distinct case when vendor_price_scheme_type = "Campaign" then platform_order_code end)/count(distinct platform_order_code) as campaign_orders_share

FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
left join categories c
on o.entity_id = c.entity_id and date_trunc(created_date, month) = c.month
left join population p on o.country_code = lower(p.country_code) and date_trunc(created_date, month) = p.month
left join total_orders t on o.entity_id = t.entity_id /*and o.region = t.region*/ and date_trunc(created_date, month)= t.month
WHERE created_date >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH) 
  and created_date < DATE_TRUNC(CURRENT_DATE(), MONTH)
  and is_sent
  and o.entity_id != "FP_DE"
   --AND vendor_vertical_parent IN ('Restaurant', 'restaurant', 'restaurants') 
    AND (
      vertical_type IN (
        'restaurants', 'coffee', 'home_based_kitchen', 'street_food', 'confectionery'
      ) 
      AND vertical_type NOT IN ('courier', 'courier_business')
    )
GROUP BY 1,2,3,4,5,6,7,8,9,10--, country_categorisation, global_entity_id, report_month
ORDER BY 1,4 desc,2
),
pm as (
  select
 -- date_trunc(created_date_local, month) as month,
  case when p.region = 'America' then 'Americas'
   when p.region = 'Asia' then 'Asia'
   when p.region = 'Europe' then 'Europe'
   when p.region ='MENA' then 'MENA'
   end as region,
  p.entity_id,
  date_trunc(created_date_local, MONTH) as month,
  ### QTY ###
  count(platform_order_code) as order_qty,
      count(case when order_price_mechanisms.is_dbdf then platform_order_code end) as dbdf_orders,
      count(case when order_price_mechanisms.is_fleet_delay then platform_order_code end) as surge_orders,
      count(case when order_price_mechanisms.is_basket_value_deal then platform_order_code end) as basket_value_orders,
      count(case when order_price_mechanisms.is_service_fee then platform_order_code end) as service_fee_orders,
      count(case when order_price_mechanisms.is_small_order_fee then platform_order_code end) as small_order_fee_orders,
      count(case when order_price_mechanisms.is_dbmov then platform_order_code end) as variable_mov_orders,
      count(case when order_price_mechanisms.is_surge_mov then platform_order_code end) as surge_mov_orders,
      count(case when vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end) as tod_orders,
      count(case when vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end) as fdnc_orders,
      count(case when vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end) as customer_location_orders,
      count(case when only_dps_scheme_price_mechanisms.mov_type = "Flat_non_zero" then platform_order_code end) as flat_mov_orders,

      count(case when price_mechanism_fields.exposed_price_mechanism_count >= 4 then platform_order_code end) as multiple_pm_orders,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 3 then platform_order_code end) as triple_pm_orders,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 2 then platform_order_code end) as double_pm_orders,
      count(case when price_mechanism_fields.exposed_price_mechanism_count = 1 then platform_order_code end) as single_pm_orders,
     ### SHARES #### 
      count(case when order_price_mechanisms.is_dbdf then platform_order_code end)/count(platform_order_code) as dbdf_share,
      count(case when order_price_mechanisms.is_fleet_delay then platform_order_code end)/count(platform_order_code) as surge_share,
      count(case when order_price_mechanisms.is_basket_value_deal then platform_order_code end)/count(platform_order_code) as basket_value_share,
      count(case when order_price_mechanisms.is_service_fee then platform_order_code end)/count(platform_order_code) as service_fee_share,
      count(case when order_price_mechanisms.is_small_order_fee then platform_order_code end)/count(platform_order_code) as small_order_fee_share,
      count(case when order_price_mechanisms.is_dbmov then platform_order_code end) / count(platform_order_code) as variable_mov_share,
      count(case when order_price_mechanisms.is_surge_mov then platform_order_code end)/count(platform_order_code) as surge_mov_share,
      count(case when vendor_price_mechanisms.vendor_has_time_condition then platform_order_code end)/count(platform_order_code) as tod_share,
      count(case when vendor_price_mechanisms.vendor_has_customer_condition then platform_order_code end)/count(platform_order_code) as fdnc_share,
      count(case when vendor_price_mechanisms.vendor_has_customer_area then platform_order_code end)/count(platform_order_code) as customer_location_share,
      count(case when vendor_price_scheme_type='Experiment' then platform_order_code end) as test_orders, #cambiar de ordenes a usuarios y vendedores expuestos al test
      count(case when vendor_price_scheme_type='Experiment' then platform_order_code end)/count(platform_order_code) as test_orders_share
  
from `logistics-data-storage-staging.long_term_pricing.sl_pricing_mechanism_data_prd` p
where  created_date_local >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH) 
  and created_date_local < DATE_TRUNC(CURRENT_DATE(), MONTH)
and is_own_delivery IS TRUE
and entity_id is not null 
and region is not null
AND entity_id != 'FP_DE'
AND (vertical_type IN ('restaurants', 'coffee', 'home_based_kitchen', 'street_food', 'confectionery') 
      AND vertical_type NOT IN ('courier', 'courier_business')) 
group by 1,2,3--,4
order by region asc
),

tests as (
  WITH base AS (
  SELECT
    region,
    entity_id,
    date(date_trunc(test_start_date, MONTH)) month,
    count(distinct CASE WHEN is_test_config_good = TRUE THEN test_name ELSE null END) AS valid_tests,#porcentaje de tiempo de test activos
    count(distinct test_name) AS test_all_entities_count #cantidad distinta de test activos
  FROM `fulfillment-dwh-production.cl._dps_experiment_configuration_versions` t
  WHERE TRUE
    AND date(t.test_start_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND date(t.test_start_date) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    and parent_vertical_flags.is_restaurant
  GROUP BY 1,2,3
),
elasticity as (
  SELECT 
  entity_id,
  date(date_trunc(test_start_date, MONTH)) month,
  count(*) as elasticity_test
FROM `logistics-data-storage-staging.long_term_pricing.pricing_elasticity_tests_summary`
 WHERE TRUE
    AND date(test_start_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 4 MONTH)
    AND date(test_start_date) < DATE_TRUNC(CURRENT_DATE(), MONTH)
    and parent_vertical_flags.is_restaurant_test
GROUP BY 1,2
ORDER BY 1,2

)
SELECT
  b.entity_id,
  b.month,
  IFNULL(elasticity.elasticity_test, 0) AS elasticity_test,
  SUM(valid_tests) OVER (PARTITION BY b.entity_id, b.region, b.month) AS test_all_valid_tests,
  SUM(test_all_entities_count) OVER (PARTITION BY b.entity_id, b.region, b.month) AS test_all_entities_count_cum
FROM base b
LEFT JOIN elasticity ON b.entity_id = elasticity.entity_id /*AND b.region = elasticity.region*/ AND b.month = elasticity.month
ORDER BY 1,2
),

avg_kpi AS (
  SELECT
    kpi.entity_id,
    kpi.month,
    safe_divide(sum(profitable_orders)over (partition by kpi.region, kpi.month),sum(orders) over (partition by kpi.region, kpi.month)) as profitable_orders_reg_threshold,
    safe_divide(sum(od_restaurant_orders)over (partition by kpi.region, kpi.month),sum(total_orders) over (partition by kpi.region, kpi.month)) as od_rest_orders_reg_threshold,
    safe_divide(sum(active_zones_population) over(partition by kpi.region, kpi.month),sum(total_population) over(partition by kpi.region, kpi.month))  as active_zones_pop_reg_threshold,
    safe_divide(sum(od_restaurant_users)over (partition by kpi.region, kpi.month),sum(addressable_population) over (partition by kpi.region, kpi.month)) as user_penetration_reg_threshold,
    safe_divide(sum(free_df_scheme_orders) over (partition by kpi.region, kpi.month),sum(orders)over (partition by kpi.region, kpi.month))  as free_df_orders_reg_threshold,
    safe_divide(sum(campaign_orders) over (partition by kpi.region, kpi.month),sum(orders)over (partition by kpi.region, kpi.month))  as campaign_orders_reg_threshold,
    avg(afv_eur) over(partition by kpi.region, kpi.month) as afv_reg_threshold,
    safe_divide(sum(df_eur)over (partition by kpi.region, kpi.month),sum(afv_eur)over (partition by kpi.region, kpi.month)) as df_afv_reg_threshold,
    safe_divide(sum(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END)over (partition by kpi.region, kpi.month) ,sum(df_eur)over (partition by kpi.region, kpi.month))  as surge_df_reg_threshold,
    safe_divide(sum(sf_eur) over (partition by kpi.region, kpi.month),sum(case when cf_eur>0 then cf_eur else 0 end)over (partition by kpi.region, kpi.month))  as sf_cf_reg_threshold,
    safe_divide(sum(mov_eur) over (partition by kpi.region, kpi.month),sum(case when afv_eur>0 then afv_eur else 0 end) over (partition by kpi.region, kpi.month)) as mov_afv_reg_threshold,
    safe_divide(sum(sbf_eur) over (partition by kpi.region, kpi.month),sum(cf_eur)over (partition by kpi.region, kpi.month))  as sbf_cf_reg_threshold,
    avg(od_restaurant_users) over (partition by kpi.region, kpi.month) as active_users_reg_threshold, 
    safe_divide(sum(od_restaurant_orders) over (partition by kpi.region, kpi.month),sum(od_restaurant_users) over (partition by kpi.region, kpi.month)) as frequency_reg_threshold,
    safe_divide(sum(test_all_valid_tests)over(partition by kpi.region, kpi.month),sum(test_all_entities_count_cum)over(partition by kpi.region, kpi.month))  as valid_tests_reg_threshold,
    safe_divide(sum(elasticity_test) over(partition by kpi.region, kpi.month),sum(test_all_entities_count_cum) over(partition by kpi.region, kpi.month)) as elasticity_tests_reg_threshold,
    safe_divide(sum(test_orders) over(partition by kpi.region, kpi.month), sum(orders)over(partition by kpi.region, kpi.month)) as total_tests_reg_threshold,
   
    ## market archetype thresholds ##
    safe_divide(sum(profitable_orders) over (partition by kpi.market_archetype, kpi.month),sum(orders) over (partition by kpi.market_archetype, kpi.month)) as profitable_orders_ma_threshold,
     safe_divide(sum(od_restaurant_orders)over (partition by kpi.market_archetype, kpi.month),sum(total_orders) over (partition by kpi.market_archetype, kpi.month)) as od_rest_orders_ma_threshold,
    safe_divide(sum(active_zones_population) over(partition by kpi.market_archetype, kpi.month),sum(total_population) over(partition by kpi.market_archetype, kpi.month)) as  active_zones_pop_ma_threshold,
    safe_divide(sum(od_restaurant_users)over (partition by kpi.market_archetype, kpi.month),sum(addressable_population) over (partition by kpi.market_archetype, kpi.month))  as user_penetration_ma_threshold,
    safe_divide(sum(free_df_scheme_orders)  over (partition by kpi.market_archetype, kpi.month),sum(orders) over (partition by kpi.market_archetype, kpi.month)) as free_df_orders_ma_threshold,
    safe_divide(sum(campaign_orders)  over (partition by kpi.market_archetype, kpi.month),sum(orders)  over (partition by kpi.market_archetype, kpi.month)) as campaign_orders_ma_threshold,
    avg(afv_eur) over(partition by  kpi.market_archetype, kpi.month) as afv_ma_threshold,
    safe_divide(sum(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END)  over (partition by kpi.market_archetype, kpi.month),sum(df_eur) over (partition by kpi.market_archetype, kpi.month))  as surge_df_ma_threshold,
    safe_divide(sum(df_eur)  over (partition by kpi.market_archetype, kpi.month),sum(afv_eur) over (partition by kpi.market_archetype, kpi.month)) as df_afv_ma_threshold,
    safe_divide(sum(sf_eur)  over (partition by kpi.market_archetype, kpi.month),sum( case when cf_eur>0 then cf_eur else 0 end) over (partition by kpi.market_archetype, kpi.month)) as sf_cf_ma_threshold,
    safe_divide(sum(mov_eur)  over (partition by kpi.market_archetype, kpi.month),sum(case when afv_eur>0 then afv_eur else 0 end) over (partition by kpi.market_archetype, kpi.month)) as mov_afv_ma_threshold,
    safe_divide(sum(sbf_eur)  over (partition by kpi.market_archetype, kpi.month),sum(case when cf_eur>0 then cf_eur else 0 end) over (partition by kpi.market_archetype, kpi.month)) as sbf_cf_ma_threshold,
    avg(od_restaurant_users) over (partition by kpi.market_archetype, kpi.month) as active_users_ma_threshold, 
    safe_divide(sum(od_restaurant_orders)over (partition by kpi.market_archetype, kpi.month),sum(od_restaurant_users) over (partition by kpi.market_archetype, kpi.month))  as frequency_ma_threshold,
    safe_divide(sum(test_all_valid_tests) over (partition by kpi.market_archetype, kpi.month),sum(test_all_entities_count_cum) over (partition by kpi.market_archetype, kpi.month)) as valid_tests_ma_threshold,
    safe_divide(sum(elasticity_test)  over (partition by kpi.market_archetype, kpi.month),sum(test_all_entities_count_cum) over (partition by kpi.market_archetype, kpi.month)) as elasticity_tests_ma_threshold,
     safe_divide(sum(test_orders) over(partition by kpi.market_archetype, kpi.month), sum(orders)over(partition by kpi.market_archetype, kpi.month)) as total_tests_ma_threshold,
    from kpi
    left join tests t on kpi.month = t.month and kpi.entity_id = t.entity_id
    LEFT JOIN pm on kpi.month = pm.month and kpi.entity_id = pm.entity_id
    group by 1,2,orders, total_orders,free_df_scheme_orders,active_zones_population,total_population,addressable_population, campaign_orders, surge_fee_eur, df_eur, sf_eur, mov_eur, sbf_eur, profitable_orders,afv_eur, cf_eur, test_orders, od_restaurant_users,kpi.region, market_archetype, test_all_valid_tests, elasticity_test, test_all_entities_count_cum, od_restaurant_orders),

kpi_score as (
  SELECT
  kpi.*,
  ### Profitability ###
  CASE WHEN profitable_orders_share >= profitable_orders_reg_threshold then 1 else 0 end as profitable_orders_score,
  CASE WHEN free_df_scheme_share <= free_df_orders_reg_threshold then 1 else 0 end as free_df_scheme_score,
  CASE WHEN campaign_orders_share <= campaign_orders_reg_threshold then 1 else 0 end as campaign_orders_score,
  CASE WHEN safe_divide(mov_eur,afv_eur) >= mov_afv_reg_threshold then 1 else 0 end as mov_afv_score,
  CASE WHEN safe_divide(surge_fee_eur,df_eur) <= surge_df_reg_threshold then 1 else 0 end as surge_df_score,
  CASE WHEN safe_divide(sbf_eur,cf_eur) >= sbf_cf_reg_threshold then 1 else 0 end as sbf_cf_score,
  CASE WHEN safe_divide(sf_eur,cf_eur) >= sf_cf_reg_threshold then 1 else 0 end as sf_cf_score,
  CASE WHEN afv_eur >= afv_reg_threshold then 1 else 0 end as afv_eur_score,
  
  ### Penetration ###
  CASE WHEN od_rest_orders_share>= od_rest_orders_reg_threshold then 1 else 0 end as od_rest_orders_score,
  CASE WHEN active_zones_population_share >= active_zones_pop_reg_threshold then 1 else 0 end as active_zones_score,
  CASE WHEN user_penetration >= user_penetration_reg_threshold then 1 else 0 end as user_penetration_score,
  CASE WHEN safe_divide(od_restaurant_orders,od_restaurant_users) >= frequency_reg_threshold then 1 else 0 end as frequency_score,
  CASE WHEN od_restaurant_users >= active_users_reg_threshold then 1 else 0 end as active_users_score,
  
  ### Experimentation ### 
  CASE WHEN safe_divide(test_all_valid_tests,test_all_entities_count_cum) >= valid_tests_reg_threshold then 1 else 0 end as valid_tests_score,
  CASE WHEN safe_divide(elasticity_test,test_all_entities_count_cum) >= valid_tests_reg_threshold then 1 else 0 end as elasticity_tests_score,
  CASE WHEN test_all_entities_count_cum >= total_tests_reg_threshold then 1 else 0 end as total_tests_score,
  
  ### market archetypes scores ####
  CASE WHEN profitable_orders_share >= profitable_orders_ma_threshold then 1 else 0 end as profitable_orders_ma_score,
  CASE WHEN od_rest_orders_share>= od_rest_orders_ma_threshold then 1 else 0 end as od_rest_orders_ma_score,
  CASE WHEN active_zones_population_share >= active_zones_pop_ma_threshold then 1 else 0 end as active_zones_ma_score,
  CASE WHEN user_penetration >= user_penetration_ma_threshold then 1 else 0 end as user_penetration_ma_score,
  CASE WHEN free_df_scheme_share <= free_df_orders_ma_threshold then 1 else 0 end as free_df_scheme_ma_score,
  CASE WHEN campaign_orders_share <= campaign_orders_ma_threshold then 1 else 0 end as campaign_orders_ma_score,
  CASE WHEN afv_eur >= afv_ma_threshold then 1 else 0 end as afv_eur_ma_score,
  CASE WHEN safe_divide(surge_fee_eur,df_eur) <= surge_df_ma_threshold then 1 else 0 end as surge_df_ma_score,
  CASE WHEN safe_divide(sf_eur,cf_eur) >= sf_cf_ma_threshold then 1 else 0 end as sf_cf_ma_score,
  CASE WHEN safe_divide(mov_eur,afv_eur) >= mov_afv_ma_threshold then 1 else 0 end as mov_afv_ma_score,
  CASE WHEN safe_divide(sbf_eur,cf_eur) >= sbf_cf_ma_threshold then 1 else 0 end as sbf_cf_ma_score,
  CASE WHEN od_restaurant_users >= active_users_ma_threshold then 1 else 0 end as active_users_ma_score,
  CASE WHEN safe_divide(od_restaurant_orders,od_restaurant_users) >= frequency_ma_threshold then 1 else 0 end as frequency_ma_score,
  CASE WHEN safe_divide(test_all_valid_tests,test_all_entities_count_cum) >= valid_tests_ma_threshold then 1 else 0 end as valid_tests_ma_score,
  CASE WHEN safe_divide(elasticity_test,test_all_entities_count_cum) >= valid_tests_ma_threshold then 1 else 0 end as elasticity_tests_ma_score,
  CASE WHEN test_all_entities_count_cum >= total_tests_ma_threshold then 1 else 0 end as total_tests_ma_score,
  
  ### Sophistication scores ###
  CASE WHEN dbdf_share >= 0.5 then 1 else 0 end as dbdf_score,
  CASE WHEN surge_share >= 0.5 then 1 else 0 end as surge_score,
  CASE WHEN service_fee_share >= 0.5 then 1 else 0 end as service_fee_score,
  CASE WHEN basket_value_share >= 0.5 then 1 else 0 end as basket_value_score,
  CASE WHEN small_order_fee_share >= 0.5 then 1 else 0 end as small_order_fee_score,
  CASE WHEN variable_mov_share >= 0.5 then 1 else 0 end as variable_mov_score,
  CASE WHEN surge_mov_share >= 0.5 then 1 else 0 end as surge_mov_score,
  CASE WHEN tod_share >= 0.5 then 1 else 0 end as tod_score,
  CASE WHEN fdnc_share >= 0.5 then 1 else 0 end as fdnc_score,
  CASE WHEN customer_location_share >= 0.5 then 1 else 0 end as customer_location_score,
  avg_kpi.* except(entity_id, month)
  FROM kpi 
  Left join avg_kpi on kpi.entity_id = avg_kpi.entity_id and kpi.month = avg_kpi.month
  LEFT JOIN tests t on kpi.month = t.month and kpi.entity_id = t.entity_id
  LEFT JOIN pm on kpi.month = pm.month and kpi.entity_id = pm.entity_id
)


SELECT
  kpi.*,
   -- Todas las columnas de la primera subconsulta
  pm.* except(region, entity_id, month),  -- Todas las columnas de la segunda subconsulta
  --penetration.* except(entity_id, month),  -- Todas las columnas de la tercera subconsulta
  tests.* except(entity_id, month),  -- Todas las columnas de la cuarta subconsulta
  (profitable_orders_score+ free_df_scheme_score + campaign_orders_score + mov_afv_score +surge_df_score +sbf_cf_score +sf_cf_score)/7 as profitability_score,
  (od_rest_orders_score+frequency_score+user_penetration_score)/3 as penetration_score,
  (valid_tests_score+total_tests_score)/2 experimentation_score,
  (profitable_orders_ma_score + free_df_scheme_ma_score + campaign_orders_ma_score+ mov_afv_ma_score+ surge_df_ma_score+ sbf_cf_ma_score+ sf_cf_ma_score)/7 as profitability_ma_score,
  (od_rest_orders_ma_score + frequency_ma_score + user_penetration_ma_score)/3 as penetration_ma_score,
  (valid_tests_ma_score + total_tests_ma_score)/2 experimentation_ma_score,
  (dbdf_score + surge_score + service_fee_score + basket_value_score + small_order_fee_score + variable_mov_score + surge_mov_score + tod_score + fdnc_score+ customer_location_score)/10 as sophistication_score
FROM
  kpi_score as kpi
LEFT JOIN
  pm ON kpi.entity_id = pm.entity_id and kpi.month = pm.month
LEFT JOIN
  tests ON kpi.entity_id = tests.entity_id and kpi.month = tests.month

Order by 1,2 asc, 4 desc
