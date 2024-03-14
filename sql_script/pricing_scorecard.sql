######################### LOGS #########################
  -- Martin Fourcade run the query on 13 Mar. 2024 with the following changes:
    -- Backfilled 1 Year data.
    -- Added affordability index and returned to previous query on served population.
    -- Profit calculated directly from DPS sessions mapped to orders.
    -- Query will run monthly without backfilling. 

######################### INPUTS

  ########### DECLARE VARIABLES
  DECLARE start_date_filter, end_date_filter DATE;
  DECLARE backfill BOOL;


  ######## SET RUN MODE
  SET backfill = FALSE;

  ######## SET END DATE
  SET end_date_filter = CURRENT_DATE();

  #SET PARTITION DATE
  IF backfill THEN
      SET start_date_filter = DATE_SUB("2023-01-01", interval 0 DAY);
  ELSE
      SET start_date_filter = DATE_TRUNC(DATE_SUB(end_date_filter, interval 2 MONTH), MONTH);
  END IF;
#####################################

##################################### STAGING TABLE 


    CREATE TEMP TABLE staging_table AS (
    with
      categories as (
      SELECT
      entity_id,
        date as month,
        country_categorisation,
      FROM `logistics-data-storage-staging.long_term_pricing.market_archetypes`
        WHERE date >= DATE_SUB(start_date_filter, INTERVAL 2 DAY) 
          AND date < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
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
    month BETWEEN start_date_filter AND end_date_filter
GROUP BY 2, 3
ORDER BY 1, 2
),

total_orders as (
SELECT
    o.global_entity_id AS entity_id,
    date_trunc(o.partition_date_local, MONTH) AS month,
    COUNT(DISTINCT order_id) AS orders
  FROM `fulfillment-dwh-production.curated_data_shared_coredata_business.orders` o
  WHERE 
    o.partition_date_local >=  DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND end_date_filter < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
    AND o.is_successful
    AND o.vertical_type IN ('restaurants', 'coffee', 'street_food') 
  GROUP BY 1,2
  ORDER BY 1 ASC, 2 DESC
),
kpi as (
  SELECT
    o.region,
    o.entity_id,
    o.country_code,
    date_trunc(created_date_local, MONTH) month,
    CASE WHEN c.country_categorisation in ('Strong Leadership', 'Leadership', 'Very strong leadership') then "Leadership"
    WHEN c.country_categorisation in ('Head to Head', 'Lagging') then "Challenger" end as market_archetype,
    p.addressable_population,
    p.total_population,
    p.active_zones_population,
    p.active_zones_population_share,
    t.orders orders,
    r.share_of_responses,
    count(distinct case when (o.vertical_type IN ('restaurants', 'coffee', 'street_food') AND o.vertical_type NOT IN ('courier', 'courier_business')) and is_own_delivery then platform_order_code end) as od_restaurant_orders,
    count(distinct case when (o.vertical_type IN ('restaurants', 'coffee', 'street_food') AND o.vertical_type NOT IN ('courier', 'courier_business')) and is_own_delivery then platform_order_code end)/t.orders as od_rest_orders_share,
    --count(distinct analytical_customer_id) users,
    count(distinct case when (o.vertical_type IN ('restaurants', 'coffee', 'street_food') AND o.vertical_type NOT IN ('courier', 'courier_business')) and is_own_delivery then analytical_customer_id end) od_restaurant_users,
    count(distinct analytical_customer_id )/p.addressable_population user_penetration,
    -- count( distinct case when ifnull((delivery_fee_eur+service_fee_eur+mov_customer_fee_eur+commission_eur) - delivery_costs_eur, 0) >= 0 then platform_order_code end) as profitable_orders, ## cambiar profit local
    count( distinct case when ifnull(profit_eur, 0) >= 0 then platform_order_code end) as profitable_orders, ## cambiar profit local
    count( distinct case when ifnull((delivery_fee_eur+service_fee_eur+mov_customer_fee_eur+commission_eur+priority_fee_eur) - delivery_costs_eur, 0) >= 0 then platform_order_code end)/count(distinct platform_order_code) as profitable_orders_share,
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
    count( distinct case when vendor_price_scheme_type = "Campaign" then platform_order_code end)/count(distinct platform_order_code) as campaign_orders_share,
    CASE WHEN region ="Americas" and o.entity_id in ("PY_CL", "PY_DO","PY_EC", "PY_SV", "PY_CR","PY_GT") then avg(weighted_df_diff_ubereats)
    WHEN region="Americas" and o.entity_id not in ("PY_CL", "PY_DO","PY_EC", "PY_SV", "PY_CR","PY_GT") then avg(weighted_df_diff_rappi)
    WHEN region="Europe" then avg(weighted_df_diff_vs_comp)
    WHEN region="Asia" then avg(weighted_df_diff_grab)
    ELSE null end as weighted_df_diff_vs_comp,
    SAFE_DIVIDE(SUM(CASE WHEN pme.pme IS NULL THEN NULL WHEN delivery_fee_local < pme.pme THEN 1 ELSE 0 END),SUM(1)) share_below_pme,




FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` o
LEFT JOIN categories c
on o.entity_id = c.entity_id and date_trunc(created_date_local, month) = c.month
LEFT JOIN population p on o.country_code = lower(p.country_code) and date_trunc(created_date, month) = p.month
left join total_orders t on o.entity_id = t.entity_id /*and o.region = t.region*/ and date_trunc(created_date_local, month)= t.month
LEFT JOIN `logistics-data-storage-staging.temp_pricing.latam_competiton` lat on o.entity_id = lat.country_code and date_trunc(created_date_local, month) = lat.month
LEFT JOIN `logistics-data-storage-staging.temp_pricing.europe_competiton` eu on o.country_code = eu.country_code and date_trunc(created_date_local, month) = eu.month
LEFT JOIN `logistics-data-storage-staging.temp_pricing.apac_competiton` ap on o.country_code = ap.country_code and date_trunc(created_date_local, month) = ap.month
LEFT JOIN `logistics-data-storage-staging.temp_pricing.churn_responses` r on o.entity_id = r.entity_id and date_trunc(created_date_local, month) = r.month
LEFT JOIN `logistics-data-storage-staging.long_term_pricing.point_of_marginal_expensiveness` pme
  ON o.country_code = pme.country_code AND DATE_TRUNC(o.created_date_local, month) = pme.month
WHERE  o.created_date_local BETWEEN start_date_filter AND end_date_filter
  AND o.created_date>= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    --AND dps.created_date< DATE_TRUNC(CURRENT_DATE() , MONTH) 
  AND is_sent
  AND (o.vendor_vertical_parent in ('Restaurant', 'Restaurants', 'restaurant', 'restaurants')
  OR  o.vendor_vertical_parent is null)
  AND o.vertical_type IN ('restaurants', 'coffee', 'street_food')
  AND o.vertical_type NOT IN ('courier', 'courier_business')
  AND o.entity_id != "FP_DE" 

GROUP BY 1,2,3,4,5,6,7,8,9,10,11
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
  
from `fulfillment-dwh-production.cl._dps_orders_with_pricing_mechanism` p
where  created_date_local  BETWEEN start_date_filter AND end_date_filter
AND p.created_date>= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
AND is_own_delivery IS TRUE
AND entity_id is not null 
AND region is not null
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
    AND date(t.test_start_date) >= DATE_SUB(start_date_filter, INTERVAL 2 DAY)
    AND date(t.test_start_date) < DATE_ADD(end_date_filter, INTERVAL 2 DAY)
    and parent_vertical_flags.is_restaurant
  GROUP BY 1,2,3
)
SELECT
  b.entity_id,
  b.month,
  SUM(valid_tests) OVER (PARTITION BY b.entity_id, b.region, b.month) AS test_all_valid_tests,
  SUM(test_all_entities_count) OVER (PARTITION BY b.entity_id, b.region, b.month) AS test_all_entities_count_cum
FROM base b
ORDER BY 1,2
),

avg_kpi AS (
  SELECT
    kpi.entity_id,
    kpi.month,
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_region,SUM(orders) OVER agg_region) as profitable_orders_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders)OVER agg_region,SUM(orders) OVER agg_region) as od_rest_orders_reg_threshold,
    SAFE_DIVIDE(SUM(weighted_df_diff_vs_comp*orders)OVER agg_region,SUM(orders) OVER agg_region) as avg_diff_comp_reg_threshold,
    SAFE_DIVIDE(SUM(share_of_responses*orders)OVER agg_region,SUM(orders) OVER agg_region) as churn_responses_reg_threshold,
    SAFE_DIVIDE(SUM(share_below_pme*orders)OVER agg_region,SUM(orders) OVER agg_region) as share_below_pme_reg_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_region,SUM(total_population) OVER agg_region)  as active_zones_pop_reg_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_region,SUM(addressable_population) OVER agg_region) as user_penetration_reg_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders) OVER agg_region,SUM(orders) OVER agg_region)  as free_df_orders_reg_threshold,
    SAFE_DIVIDE(SUM(campaign_orders) OVER agg_region,SUM(orders) OVER agg_region)  as campaign_orders_reg_threshold,
    AVG(afv_eur) OVER agg_region as afv_reg_threshold,
    SAFE_DIVIDE(SUM(df_eur) OVER agg_region, SUM(afv_eur) OVER agg_region) as df_afv_reg_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END) OVER agg_region ,SUM(df_eur) OVER agg_region)  as surge_df_reg_threshold,
    SAFE_DIVIDE(SUM(sf_eur) OVER agg_region,SUM(case when cf_eur>0 then cf_eur else 0 end) OVER agg_region)  as sf_cf_reg_threshold,
    SAFE_DIVIDE(SUM(mov_eur) OVER agg_region,SUM(case when afv_eur>0 then afv_eur else 0 end) OVER agg_region) as mov_afv_reg_threshold,
    SAFE_DIVIDE(SUM(sbf_eur) OVER agg_region,SUM(cf_eur) OVER agg_region)  as sbf_cf_reg_threshold,
    AVG(od_restaurant_users) OVER agg_region as active_users_reg_threshold, 
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_region,SUM(od_restaurant_users) OVER agg_region) as frequency_reg_threshold,
    SAFE_DIVIDE(SUM(test_all_valid_tests)OVER agg_region,SUM(test_all_entities_count_cum) OVER agg_region)  as valid_tests_reg_threshold,
    SAFE_DIVIDE(SUM(test_orders) OVER agg_region, SUM(orders) OVER agg_region) as total_tests_reg_threshold,
   
    ## market archetype thresholds ##
    SAFE_DIVIDE(SUM(profitable_orders) OVER agg_archetype,SUM(orders) OVER agg_archetype) as profitable_orders_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_orders) OVER agg_archetype,SUM(orders) OVER agg_archetype) as od_rest_orders_ma_threshold,
    SAFE_DIVIDE(SUM(weighted_df_diff_vs_comp*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) as avg_diff_comp_ma_threshold,
    SAFE_DIVIDE(SUM(share_of_responses*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) as churn_responses_ma_threshold,
    SAFE_DIVIDE(SUM(share_below_pme*orders)OVER agg_archetype,SUM(orders) OVER agg_archetype) as share_below_pme_ma_threshold,
    SAFE_DIVIDE(SUM(active_zones_population) OVER agg_archetype,SUM(total_population) OVER agg_archetype) as  active_zones_pop_ma_threshold,
    SAFE_DIVIDE(SUM(od_restaurant_users) OVER agg_archetype,SUM(addressable_population) OVER agg_archetype)  as user_penetration_ma_threshold,
    SAFE_DIVIDE(SUM(free_df_scheme_orders)  OVER agg_archetype,SUM(orders) OVER agg_archetype) as free_df_orders_ma_threshold,
    SAFE_DIVIDE(SUM(campaign_orders)  OVER agg_archetype,SUM(orders)  OVER agg_archetype) as campaign_orders_ma_threshold,
    AVG(afv_eur) OVER agg_archetype as afv_ma_threshold,
    SAFE_DIVIDE(SUM(CASE WHEN surge_fee_eur > 0 THEN surge_fee_eur ELSE 0 END)  OVER agg_archetype,SUM(df_eur) OVER agg_archetype)  as surge_df_ma_threshold,
    SAFE_DIVIDE(SUM(df_eur)  OVER agg_archetype,SUM(afv_eur) OVER agg_archetype) as df_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sf_eur)  OVER agg_archetype,SUM( case when cf_eur>0 then cf_eur else 0 end) OVER agg_archetype) as sf_cf_ma_threshold,
    SAFE_DIVIDE(SUM(mov_eur)  OVER agg_archetype,SUM(case when afv_eur>0 then afv_eur else 0 end) OVER agg_archetype) as mov_afv_ma_threshold,
    SAFE_DIVIDE(SUM(sbf_eur)  OVER agg_archetype,SUM(case when cf_eur>0 then cf_eur else 0 end) OVER agg_archetype) as sbf_cf_ma_threshold,
    AVG(od_restaurant_users) OVER agg_archetype as active_users_ma_threshold, 
    SAFE_DIVIDE(SUM(od_restaurant_orders)OVER agg_archetype,SUM(od_restaurant_users) OVER agg_archetype)  as frequency_ma_threshold,
    SAFE_DIVIDE(SUM(test_all_valid_tests) OVER agg_archetype,SUM(test_all_entities_count_cum) OVER agg_archetype) as valid_tests_ma_threshold,
     SAFE_DIVIDE(SUM(test_orders) OVER agg_archetype, SUM(orders) OVER agg_archetype) as total_tests_ma_threshold,
    from kpi
    left join tests t on kpi.month = t.month and kpi.entity_id = t.entity_id
    LEFT JOIN pm on kpi.month = pm.month and kpi.entity_id = pm.entity_id
     WINDOW agg_region AS (PARTITION BY kpi.region, kpi.month)
    , agg_archetype AS (PARTITION BY kpi.market_archetype, kpi.month)
)
,kpi_score as (
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

  ### Affordability ###
  CASE WHEN weighted_df_diff_vs_comp <= avg_diff_comp_reg_threshold then 1 else 0 end as diff_df_comp_score,
  CASE WHEN share_of_responses <= churn_responses_reg_threshold then 1 else 0 end as churn_responses_score,
  CASE WHEN share_below_pme >= share_below_pme_reg_threshold then 1 else 0 end as share_below_pme_score,

  
  ### Penetration ###
  CASE WHEN od_rest_orders_share>= od_rest_orders_reg_threshold then 1 else 0 end as od_rest_orders_score,
  CASE WHEN active_zones_population_share >= active_zones_pop_reg_threshold then 1 else 0 end as active_zones_score,
  CASE WHEN user_penetration >= user_penetration_reg_threshold then 1 else 0 end as user_penetration_score,
  CASE WHEN safe_divide(od_restaurant_orders,od_restaurant_users) >= frequency_reg_threshold then 1 else 0 end as frequency_score,
  CASE WHEN od_restaurant_users >= active_users_reg_threshold then 1 else 0 end as active_users_score,
  
  ### Experimentation ### 
  CASE WHEN safe_divide(test_all_valid_tests,test_all_entities_count_cum) >= valid_tests_reg_threshold then 1 else 0 end as valid_tests_score,
  CASE WHEN test_all_entities_count_cum >= total_tests_reg_threshold then 1 else 0 end as total_tests_score,
  
  ### market archetypes scores ####
  CASE WHEN profitable_orders_share >= profitable_orders_ma_threshold then 1 else 0 end as profitable_orders_ma_score,
  CASE WHEN od_rest_orders_share >= od_rest_orders_ma_threshold then 1 else 0 end as od_rest_orders_ma_score,
  CASE WHEN weighted_df_diff_vs_comp <= avg_diff_comp_ma_threshold then 1 else 0 end as diff_df_comp_ma_score,
  CASE WHEN share_of_responses <= churn_responses_ma_threshold then 1 else 0 end as churn_responses_ma_score,
  CASE WHEN share_below_pme >= share_below_pme_ma_threshold then 1 else 0 end as share_below_pme_ma_score,
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
  (diff_df_comp_score+churn_responses_score+share_below_pme_score)/3 as affordability_score, 
  (od_rest_orders_score+frequency_score+user_penetration_score)/3 as penetration_score,
  (valid_tests_score+total_tests_score)/2 experimentation_score,
  (profitable_orders_ma_score + free_df_scheme_ma_score + campaign_orders_ma_score+ mov_afv_ma_score+ surge_df_ma_score+ sbf_cf_ma_score+ sf_cf_ma_score)/7 as profitability_ma_score,
  (diff_df_comp_ma_score+churn_responses_ma_score+share_below_pme_ma_score)/3 as affordability_ma_score, 
  (od_rest_orders_ma_score + frequency_ma_score + user_penetration_ma_score)/3 as penetration_ma_score,
  (valid_tests_ma_score + total_tests_ma_score)/2 experimentation_ma_score,
  (dbdf_score + surge_score + service_fee_score + basket_value_score + small_order_fee_score + variable_mov_score + surge_mov_score + tod_score + fdnc_score+ customer_location_score)/10 as sophistication_score
FROM
  kpi_score as kpi
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
