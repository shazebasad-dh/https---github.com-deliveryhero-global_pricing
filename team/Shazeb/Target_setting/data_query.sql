test_data = """
with users as (
SELECT DISTINCT
              entity_id,
              test_id,
              variant,
              count(distinct perseus_client_id) users
  FROM `fulfillment-dwh-production.curated_data_shared.dps_test_cvr_treatment`
  WHERE true
    AND created_date >= "2022-01-01" and created_date < "2025-01-01"
    AND entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HS_SA', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
  GROUP BY entity_id, test_id, variant
), 
orders as (
  SELECT
    o.entity_id,
    o.test_id,
    o.test_variant variant,
    c.users users_control,
    v.users users_variation,
    count(distinct o.platform_order_code) raw_orders,
    count(distinct o.platform_order_code) * (c.users / v.users) orders,    
    sum(o.fully_loaded_gross_profit_eur * c.users / v.users) flgp_eur,
    sum(o.fully_loaded_gross_profit_eur) / count(distinct o.platform_order_code) flgpo_eur
  FROM `fulfillment-dwh-production.curated_data_shared.dps_test_orders` o
  LEFT JOIN users c ON o.entity_id = c.entity_id AND c.test_id = o.test_id AND c.variant = 'Control'
  LEFT JOIN users v ON o.entity_id = v.entity_id AND v.test_id = o.test_id AND v.variant = o.test_variant
  WHERE true
    AND o.created_date >= "2022-01-01" and o.created_date < "2025-01-01"
    AND o.entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HS_SA', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
    AND is_sent = True
    AND is_own_delivery = True
    AND vendor_vertical_parent in ('Restaurant','restaurant','restaurants')
  GROUP BY o.entity_id, o.test_id, o.test_variant,c.users, v.users
  HAVING c.users > 0 AND v.users > 0
  ORDER BY o.test_id, o.entity_id
),  
stats as (
  SELECT
    c.entity_id,
    c.test_id,
    v.variant variation,
    c.users_control,
    v.users_variation,
    c.raw_orders raw_orders_control,
    v.raw_orders raw_orders_variation,
    c.orders orders_control,
    v.orders orders_variation,
    c.flgpo_eur flgpo_eur_control,
    v.flgpo_eur flgpo_eur_variation,
    c.flgp_eur flgp_eur_control,
    v.flgp_eur flgp_eur_variation,
    c.flgp_eur / c.users_control flgpu_control,
    v.flgp_eur / v.users_control flgpu_variation,
    v.orders - c.orders incremental_orders,
    v.flgp_eur - c.flgp_eur incremental_profit,
    (v.orders - c.orders) / NULLIF(c.orders, null) order_change_pct, 
    (v.flgp_eur - c.flgp_eur) / NULLIF(c.flgp_eur, 0) profit_change_pct, 
    c.raw_orders / NULLIF(c.users_control, 0) orders_per_user_control,
    v.raw_orders / NULLIF(v.users_variation, 0) orders_per_user_variation, 
    RANK() OVER (PARTITION BY c.entity_id, c.test_id ORDER BY (v.orders - c.orders) / NULLIF(c.orders, 0) DESC) AS order_rank,
    RANK() OVER (PARTITION BY c.entity_id, c.test_id ORDER BY (v.flgp_eur - c.flgp_eur) / NULLIF(c.flgp_eur, 0) DESC) AS profit_rank,
    case 
      when c.orders = v.orders and c.flgpo_eur = v.flgpo_eur
      then 'Neutral'
      when c.orders <= v.orders and ((c.flgpo_eur <= v.flgpo_eur) or (c.flgp_eur < v.flgp_eur))
      then 'Win-Win'
      when c.orders >= v.orders and c.flgp_eur >= v.flgp_eur
      then 'Lose-Lose'
      when c.orders > v.orders and c.flgp_eur < v.flgp_eur
      then 'MPOL - Profit at a Cost'
      when c.orders < v.orders and c.flgp_eur > v.flgp_eur
      then 'CPiO - Growth at a Cost'
    end scenario  
  FROM orders c
  LEFT JOIN orders v ON c.entity_id = v.entity_id AND c.test_id = v.test_id AND v.variant <> 'Control'
  WHERE c.variant = 'Control'
    AND c.orders > 0
    AND v.orders > 0
),
winning_variants AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
    PARTITION BY entity_id, test_id 
    ORDER BY 
        CASE WHEN scenario = 'Win-Win' THEN 1 ELSE 2 END, -- Order 'Win-Win' rows first
        order_rank ASC,
        profit_rank ASC
    ) AS final_rank
  FROM stats
),
experiments AS (
  SELECT distinct 
    entity_id,
    country_code,
    test_id,
    test_name,
    hypothesis,
    objective,
    cast(date_trunc(test_start_date, MONTH) as date) start_month,
    cast(date_trunc(test_start_date, ISOWEEK) as date) start_week,
    cast(date_trunc(test_end_date, MONTH) as date) end_month,
    cast(date_trunc(test_end_date, ISOWEEK) as date) end_week,
    test_start_date,
    test_end_date,
    TIMESTAMP_DIFF(IFNULL(test_end_date, CURRENT_TIMESTAMP() - INTERVAL 1 DAY), test_start_date, DAY) test_length,
    is_active,
    variation_share
    --case when test_id = 86 and entity_id = 'PY_SV' then 1 else 0 end check
  FROM `fulfillment-dwh-production.curated_data_shared.dps_experiment_setups`
  WHERE NOT misconfigured
    AND entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HS_SA', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
    AND is_already_executed
    AND experiment_type = 'AB'
    AND test_start_date >= '2022-01-01' and test_start_date < "2025-01-01"
    AND ('restaurant' IN UNNEST(test_vertical_parents) OR 'Restaurant' IN UNNEST(test_vertical_parents) 
    OR  'restaurants' IN UNNEST(test_vertical_parents))
    AND test_name NOT LIKE '%FDNC%'
)
SELECT
  c.region,
  c.brand_name,
  c.country_name,
  e.*,
  w.* except(entity_id, test_id),
  w.orders_per_user_variation -  w.orders_per_user_control orders_per_user_incremental,
  w.flgpu_variation -  w.flgpu_control flgp_per_user_incremental
FROM experiments e
INNER JOIN winning_variants w ON e.entity_id = w.entity_id AND e.test_id = w.test_id 
left join 
          (
            select DISTINCT
                    p.entity_id,
                    c.country_name,
                    c.region,
                    p.brand_name
            from `fulfillment-dwh-production.cl.countries` c
            left join unnest(c.platforms) p
          ) c on c.entity_id = e.entity_id
--where check = 0
ORDER BY e.entity_id, e.test_id
"""



seasonal = """
with session as (
SELECT   ss.entity_id
        ,EXTRACT(YEAR FROM created_date) AS Year
        ,DATE_TRUNC(created_date, MONTH) Month
        ,count(distinct sessions.perseus_client_id) total_users
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_ga_sessions` ss
where entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
and created_date >= '2023-01-01' and created_date <= '2023-05-31'
group by 1,2,3
UNION ALL
SELECT   ss.entity_id
        ,EXTRACT(YEAR FROM created_date) AS Year
        ,DATE_TRUNC(created_date, MONTH) Month
        ,count(distinct sessions.perseus_client_id) total_users
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_perseus_sessions` ss
where entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
and created_date >= '2023-06-01' and created_date < "2024-12-01"
group by 1,2,3
), 
orders as (
SELECT   o.entity_id
        ,EXTRACT(YEAR FROM created_date_local) AS Year
        ,DATE_TRUNC(created_date_local, MONTH) Month
        ,sum(fully_loaded_gross_profit_eur) fully_loaded_gross_profit_eur
        ,count(distinct platform_order_code) total_orders
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` o
where o.entity_id in ('AP_PA', 'PY_AR', 'PY_BO', 'PY_CL', 'PY_CR', 'PY_DO', 'PY_EC', 'PY_GT', 'PY_HN', 'PY_NI', 'PY_PE', 'PY_PY', 'PY_SV', 'PY_UY', 'PY_VE', 'FP_BD', 'FP_HK', 'FP_KH', 'FP_LA', 'FP_MM', 'FP_MY', 'FP_PH', 'FP_PK', 'FP_SG', 'FP_TH', 'FP_TW', 'DJ_CZ', 'FO_NO', 'FP_SK', 'MJM_AT', 'NP_HU', 'OP_SE', 'PO_FI', 'FY_CY', 'EF_GR', 'HF_EG', 'TB_AE', 'TB_BH', 'TB_IQ', 'TB_JO', 'TB_KW', 'TB_OM', 'TB_QA', 'YS_TR')
AND is_sent = True
AND is_own_delivery = True
AND vendor_vertical_parent in ('Restaurant','restaurant','restaurants')
AND created_date >= '2022-12-31' and created_date <= '2024-12-02'
AND created_date_local >= '2023-01-01' and created_date_local < "2024-12-01"
group by 1,2,3
)
select   s.entity_id
        ,s.YEAR
        ,s.Month
        ,s.total_users
        ,o.fully_loaded_gross_profit_eur
        ,o.total_orders
        ,fully_loaded_gross_profit_eur / total_users flgpu_eur
        ,total_orders / total_users orders_per_user
from session s
left join orders o on o.entity_id = s.entity_id and o.Month = s.Month
"""



significance = """
WITH filtered_data AS (
    SELECT 
        country_code
        ,test_name
        ,group_b variation
        ,max(case when kpi_label = 'orders_per_user' then mean_a else null end) AS orders_per_user_control
        ,max(case when kpi_label = 'orders_per_user' then incremental else null end) AS incremental_orders_per_user
        ,max(case when kpi_label = 'orders_per_user' then significance else null end) AS significance_orders_per_user
        ,max(case when kpi_label = 'orders_per_user' then rn else null end) AS rn_orders_per_user
        ,max(case when kpi_label in ('fully_loaded_gross_profit_local_per_user', 'profit_local_per_user') then mean_a else null end) AS profit_per_user_control
        ,max(case when kpi_label in ('fully_loaded_gross_profit_local_per_user', 'profit_local_per_user') then incremental else null end) AS incremental_profit_per_user
        ,max(case when kpi_label in ('fully_loaded_gross_profit_local_per_user', 'profit_local_per_user') then significance else null end) AS significance_profit_per_user
        ,max(case when kpi_label in ('fully_loaded_gross_profit_local_per_user', 'profit_local_per_user') then rn else null end) AS rn_profit_per_user
        ,max(case when kpi_label = 'fully_loaded_gross_profit_local_per_user' then mean_a else null end) AS fully_loaded_gross_profit_local_per_user_control
        ,max(case when kpi_label = 'fully_loaded_gross_profit_local_per_user' then incremental else null end) AS incremental_fully_loaded_gross_profit_local_per_user
        ,max(case when kpi_label = 'fully_loaded_gross_profit_local_per_user' then significance else null end) AS significance_fully_loaded_gross_profit_local_per_user
        ,max(case when kpi_label = 'fully_loaded_gross_profit_local_per_user' then rn else null end) AS rn_fully_loaded_gross_profit_local_per_user
    FROM (
        SELECT 
             country_code
            ,test_name
            ,kpi_label
            ,mean_a
            ,group_b
            ,LEAST(p_value, corrected_p_value, cuped_p_value, corrected_cuped_p_value) AS min_p_value
            ,CASE 
                WHEN LEAST(p_value, corrected_p_value, cuped_p_value, corrected_cuped_p_value) <= 0.05 THEN 1 
                ELSE 0 
            END AS significance
            ,(mean_b - mean_a) AS incremental
            ,ROW_NUMBER() OVER (
                PARTITION BY test_name, kpi_label
                ORDER BY 
                    CASE 
                        WHEN LEAST(p_value, corrected_p_value, cuped_p_value, corrected_cuped_p_value) <= 0.05 THEN 1 
                        ELSE 0 
                    END DESC,
                    (mean_b - mean_a) DESC
            ) AS rn
        FROM 
            `fulfillment-dwh-production.rl.dps_ab_test_significance_dataset_temp`
        WHERE processing_status = 'success'
            AND group_a = 'Control'
            AND group_b != 'Control'
            AND kpi_label IN ('orders_per_user', 'fully_loaded_gross_profit_local_per_user', 'profit_local_per_user')
            AND label = 'All'
            AND test_name NOT LIKE '%FDNC%'
    )
    GROUP BY country_code, test_name, group_b
),
ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
        PARTITION BY country_code, test_name
        ORDER BY 
            CASE 
                WHEN significance_orders_per_user = 1 AND significance_profit_per_user = 1 THEN 1
                WHEN significance_orders_per_user = 1 THEN 3
                WHEN significance_profit_per_user = 1 THEN 2
                WHEN incremental_orders_per_user > 0 AND incremental_profit_per_user > 0 THEN 4
                ELSE 5
            END ASC,
            CASE 
                WHEN incremental_orders_per_user > 0 AND incremental_profit_per_user > 0 THEN NULL
                ELSE incremental_orders_per_user
            END DESC,
            significance_orders_per_user DESC
    ) AS rank
    FROM filtered_data
)
SELECT  *
FROM ranked_data
--where rank = 1
"""


hungersta_seasonal = """
with session as (
SELECT   ss.entity_id
        ,EXTRACT(YEAR FROM created_date) AS Year
        ,DATE_TRUNC(created_date, MONTH) Month
        ,count(distinct sessions.perseus_client_id) total_users
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_ga_sessions` ss
where entity_id = 'HS_SA'
and created_date >= '2023-01-01' and created_date <= '2023-05-31'
group by 1,2,3
UNION ALL
SELECT   ss.entity_id
        ,EXTRACT(YEAR FROM created_date) AS Year
        ,DATE_TRUNC(created_date, MONTH) Month
        ,count(distinct sessions.perseus_client_id) total_users
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_perseus_sessions` ss
where entity_id ='HS_SA'
and created_date >= '2023-06-01' and created_date < "2024-12-01"
group by 1,2,3
),
orders as (
select entity_id,
       Year,
       Month,
       flgp_eur + (rdf_adjusted_df - delivery_fee_eur) AS Adjusted_Flgp_eur,
       flgp_eur non_adjusted_flgp,
       total_orders
from (
SELECT 
    o.entity_id,
    EXTRACT(YEAR FROM created_date_local) AS Year,
    DATE_TRUNC(created_date_local, MONTH) Month,
    count(distinct o.platform_order_code) total_orders,
    SUM(rdf.OD_delivery_fee / ex.exchange_rate) AS converted_OD_delivery_fee,
    SUM(
        COALESCE((rdf.OD_delivery_fee + 4.7) / ex.exchange_rate, o.delivery_fee_eur)
    ) AS rdf_adjusted_df,
    SUM(o.delivery_fee_eur) delivery_fee_eur,
    SUM(o.revenue_eur) AS total_revenue,
    SUM(o.revenue_eur) - SUM(o.delivery_costs_eur) gross_profit_eur,
    SUM(o.revenue_eur) - SUM(o.delivery_costs_eur) - SUM(o.discount_dh_eur) - SUM(o.voucher_dh_eur) AS flgp_manual_eur,
    SUM(o.delivery_costs_eur) + SUM(o.discount_dh_eur) + SUM(o.voucher_dh_eur) total_costs,
    SUM(o.fully_loaded_gross_profit_eur) AS flgp_eur,
    (
        SUM(o.joker_vendor_fee_eur)
        + SUM(commission_eur)
        + SUM(o.priority_fee_eur * (1 - o.vat_rate))
        + SUM(o.service_fee_eur * (1 - o.vat_rate))
        + SUM(o.mov_customer_fee_eur * (1 - o.vat_rate))
        + SUM(o.front_margin_eur)
        + SUM(o.delivery_fee_eur * (1 - o.vat_rate))
    ) AS manual_revenue,
    (
        SUM(o.joker_vendor_fee_eur)
        + SUM(commission_eur)
        + SUM(o.priority_fee_eur * (1 - o.vat_rate))
        + SUM(o.service_fee_eur * (1 - o.vat_rate))
        + SUM(o.mov_customer_fee_eur * (1 - o.vat_rate))
        + SUM(o.front_margin_eur)
        + SUM(
        COALESCE((rdf.OD_delivery_fee + 4.7) / ex.exchange_rate, o.delivery_fee_eur)
         * (1 - o.vat_rate))
    ) AS adjusted_manual_revenue
--     o.created_date,
--     o.platform_order_code,
--     o.delivery_fee_eur,
--     rdf.OD_delivery_fee,
--     ex.exchange_rate,
--     o.vat_rate
FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` o
left JOIN `logistics-data-storage-staging.long_term_pricing.hs_sa_rdf_orders` rdf ON CAST(rdf.platform_order_code AS STRING) = o.platform_order_code
LEFT JOIN (
    SELECT 
        date,
        currency_code, 
        max(exchange_rate) exchange_rate
    FROM 
        `fulfillment-dwh-production.cl._currency_exchange`
    WHERE 
        country_code = 'sa' 
    GROUP BY 1,2
) ex ON ex.currency_code = o.currency_code and ex.date = o.created_date
WHERE o.entity_id = 'HS_SA'
      AND is_sent = True
      AND is_own_delivery = True
      AND created_date >= '2022-12-31' and created_date < "2024-12-02"
      AND created_date_local >= '2023-01-01' and created_date_local < "2024-12-01"
      AND vendor_id in (
                select distinct vendor_id
                from (
                SELECT 
                    vendor_id,
                    COALESCE(
                    FIRST_VALUE(vendor_vertical_parent IGNORE NULLS) OVER (
                        PARTITION BY vendor_id
                        ORDER BY created_date DESC
                    ),
                    FIRST_VALUE(vertical_type IGNORE NULLS) OVER (
                        PARTITION BY vendor_id
                        ORDER BY created_date DESC
                    )
                ) AS selected_vertical
            FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders`
            WHERE entity_id = 'HS_SA'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY created_date DESC) = 1
            )
            where selected_vertical in('Restaurant','restaurant','restaurants','Restaurants')
) 
group by 1,2,3
))
select   s.entity_id
        ,s.YEAR
        ,s.Month
        ,s.total_users
        ,o.Adjusted_Flgp_eur fully_loaded_gross_profit_eur
        ,o.total_orders
        ,o.Adjusted_Flgp_eur / s.total_users flgpu_eur
        ,o.total_orders / s.total_users orders_per_user
        ,o.non_adjusted_flgp
from session s
left join orders o on o.entity_id = s.entity_id and o.Month = s.Month
"""