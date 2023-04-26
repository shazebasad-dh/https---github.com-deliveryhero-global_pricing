with load_data as (
select 
 variant
 , variable
from `dh-logistics-product-ops.pricing._dps_ab_test_significance_orders`
LEFT JOIN UNNEST(gfv_local) variable
where test_name = "AR_20230314_R_00_O_AA_Test_MDQ_v2"
-- and treatment = TRUE
)

, calculate_outliers as (
  SELECT
  variant
  , approx_quantiles(variable, 100)[OFFSET(1)] as perc_1
  , approx_quantiles(variable, 100)[OFFSET(99)] as perc_99
  from load_data
  group by 1

)

, filter_outliers as (
  select a.*
  from load_data a
  INNER JOIN calculate_outliers b
    on a.variant = b.variant
    AND a.variable BETWEEN b.perc_1 AND b.perc_99
)


select 
variant
, avg(variable) mean
, count(variable) as value_counts
, stddev_samp(variable) as std_dev
from filter_outliers
group by 1