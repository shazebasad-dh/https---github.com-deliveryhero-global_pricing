DECLARE to_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);
DECLARE from_date DATE DEFAULT DATE_SUB(to_date, INTERVAL 1 WEEK);

WITH load_session_data as (

  SELECT created_date_local
  , entity_id
  , fe_session_id
  , perseus_client_id
  , ANY_VALUE(perseus_location) AS perseus_location
  , IFNULL(COUNT(DISTINCT transaction_no),0) as has_transaction

  FROM `fulfillment-dwh-production.curated_data_shared.dps_cvr_frontend_events` 
  WHERE created_date >= DATE_SUB(from_date, interval 2 DAY)
  AND created_date_local BETWEEN from_date AND to_date 
  AND entity_id = "YS_TR"
  GROUP BY 1,2,3,4
)

, load_istanbul_districts AS (
  SELECT 
    name_1 as city
    , name_2 as area_name
    , ST_GEOGFROMTEXT(geometry) AS area_shape
  FROM `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_2`
  WHERE gid_0 = "TUR"
  and name_1 = "Istanbul"
)

, add_area_info AS (
  SELECT a.*
  , b.area_name
  , b.city
  -- , b.area_shape
  FROM load_session_data a
  INNER JOIN load_istanbul_districts b
    ON ST_CONTAINS(b.area_shape, a.perseus_location)
)

, aggregate_info_per_area AS (
  SELECT
  city
    , area_name
    , perseus_client_id
    , COUNT(fe_session_id) as n_sessions
    , SUM(has_transaction) as n_conversions
    -- , ANY_VALUE(area_shape) as area_shape
  FROM add_area_info
  GROUP BY 1,2,3
)

SELECT *
FROM aggregate_info_per_area


### aggregate info
-- SELECT 
-- city
-- , AVG(n_sessions) as avg_session_per_user
-- , AVG(n_conversions) as avg_conversion_per_user
-- , VAR_SAMP(n_sessions) as var_session_per_user
-- , VAR_SAMP(n_conversions) as var_conversion_per_user
-- , COVAR_SAMP(n_sessions, n_conversions) as cov_sessions_and_conversions
-- , SAFE_DIVIDE(SUM(n_conversions),SUM(n_sessions)) as cvr
-- , COUNT(DISTINCT perseus_client_id) as n_users
-- FROM aggregate_info_per_area
-- GROUP BY 1