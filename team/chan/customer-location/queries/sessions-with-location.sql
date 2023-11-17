DECLARE to_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

-- -- testing args
-- DECLARE entity_id_filter STRING DEFAULT "PY_BO";
-- DECLARE country_code_filter STRING DEFAULT "bo";
-- DECLARE city_id_filter INT64 DEFAULT 1;
-- DECLARE city_filter STRING DEFAULT "Santa Cruz";
-- DECLARE weeks_ago INT64 DEFAULT 1;
-- DECLARE assignment_id_filter ARRAY<INT64> DEFAULT [8];


-- --template args
DECLARE entity_id_filter STRING DEFAULT "{entity_id}";
DECLARE country_code_filter STRING DEFAULT "{country_code}";
DECLARE city_filter STRING DEFAULT "{city}";
DECLARE weeks_ago INT64 DEFAULT {weeks_ago};
DECLARE city_id_filter INT64 DEFAULT {city_id};
DECLARE assignment_id_filter ARRAY<INT64> DEFAULT {asa_ids};





DECLARE from_date DATE DEFAULT DATE_SUB(to_date, INTERVAL weeks_ago WEEK);


WITH 
  load_assignment_data as (

  SELECT DISTINCT vendor_code
  FROM `fulfillment-dwh-production.curated_data_shared.vendor_asa_configuration_versions`
  LEFT JOIN UNNEST(dps_asa_configuration_history) asa
  WHERE active_to IS NULL
  AND asa_id IN UNNEST(assignment_id_filter)
  AND entity_id = entity_id_filter

)

, load_session_data as (

  SELECT created_date_local
  , entity_id
  , fe_session_id
  , perseus_client_id
  , ANY_VALUE(perseus_location) AS perseus_location
  , IFNULL(COUNT(DISTINCT transaction_no),0) as has_transaction

  FROM `fulfillment-dwh-production.curated_data_shared.dps_cvr_frontend_events` ses
  LEFT JOIN load_assignment_data asa
    ON ses.vendor_code = asa.vendor_code
  WHERE created_date >= DATE_SUB(from_date, interval 2 DAY)
  AND created_date_local BETWEEN from_date AND to_date 
  AND (
    CASE 
      WHEN ses.vendor_code IS NULL THEN TRUE
      ELSE asa.vendor_code IS NOT NULL
    END
  )
  GROUP BY 1,2,3,4
)

, load_dh_city_shape as (
     SELECT
        p.entity_id
        , ci.id as city_id
        , ci.name as city_name
        , ci.updated_at
        , ST_UNION_AGG(z.shape) as city_shape
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(zones) z
    WHERE ci.is_active
    AND z.is_active
    AND p.entity_id IS NOT NULL
    AND p.entity_id = entity_id_filter
    AND ci.id = city_id_filter
    AND ci.shape IS NOT NULL
    GROUP BY 1,2,3,4
    QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id, city_id ORDER BY ci.updated_at DESC) = 1
)

-- , load_city_shapes AS (
-- SELECT 
--   country_code
--   , admin_area_names.level_1 as city
--   , CASE data_source 
--       WHEN "gadm_level_3" THEN admin_area_names.level_3
--       WHEN "gadm_level_2" THEN admin_area_names.level_2
--       ELSE admin_area_names.level_1
--   END as area_name
--   , admin_area_geometry as area_shape

-- FROM `logistics-data-storage-staging.temp_pricing.customer_location_admin_area_data`
-- WHERE country_code = country_code_filter
-- AND admin_area_names.level_1 = city_filter
-- )

, add_area_info AS (
  SELECT a.* EXCEPT(perseus_location)
  , c.city_name as city
  -- , b.area_name
  -- , perseus_location
  , ST_GEOGPOINT(
      ROUND(ST_X(a.perseus_location), 5)
      , ROUND(ST_Y(a.perseus_location), 5)
   ) as perseus_location 

  FROM load_session_data a

  INNER JOIN load_dh_city_shape c
    ON ST_CONTAINS(c.city_shape, a.perseus_location)

  -- INNER JOIN load_city_shapes b
  --   ON ST_CONTAINS(b.area_shape, a.perseus_location)

)

, aggregate_info_per_area AS (
  SELECT
  city
    , perseus_client_id
    , ST_ASTEXT(perseus_location) AS perseus_location
    , COUNT(fe_session_id) as n_sessions
    , SUM(has_transaction) as n_conversions
  FROM add_area_info
  GROUP BY 1,2,3
)


SELECT *
FROM aggregate_info_per_area a