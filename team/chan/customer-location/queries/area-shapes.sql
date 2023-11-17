SELECT 
  country_code
  , city_name as city
  , raw_admin_area_name as area_name
  , admin_area_geometry as area_shape

FROM `logistics-data-storage-staging.long_term_pricing.customer_location_admin_area_data`
WHERE country_code = "{country_code}"
AND city_name = "{city}"