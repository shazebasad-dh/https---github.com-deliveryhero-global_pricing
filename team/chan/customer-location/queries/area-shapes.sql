  SELECT 
      name_1 as city
      , name_2 as area_name
      , geometry AS area_shape
  FROM `logistics-data-storage-staging.long_term_pricing.gadm_geo_spatial_data_level_2`
  WHERE gid_0 = "TUR"
  and name_1 = "Istanbul"