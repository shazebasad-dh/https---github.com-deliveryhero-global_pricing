-- Date:            2021/03/22
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Pasquale
-- Comment:         

SELECT p.entity_id
    , country_code
    , ci.name AS city_name
    , ci.id AS city_id
    , zo.shape AS zone_shape 
    , zo.name AS zone_name
    , zo.id AS zone_id
  FROM cl.countries co
  LEFT JOIN UNNEST(co.platforms) p
  LEFT JOIN UNNEST(co.cities) ci
  LEFT JOIN UNNEST(ci.zones) zo
  WHERE country_code = "my" 
