-- Date:            2020/06/02
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Pasquale
-- Comment:


SELECT
  ci.name as city_name,
  zo.name as zone_name,
  se.drive_time,
  se.delivery_fee.amount as delievery_fee
FROM
  `fulfillment-dwh-production.cl.countries` co
  , co.cities ci
  , ci.zones zo
  , zo.default_delivery_area_settings se
WHERE country_code = 'my'
order by 1,2,3
