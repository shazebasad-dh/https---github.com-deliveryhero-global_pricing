-- Date:            2020/06/02
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Daniel
-- Comment:

SELECT
  e.*
FROM
  `fulfillment-dwh-production.cl.countries` co
  , co.cities ci
  , ci.zones zo
  , zo.events e
WHERE country_code = 'hk'
