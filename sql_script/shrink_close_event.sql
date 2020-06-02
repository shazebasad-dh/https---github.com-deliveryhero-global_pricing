-- Date:            2020/06/02
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Daniel
-- Comment:

SELECT
p.city_id
, p.created_date
, t.zone_id
, t.activation_threshold
, t.deactivation_threshold
, t.action
, t.value
FROM `fulfillment-dwh-production.cl.porygon_events` p
left join unnest(transactions) t
where country_code = "my"
