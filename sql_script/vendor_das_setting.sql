-- Date:            2020/06/02
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Pasquale
-- Comment:         


select vendor_code
, h.settings.delivery_fee.amount
, TO_JSON_STRING(p.delivery_provider) delivery_provider
, max(h.drive_time)
from cl.vendors_v2 v
left join unnest(delivery_areas) de
left join unnest(history) h
left join unnest(porygon) p
where entity_id = "FP_PK" and v.is_active and h.active_to is Null and de.is_deleted = False
group by 1,2,3
order by 1
