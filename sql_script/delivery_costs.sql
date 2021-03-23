-- Date:            2021/03/23
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Xun, Trupti, Fabian
-- Comment:    

-- Xun's approach (confirmed by Trupti and Fabian)
select
  entity_id,
  country_code,
  order_id,
  sum(delivery_costs) delivery_costs
from fulfillment-dwh-production.cl.utr_timings
group by 1,2,3

-- Laurent's approach
select
    p.entity_id,
    l.platform_order_code order_code,
    sum(p.delivery_costs) delivery_costs,
  from 
    (select
      entity_id,
      country_code,
      created_date,
      order_id,
      delivery_costs,
      row_number() over(partition by entity_id, order_id order by created_date desc) as rank
    from fulfillment-dwh-production.cl.utr_timings) p
  left join fulfillment-dwh-production.cl.orders l on p.order_id = l.order_id and p.country_code = l.country_code
  where rank = 1
  group by 1,2
