-- Date:            2020/06/02
-- Contributor:     Yuzhu Zhang
-- Enviroment:      Bigquery
-- BI consultant:   Pasquale
-- Comment:         

SELECT region
   -- i'm using rider_dropped_off_at which is the delivery date , this is more accurate than the creation date, because of normally we check the date when order is delivered not created
  , FORMAT_DATE('%Y-%m', DATE(d.rider_dropped_off_at, o.timezone)) AS report_month
  -- checking delivery time (need to / 60 because all timings are in seconds) , also exclude pre orders as due to their advanced creation timestamp they should not be really part of delivery time
  , SUM(IF(is_preorder IS FALSE, o.timings.actual_delivery_time / 60, NULL)) AS delivery_time_numerator
  , COUNT(IF(is_preorder IS FALSE, o.timings.actual_delivery_time / 60, NULL)) AS delivery_time_denominator
FROM cl.orders o
LEFT JOIN UNNEST (deliveries) d  ON is_primary  -- this filter is to count only on order level and not delivery level, because normally the deliveruy time is when the first delivery is delivered (is_primary flag for orders having multiple deliveries )
WHERE d.delivery_status = 'completed'
GROUP BY 1, 2
