select * from (
  select 
    centroid_id, 
    city_name,
    count(*) orders
  from `dh-logistics-product-ops.pricing.vendor_clusters_ar_food_afv_distance`
  group by 1,2
  order by 3 desc)
  pivot (sum(orders) as orders for centroid_id in (1,2,3,4,5)
)
