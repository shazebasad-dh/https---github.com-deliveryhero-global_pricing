select
    de.rdbms_id                                                  as rdbms_id,
    c.country_name                                               as country,
    de.entity_display_name                                       as entity_display_name,
    lc.name                                                      as city,
    de.city_id                                                   as city_id,
    z.name                                                       as zone,
    o.zone_id                                                    as zone_id,
    v.vertical_type                                              as vertical,
    de.rider_dropped_off_at::date                                as date,
    de.delivery_id                                               as delivery_id,
    (de.dropoff_distance_manhattan)                              as dropoff_distance_manhattan,
    ST_DistanceSphere(
        ST_GeomFromText(de.pickup),
        ST_MakePoint(
            ST_X(ST_GeomFromText(de.pickup)),
            ST_Y(ST_GeomFromText(de.dropoff)))) +
    ST_DistanceSphere(
        ST_GeomFromText(de.dropoff),
        ST_MakePoint(
            ST_X(ST_GeomFromText(de.pickup)),
            ST_Y(ST_GeomFromText(de.dropoff))))                  as manual_manhattan_pickup_2_dropoff,
    ST_DistanceSphere(
        ST_GeomFromText(v.location),
        ST_MakePoint(
            ST_X(ST_GeomFromText(v.location)),
            ST_Y(ST_GeomFromText(o.customer_location)))) +
    ST_DistanceSphere(
        ST_GeomFromText(o.customer_location),
        ST_MakePoint(
            ST_X(ST_GeomFromText(v.location)),
            ST_Y(ST_GeomFromText(o.customer_location))))         as manual_manhattan_vendor_2_customer,
    ST_DistanceSphere(
        ST_GeomFromText(de.dropoff),
        ST_GeomFromText(o.customer_location))                    as dropoff_2_customer,
    ST_DistanceSphere(
        ST_GeomFromText(de.pickup),
        ST_GeomFromText(v.location))                             as pickup_2_vendor,
    1                                                            as completed_deliveries
from dwh_redshift_logistic.v_clg_orders o
left join dwh_redshift_logistic.v_clg_cities lc using(rdbms_id, city_id)
left join dwh_redshift_logistic.v_clg_zones z using(rdbms_id, city_id, zone_id)
left join dwh_redshift_logistic.v_clg_countries c using(rdbms_id)
left join dwh_redshift_logistic.v_clg_deliveries de using(rdbms_id, entity_display_name, order_id)
left join dwh_redshift_logistic.v_clg_vendors v on o.country_code = v.country_code and o.vendor_id = v.hurrier_id
where de.delivery_status = 'completed'
    and de.rider_dropped_off_at between current_date - 7 and current_date
    and c.region = 'Americas'