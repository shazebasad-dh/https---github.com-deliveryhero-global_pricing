-- zone_temp to get basic zone table information
with zone_temp as (
	select
		country_code,
		c.id as city_id,
		c.name as city_name,
		z.id as zone_id,
		z.name as zone_name,
		st_astext(z.shape) as zone_shape,
		z.zone_shape_updated_at as zone_update,
	from `fulfillment-dwh-production.cl.countries`
	left join unnest(platforms) p
	left join unnest(cities) c
	left join unnest(c.zones) z
	where country_code = "tw"
	and c.id = 3
	group by 1,2,3,4,5,6,7),

-- zone table to ge the most uptodate zone shape
zone as (
	select a.*
	from zone_temp a
	join
		(select
			country_code,
			platform,
			city_id,
			zone_id,
			zone_name,
			zone_shape,
			max(zone_update) as zone_update
		from zone_temp
		group by 1,2,3,4,5,6) b
	on a.country_code = b.country_code and a.platform = b.platform and a.city_id = b.city_id and a.zone_id = b.zone_id and a.zone_update = b.zone_update),

-- res_location table to get restaurant location info
res_location as
	(select
		country_code,
		city_id,
		vendor_code,
		st_x(location) as res_long,
		st_y(location) as res_lat,
		d.platform as platform
	from `fulfillment-dwh-production.cl.vendors`
	left join unnest(delivery_areas) d
	where country_code = "tw"
	and city_id = 3
	group by 1,2,3,4,5,6),

-- res_zone to map restaurant with zone_id by looking at restaurant location and up-to-date zone shape
res_zone as (
	select
		z.country_code,
		z.city_id,
		vendor_code,
		z.platform as platform,
		z.zone_id  as res_zone_id
	from zone z
	inner join res_location r
	on z.country_code = r.country_code
	and z.city_id = r.city_id
	and st_within(st_geogpoint(r.res_long,r.res_lat), st_geogfromtext(z.zone_shape))),

-- each restaurant should has its zone_id, restaurant could have more than one zone_id
res_zoneid as(
	select
		country_code,
		city_id,
		vendor_code,
		platform,
		res_zone_id
	from res_zone
	where true
	and res_zone_id is not null
	group by 1,2,3,4,5),

--add porygon vehicle_profile by restaurant
asdas

-- porygon_temp table to have up to date porygon area
porygon_temp as(
	select
		a.restaurant_id,
		a.time,
		a.country_code,
		a.platform,
		a.shape_wkt
	from `fulfillment-dwh-production.dl.porygon_drive_time_polygons` a
	inner join
		(select
			restaurant_id,
			time,
			country_code,
			platform,
			shape_wkt,
			vehicle_profile,
			max(updated_at) as updated_at
		from `fulfillment-dwh-production.dl.porygon_drive_time_polygons`
		where vehicle_profile = "default" and is_fallback = false and country_code = "tw"
		group by 1,2,3,4,5,6) b
	on a.restaurant_id = b.restaurant_id and a.country_code = b.country_code and a.platform = b.platform and a.updated_at = b.updated_at
	group by 1,2,3,4,5),

-- make the timeframe dynamic (last x days?)

-- porygon table to have zone_id for each restaurant and up-to-date shape for different driving times with car-reduced
porygon as (
	select
		p.*,
		v.city_id,
		r.res_zone_id
	from porygon_temp p
	inner join
		(select
			vendor.vendor_code as vendor_code,
			city_id,
			platform,
			country_code
		from `fulfillment-dwh-production.cl.orders`
		where cast(order_placed_at as date) >= "2020-01-01"
			and country_code = "tw"
			and city_id = 3 -- Taipei
	group by 1,2,3,4) v
	on p.restaurant_id = v.vendor_code and lower(p.country_code) = lower(v.country_code) and lower(p.platform) = lower(v.platform)
	inner join res_zoneid r
	on p.restaurant_id = r.vendor_code and lower(p.country_code) = lower(r.country_code) and lower(p.platform) = lower(r.platform)),


-- get all user location located in zone 38
user_location as(
	select
		ST_X(customer.location) as user_lon, --> check if we can change this to dropoff location
		ST_Y(customer.location) user_lat, --> check if we can change this to dropoff location
		zone_id
	from `fulfillment-dwh-production.cl.orders`
	where city_id = 3 -- Taipei
		and country_code = "tw"
		and zone_id is not null
		and cast(order_placed_at as date) between "2020-03-23" and "2020-04-05"
	group by 1,2,3),

-- sample certain user amount
user_geohash as(
	select
		st_geohash(st_geogpoint(user_lon, user_lat),6) as geohash, --> 6: Area width 1.2km x 609.4m height
		zone_id as user_zone_id,
		count(*) as weight
	from user_location
	group by 1,2),

first_match as (
	select
		u.*,
		p.*
	from user_geohash u
	cross join porygon p
	where u.user_zone_id = p.res_zone_id and st_within(st_geogpointfromgeohash(u.geohash), safe.ST_GEOGFROMTEXT(p.shape_wkt)))

-- final results
select
	user_zone_id,
	geohash,
	st_astext(st_geogpointfromgeohash(geohash)) as geohash_center,
	restaurant_id,
	avg(weight) as weight,
	min(time) as porygon_dt
from first_match
group by 1,2,3,4