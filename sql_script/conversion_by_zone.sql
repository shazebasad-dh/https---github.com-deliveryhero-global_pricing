with zones as (
  select country_name, c.name city, z.name zone, z.shape
  from cl.countries
  left join unnest (cities) c
  left join unnest (zones) z
  where country_code in ('cl','ar'))

select
  date,
  platform,
  estimatedDeliveryTime,
  z.country_name,
  z.city,
  z.zone,
  count(*) sessions,
  count(timestartlist) sessions_listing,
  count(timestartmenu) sessions_menu,
  sum(case when istransaction then 1 end) transactioned,
  sum(totaltransactions) totaltransactions,
  sum(NoShopsFirstShown) SumNoShopsFirstShown,
  count(NoShopsFirstShown) SessionsWithShopsFirst,
  sum(MaxShopsShown) SumMaxShopsShown,
  count(MaxShopsShown) SessionsWithMaxShopsShown
FROM `dhh-digital-analytics-dwh.shared_views_to_pricing_team.sessions_location_details` l
join zones z on ST_CONTAINS(z.shape,ST_GEOGPOINT(CAST(l.cdLongitude as FLOAT64),CAST(l.cdLatitude as FLOAT64)))
where
  partitiondate >= '2020-04-01'
  and country in ('Chile', 'Argentina')
  and safe_cast(cdLongitude as float64) is not null
  and safe_cast(cdLatitude as float64) is not null
group by 1,2,3,4,5,6
order by 1,2,3,4,5,6