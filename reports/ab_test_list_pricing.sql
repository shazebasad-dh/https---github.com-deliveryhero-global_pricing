with first_logic as(


select *, case when test_end_date is null then 'live' when test_end_date> current_date()  then 'live' else 'finished' end is_live
from(
select * , ROW_NUMBER() OVER(Partition by test_name,start_week ORDER BY test_name,start_week,Test_approach, distinc_zones desc ) order_num
from 
(


select a.*, case when b.test_start_date is not null then 'Experimentation III' else 'Other' end Test_approach, 
case when b.test_start_date is not null then 1 else 0 end counter_exp3
from
(
select *,extract(isoweek from test_start_date) as start_week,1 counter, current_date()	update_date
from
(select * from

(
select a.* 
from 
(select region, country_code,test_name, test_start_date,test_end_date
,extract(year from test_start_date) as start_year
,extract(month from test_start_date) as start_month
--,date_diff(current_date(), test_end_date, day) finalizado
, count(distinct zone_id) distinc_zones 
,ARRAY_TO_STRING(ARRAY_AGG(cast(zone_id as string) ORDER BY zone_id), ', ') AS concat_zones
,ARRAY_TO_STRING(ARRAY_AGG( distinct  cast(city_name as string) ORDER BY city_name), ', ') AS concat_cities
, date_diff(ifnull(test_end_date,current_date()), test_start_date, day)+1 as lenght_days
, case when date_diff(current_date(), test_end_date, day)  between 0 and 7 then ' Finalized Last Week' when test_end_date<current_date() then 'Finished'   else 'Running' end test_current_status
, case when test_start_date is null then 'no start_date' 
when test_end_date is not null and date_diff(test_end_date, test_start_date,day)+1 <7 then 'Short test'
when test_end_date is not null and date_diff(test_end_date, test_start_date,day)+1 >90 then 'Long test'
when test_end_date is null then 'without end_date' else 'Ok' end warnings
from (
select distinct base.* , b.city_name  -- , lower(b.city_name) as city_name , b.city_name as city_name2 
from
(
select * from (
SELECT
DISTINCT c.region,
dps.country_code,
zone_id,
test_start_date,
test_end_date,
test_name,
FROM `fulfillment-dwh-production.dl.dps_ab_test_setups_imported` dps
LEFT JOIN `fulfillment-dwh-production.cl.countries` c ON dps.country_code = c.country_code
WHERE TRUE
AND test_name IS NOT NULL
union distinct SELECT  distinct 'Americas',country, Zone_Id,Start_date,	End_Date, Test_Name
FROM `peya-delivery-and-support.user_martin_fourcade.Test_Names`  -- important table to disconect once regional start to use paralell testing
where Test_Name is not null

union distinct SELECT  distinct 'Asia',ifnull(cou.country_code,asi.country), null,Start_date,	End_Date, concat(NA,' ',Test_Name)
FROM `dh-logistics-product-ops.pricing.ab_tes_complementary_APAC` asi
left join (
SELECT distinct country_name, lower(country_iso) country_code FROM `fulfillment-dwh-production.cl.countries` 
    )cou
    on asi.country=cou.country_name
where Test_Name is not null
-- union distinct select distinct 'MENA',lower(country_code), null Zone_Id,Start_date,	End_Date, Test_Name
-- FROM `dh-logistics-product-ops.pricing.ab_tes_complementary_MENA`
-- where Test_Name is not null 
)
union distinct 

select b.region ,a.country_code	,zone_ids,test_start_date	,test_end_date,test_name	
from(select  distinct country_code,zone_ids,test_name,cast(test_start_date as date)test_start_date, cast(test_end_date as date)test_end_date , is_active
from `fulfillment-dwh-production.cl.dps_experiment_setups`, unnest(zone_ids) zone_ids)  a
left join ( select distinct region, country_code FROM `fulfillment-dwh-production.cl.countries`)b
on a.country_code=b.country_code 
where test_start_date <= current_date() -- is not null 
and( date_diff(test_end_date, test_start_date, day)>=0 or date_diff(test_end_date, test_start_date, day) is null)
) base
left join 
(
select * from (    
select
        p.entity_id
        ,  co.country_code as country_code
        , ci.name city_name
        , ci.id city_id
        , zo.id as zone_id
        


    from fulfillment-dwh-production.cl.countries co
    left join unnest(co.platforms) p
    left join unnest(co.cities) ci
    left join unnest (ci.zones) zo
    WHERE TRUE 
    AND ci.is_active
    AND zo.is_active
    and zo.id is not null
    )

)b
on lower(base.country_code)=lower(b.country_code)
and cast(base.zone_id as numeric)=cast(b.zone_id as numeric)


) group by 1,2,3,4,5,6,7 -- ,8
)a
left join ( SELECT * FROM `dh-logistics-product-ops.pricing.ab_test_dahoc`  where action = 'exclude' and test_name is not null) b
on a.test_name=b.test_name	and ifnull(a.test_start_date,'2030-1-1')=ifnull(b.test_start_date,'2030-1-1') and	ifnull(a.test_end_date,'2030-1-1')=ifnull(b.test_end_date,'2030-1-1')
where b.test_name is null

union all

select region, country_code,test_name, test_start_date,test_end_date
,extract(year from test_start_date) as start_year
,extract(month from test_start_date) as start_month
--,date_diff(current_date(), test_end_date, day) finalizado
, null  distinc_zones 
, null AS concat_zones
, null AS concat_cities
, date_diff(ifnull(test_end_date,current_date()), test_start_date, day)+1 as lenght_days
, case when date_diff(current_date(), test_end_date, day)  between 0 and 7 then ' Finalized Last Week' when test_end_date<current_date() then 'Finished'   else 'Running' end test_current_status
, case when test_start_date is null then 'no start_date' when test_end_date is null then 'no end_date' else 'Ok' end warnings
from `dh-logistics-product-ops.pricing.ab_test_dahoc`  where action='include'

order by 4 desc))
)a
left join
(

select distinct test_start_date	,test_end_date,test_name	
from(select  distinct country_code,zone_ids,test_name,cast(test_start_date as date)test_start_date, cast(test_end_date as date)test_end_date , is_active
from `fulfillment-dwh-production.cl.dps_experiment_setups`, unnest(zone_ids) zone_ids)  a
left join ( select distinct region, country_code FROM `fulfillment-dwh-production.cl.countries`)b
on a.country_code=b.country_code 

where test_start_date <= current_date() and( date_diff(test_end_date, test_start_date, day)>=0 or date_diff(test_end_date, test_start_date, day) is null)
)b
on a.test_name=b.test_name	and ifnull(a.test_start_date,'2030-1-1')=ifnull(b.test_start_date,'2030-1-1') and	ifnull(a.test_end_date,'2030-1-1')=ifnull(b.test_end_date,'2030-1-1')
 where a.test_start_date <= current_date()


)

order by 3 asc,Test_approach asc
) where order_num =1 and test_name is not null and test_start_date is not null 

),
Check_nomenclature as(
Select *
, case when lower(substr(test_name, 13,1)) in (select distinct lower(Vertical_letter) from  `dh-logistics-product-ops.pricing.nomenclature_vertical` ) 
                    and lower(substr(test_name, 12,1))='_' and lower(substr(test_name, 14,1))='_' then 'ok' else 'wrong' end check_vertical
, case when lower(substr(test_name, 15,1)) in (select distinct lower(feature_letter) from  `dh-logistics-product-ops.pricing.nomenclature_feature` ) 
        and lower(substr(test_name, 16,1)) in (select distinct lower(feature_letter) from  `dh-logistics-product-ops.pricing.nomenclature_feature` )
        and lower(substr(test_name, 17,1)) ='_' then 'ok' else 'wrong' end check_feature
, case when lower(substr(test_name, 18,1)) in (select distinct lower(objetive_letter) from  `dh-logistics-product-ops.pricing.nomenclature_objective` ) then 'ok' else 'wrong' end check_objetive

FROM FIRST_LOgic a
),

 total_checked_nomenclature as(
select * 
,case when check_vertical ='ok' and check_feature='ok' and check_objetive='ok' then 'ok' else 'wrong nomenclature' end check_nomenclature
,case when check_vertical ='ok' and check_feature='ok' and check_objetive='ok' then 1 else 0 end nomenclature_adoption
from Check_nomenclature
),

final_data_specific_check as(
select a.* ,b.vertical as vertical_tested ,c.Pricing_feature as first_mechanism_tested
,d.Pricing_feature as second_mechanism_tested, e.Test_objective
from total_checked_nomenclature a
left join `dh-logistics-product-ops.pricing.nomenclature_vertical` b
on lower(substr(test_name, 13,1))= lower(b.Vertical_letter) and  check_nomenclature= 'ok'
left join `dh-logistics-product-ops.pricing.nomenclature_feature` c
on lower(substr(test_name, 15,1))= lower(c.feature_letter) and  check_nomenclature= 'ok'
left join `dh-logistics-product-ops.pricing.nomenclature_feature` d
on lower(substr(test_name, 16,1))= lower(d.feature_letter) and  check_nomenclature= 'ok'
left join `dh-logistics-product-ops.pricing.nomenclature_objective` e
on lower(substr(test_name, 18,1))= lower(e.Objetive_letter) and  check_nomenclature= 'ok'
),

----------- join wiht business KPIs from BI tables -> delta orders, revenue, profit
business_kpi1 as(
SELECT test_name
,count(distinct case when variant='Control' then order_id else null end) ordenes_control
,count(distinct case when variant='Variation1' then order_id else null end) ordenes_var1
,count(distinct case when variant='Variation2' then order_id else null end) ordenes_var2
,count(distinct case when variant='Variation3' then order_id else null end) ordenes_var3
,count(distinct case when variant='Variation4' then order_id else null end) ordenes_var4

,sum( case when variant='Control' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local else 0 end) revenue_control
,sum( case when variant='Variation1' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local  else 0 end) revenue_var1
,sum( case when variant='Variation2' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local  else 0 end) revenue_var2
,sum( case when variant='Variation3' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local  else 0 end) revenue_var3
,sum( case when variant='Variation4' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local  else 0 end) revenue_var4

,sum( case when variant='Control' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local-delivery_costs_local else 0 end) profit_control
,sum( case when variant='Variation1' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local-delivery_costs_local  else 0 end) profit_var1
,sum( case when variant='Variation2' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local-delivery_costs_local  else 0 end) profit_var2
,sum( case when variant='Variation3' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local-delivery_costs_local  else 0 end) profit_var3
,sum( case when variant='Variation4' then delivery_fee_local/(1+vat_ratio)+ commission_local+joker_vendor_fee_local-delivery_costs_local  else 0 end) profit_var4


FROM `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` 
where treatment is true
group by 1
order by 1),

business_kpi2 as(

select * 
-- , ordenes_var1,ordenes_control
,safe_divide(revenue_control,ordenes_control) as RPO_control
,safe_divide(profit_control,ordenes_control) as PPO_control 

, safe_divide(revenue_var1,ordenes_var1) as RPO_v1 
, safe_divide(profit_var1,ordenes_var1)as PPO_v1 

-- , ordenes_var2,ordenes_control
,  safe_divide(revenue_var2,ordenes_var2)as RPO_v2 
,  safe_divide(profit_var2,ordenes_var2)as PPO_v2

,  safe_divide(revenue_var3,ordenes_var3)as RPO_v3 
,  safe_divide(profit_var3,ordenes_var3)as PPO_v3

,  safe_divide(revenue_var4,ordenes_var4)as RPO_v4 
,  safe_divide(profit_var4,ordenes_var4)as PPO_v4

from business_kpi1

),
-- logic to change deltas in case of negative to positive changes, the division is a negative number however is an KPI improvement 
business_kpi3 as(	
select * 
, safe_divide(ordenes_var1,ordenes_control)-1 as Deta_ordenes_V1_vs_control
, case when RPO_control<0 and RPO_v1>RPO_control  then abs(safe_divide(RPO_v1,RPO_control)-1)
when RPO_control<0 and RPO_v1<0  then -(safe_divide(RPO_v1,RPO_control)-1)
else safe_divide(RPO_v1,RPO_control)-1 end as Deta_revenue_V1_vs_control
, case when PPO_control<0 and PPO_v1>PPO_control  then abs(safe_divide(PPO_v1,PPO_control)-1)
when PPO_control<0 and PPO_v1<0  then -(safe_divide(PPO_v1,PPO_control)-1)
else safe_divide(PPO_v1,PPO_control)-1 end as Deta_profit_V1_vs_control

, safe_divide(ordenes_var2,ordenes_control)-1 as Deta_ordenes_V2_vs_control
, case when RPO_control<0 and RPO_v2>RPO_control  then abs(safe_divide(RPO_v2,RPO_control)-1)
when RPO_control<0 and RPO_v2<0  then -(safe_divide(RPO_v2,RPO_control)-1)
else safe_divide(RPO_v2,RPO_control)-1 end as Deta_revenue_v2_vs_control
, case when PPO_control<0 and PPO_v2>PPO_control  then abs(safe_divide(PPO_v2,PPO_control)-1)
when PPO_control<0 and PPO_v2<0  then -(safe_divide(PPO_v2,PPO_control)-1)
else safe_divide(PPO_v2,PPO_control)-1 end as Deta_profit_v2_vs_control

, safe_divide(ordenes_var3,ordenes_control)-1 as Deta_ordenes_V3_vs_control
, case when RPO_control<0 and RPO_v3>RPO_control  then abs(safe_divide(RPO_v3,RPO_control)-1)
when RPO_control<0 and RPO_v3<0  then -(safe_divide(RPO_v3,RPO_control)-1)
else safe_divide(RPO_v3,RPO_control)-1 end as Deta_revenue_v3_vs_control
, case when PPO_control<0 and PPO_v3>PPO_control  then abs(safe_divide(PPO_v3,PPO_control)-1)
when PPO_control<0 and PPO_v3<0  then -(safe_divide(PPO_v3,PPO_control)-1)
else safe_divide(PPO_v3,PPO_control)-1 end as Deta_profit_v3_vs_control

, safe_divide(ordenes_var4,ordenes_control)-1 as Deta_ordenes_v4_vs_control
, case when RPO_control<0 and RPO_v4>RPO_control  then abs(safe_divide(RPO_v4,RPO_control)-1)
when RPO_control<0 and RPO_v4<0  then -(safe_divide(RPO_v4,RPO_control)-1)
else safe_divide(RPO_v4,RPO_control)-1 end as Deta_revenue_v4_vs_control
, case when PPO_control<0 and PPO_v4>PPO_control  then abs(safe_divide(PPO_v4,PPO_control)-1)
when PPO_control<0 and PPO_v4<0  then -(safe_divide(PPO_v4,PPO_control)-1)
else safe_divide(PPO_v4,PPO_control)-1 end as Deta_profit_v4_vs_control



from business_kpi2
),

table_deltas as (

select a.* 
, b.Deta_ordenes_V1_vs_control, Deta_revenue_V1_vs_control  , Deta_profit_V1_vs_control

,if(ordenes_var2 is null or ordenes_var2=0 , null,Deta_ordenes_V2_vs_control) Deta_ordenes_V2_vs_control
,if(RPO_v2 is null, null,Deta_revenue_V2_vs_control) Deta_revenue_V2_vs_control
,if(PPO_v2 is null, null,Deta_profit_V2_vs_control) Deta_profit_V2_vs_control

,if(ordenes_var3 is null or ordenes_var3=0 , null,Deta_ordenes_v3_vs_control) Deta_ordenes_v3_vs_control
,if(RPO_v3 is null, null,Deta_revenue_v3_vs_control) Deta_revenue_v3_vs_control
,if(PPO_v3 is null, null,Deta_profit_v3_vs_control) Deta_profit_v3_vs_control

,if(ordenes_var3 is null or ordenes_var4=0 , null,Deta_ordenes_v4_vs_control) Deta_ordenes_v4_vs_control
,if(RPO_v4 is null, null,Deta_revenue_v4_vs_control) Deta_revenue_v4_vs_control
,if(PPO_v4 is null, null,Deta_profit_v4_vs_control) Deta_profit_v4_vs_control


from final_data_specific_check a
left join  business_kpi3 b
on a.test_name=b.test_name and Test_approach ='Experimentation III'
)


select * 
, case when 
 Deta_ordenes_V2_vs_control is null and Deta_ordenes_V3_vs_control is null and Deta_ordenes_V4_vs_control is null then concat(round(Deta_ordenes_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_ordenes_V1_vs_control>ifnull(Deta_ordenes_V3_vs_control,Deta_ordenes_V1_vs_control-1)
 and Deta_ordenes_V1_vs_control>ifnull(Deta_ordenes_V4_vs_control,Deta_ordenes_V1_vs_control-1)
 and Deta_ordenes_V1_vs_control>ifnull(Deta_ordenes_V2_vs_control,Deta_ordenes_V1_vs_control-1) then concat(round(Deta_ordenes_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_ordenes_V2_vs_control>ifnull(Deta_ordenes_V3_vs_control,Deta_ordenes_V2_vs_control-1)
 and Deta_ordenes_V2_vs_control>ifnull(Deta_ordenes_V4_vs_control,Deta_ordenes_V2_vs_control-1)
 and Deta_ordenes_V2_vs_control>ifnull(Deta_ordenes_V1_vs_control,Deta_ordenes_V2_vs_control-1) then concat(round(Deta_ordenes_V2_vs_control*100,1),'% ,Var2')
 when 
     Deta_ordenes_V3_vs_control>ifnull(Deta_ordenes_V2_vs_control,Deta_ordenes_V3_vs_control-1)
 and Deta_ordenes_V3_vs_control>ifnull(Deta_ordenes_V4_vs_control,Deta_ordenes_V3_vs_control-1)
 and Deta_ordenes_V3_vs_control>ifnull(Deta_ordenes_V1_vs_control,Deta_ordenes_V3_vs_control-1) then concat(round(Deta_ordenes_V3_vs_control*100,1),'% ,Var3')
 when 
     Deta_ordenes_V4_vs_control>ifnull(Deta_ordenes_V1_vs_control,Deta_ordenes_V4_vs_control-1)
 and Deta_ordenes_V4_vs_control>ifnull(Deta_ordenes_V2_vs_control,Deta_ordenes_V4_vs_control-1)
 and Deta_ordenes_V4_vs_control>ifnull(Deta_ordenes_V3_vs_control,Deta_ordenes_V4_vs_control-1) then concat(round(Deta_ordenes_V4_vs_control*100,1),'% ,Var4')
end Best_orders_var

, case when 
 Deta_revenue_V2_vs_control is null and Deta_revenue_V3_vs_control is null and Deta_revenue_V4_vs_control is null then concat(round(Deta_revenue_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_revenue_V1_vs_control>ifnull(Deta_revenue_V3_vs_control,Deta_revenue_V1_vs_control-1)
 and Deta_revenue_V1_vs_control>ifnull(Deta_revenue_V4_vs_control,Deta_revenue_V1_vs_control-1)
 and Deta_revenue_V1_vs_control>ifnull(Deta_revenue_V2_vs_control,Deta_revenue_V1_vs_control-1) then concat(round(Deta_revenue_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_revenue_V2_vs_control>ifnull(Deta_revenue_V3_vs_control,Deta_revenue_V2_vs_control-1)
 and Deta_revenue_V2_vs_control>ifnull(Deta_revenue_V4_vs_control,Deta_revenue_V2_vs_control-1)
 and Deta_revenue_V2_vs_control>ifnull(Deta_revenue_V1_vs_control,Deta_revenue_V2_vs_control-1) then concat(round(Deta_revenue_V2_vs_control*100,1),'% ,Var2')
 when 
     Deta_revenue_V3_vs_control>ifnull(Deta_revenue_V2_vs_control,Deta_revenue_V3_vs_control-1)
 and Deta_revenue_V3_vs_control>ifnull(Deta_revenue_V4_vs_control,Deta_revenue_V3_vs_control-1)
 and Deta_revenue_V3_vs_control>ifnull(Deta_revenue_V1_vs_control,Deta_revenue_V3_vs_control-1) then concat(round(Deta_revenue_V3_vs_control*100,1),'% ,Var3')
 when 
     Deta_revenue_V4_vs_control>ifnull(Deta_revenue_V1_vs_control,Deta_revenue_V4_vs_control-1)
 and Deta_revenue_V4_vs_control>ifnull(Deta_revenue_V2_vs_control,Deta_revenue_V4_vs_control-1)
 and Deta_revenue_V4_vs_control>ifnull(Deta_revenue_V3_vs_control,Deta_revenue_V4_vs_control-1) then concat(round(Deta_revenue_V4_vs_control*100,1),'% ,Var4')
 end Best_revenue_var

, case when 
 Deta_profit_V2_vs_control is null and Deta_profit_V3_vs_control is null and Deta_profit_V4_vs_control is null then concat(round(Deta_profit_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_profit_V1_vs_control>ifnull(Deta_profit_V3_vs_control,Deta_profit_V1_vs_control-1)
 and Deta_profit_V1_vs_control>ifnull(Deta_profit_V4_vs_control,Deta_profit_V1_vs_control-1)
 and Deta_profit_V1_vs_control>ifnull(Deta_profit_V2_vs_control,Deta_profit_V1_vs_control-1) then concat(round(Deta_profit_V1_vs_control*100,1),'% ,Var1')
 when 
     Deta_profit_V2_vs_control>ifnull(Deta_profit_V3_vs_control,Deta_profit_V2_vs_control-1)
 and Deta_profit_V2_vs_control>ifnull(Deta_profit_V4_vs_control,Deta_profit_V2_vs_control-1)
 and Deta_profit_V2_vs_control>ifnull(Deta_profit_V1_vs_control,Deta_profit_V2_vs_control-1) then concat(round(Deta_profit_V2_vs_control*100,1),'% ,Var2')
 when 
     Deta_profit_V3_vs_control>ifnull(Deta_profit_V2_vs_control,Deta_profit_V3_vs_control-1)
 and Deta_profit_V3_vs_control>ifnull(Deta_profit_V4_vs_control,Deta_profit_V3_vs_control-1)
 and Deta_profit_V3_vs_control>ifnull(Deta_profit_V1_vs_control,Deta_profit_V3_vs_control-1) then concat(round(Deta_profit_V3_vs_control*100,1),'% ,Var3')
 when 
     Deta_profit_V4_vs_control>ifnull(Deta_profit_V1_vs_control,Deta_profit_V4_vs_control-1)
 and Deta_profit_V4_vs_control>ifnull(Deta_profit_V2_vs_control,Deta_profit_V4_vs_control-1)
 and Deta_profit_V4_vs_control>ifnull(Deta_profit_V3_vs_control,Deta_profit_V4_vs_control-1) then concat(round(Deta_profit_V4_vs_control*100,1),'% ,Var4')
 end Best_profit_var
, case when  test_name like '%iscon%' or(lenght_days<=3 and test_current_status='Finished') then 'Invalid test' else 'correct test' end valid_test


from table_deltas
