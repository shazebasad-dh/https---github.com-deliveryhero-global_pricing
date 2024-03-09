/*

What's considered discount a DPS discount? 

Any order where
- Orders made by subscription users who got delivery fee benefits
- Orders made with campaign assignments
- Orders made with customer conditions

IF an order has a dps discount, the ideal situation would be:

- dps_delivery_fee_local is always the value inclusive of discount, hence it's called incentivized delivery fee
- total_incentives_local (which is a combination of dps_incentive_local/dps_basket_discount) always logs the correct incentive amount given (including assignments that would have apply and all conditions)
- the "base delivery fee", the fee before incentives can be retrieved adding up the dps_delivery_fee_local and total_incentives_total.

Now, these are the few cases where this does not apply. It generally happens that total_incentives_local IS NULL. 
Therefore, we cannot get the base delivery fee as in the ideal situation. What to do? 

We assume that the base delivery fee is equal to IFNULL(dps_standard_fee_local, asa_dps_delivery_fee). 

- dps_standard_fee_local represents the dps_travel_time_fee_local that would have applied had the incentive not applied. It's main purpose is to be used as strikethrough price for communication to the end-user. It's come
directly from DPS logs. 

- asa_dps_delivery_fee is similar to the dps_standard_fee_local in what it represents. The key difference is that it comes from simulating the DPS logic leveraging the tables with vendor price configurations at our disposal and
estimating the fee that would have applied. 

Since dps_standard_fee_local comes directly from the data producedr, it's considered of HIGHER quality until proven otherwise. This explains the use of IFNULL(dps_standard_fee_local, asa_dps_delivery_fee). However,
both values have a weakness, they both omit any dps_surge_fee_local in case it would have applied. It needs to be assessed how to retrieve this information to improve the accuracy of base_delivery_fee estimation.

Once we estimate base_delivery_fee, the incentive amount is simply the difference between the base_delivery_fee and dps_delivery_fee_local


Why the incentive amount would be null? there are two reason:
- The base_delivery_fee is equal or lower than the incentivized delivery fee. Any time the incentive is 0 or negative, it will be NULL (as confirmed by the DPS API team).
- Logging problems, we can identify/quantify them if the base_delivery_fee (after estimating it) is still higher than dps_delivery_fee but DPS failed to register a discount. 
 
*/

with load_data AS (
  SELECT 
    entity_id 
    , platform_order_code
    , created_date_local
    , order_placed_at_local
    , dps_delivery_fee_local as incentivized_delivery_fee
    , has_dps_discount
    , exchange_rate
    , CASE 
        WHEN total_incentives_local IS NOT NULL THEN dps_delivery_fee_local +
        ELSE IFNULL(dps_standard_fee_local, asa_dps_delivery_fee)
      END AS base_delivery_delivery_fee
    , CASE
        WHEN total_incentives_local IS NOT NULL THEN total_incentives_local
        ELSE IFNULL(dps_standard_fee_local, asa_dps_delivery_fee) - dps_delivery_fee_local
      END AS discount_amount_total

    FROM `logistics-data-storage-staging.temp_pricing.pricing_subs_incentive_data` 
    LEFT JOIN UNNEST(dps_incentive) dps
    WHERE has_dps_discount
    AND entity_id LIKE ANY ("TB%", "HF_EG")
)


  SELECT 
  entity_id
  , has_dps_discount
  , is_base_delivery_fee_higher_than_incentivized
  , COUNT(platform_order_code) as n_orders
  , SUM(base_delivery_delivery_fee) as total_base_delivery_fee_local
  , SUM(incentivized_delivery_fee) AS total_incentivized_delivery_fee_local
  , SUM(discount_amount_total) AS total_discount_amount_local
  , SUM(SAFE_DIVIDE(base_delivery_delivery_fee, exchange_rate)) as total_base_delivery_fee_eur
  , SUM(SAFE_DIVIDE(incentivized_delivery_fee, exchange_rate)) AS total_incentivized_delivery_fee_eur
  , SUM(SAFE_DIVIDE(discount_amount_total, exchange_rate)) AS total_discount_amount_eur
  , COUNT(IF(base_delivery_delivery_fee IS NULL, 1, NULL)) AS n_orders_without_base_delivery_fee
  FROM (
    SELECT *
    ,  base_delivery_delivery_fee > incentivized_delivery_fee as is_base_delivery_fee_higher_than_incentivized
    FROM load_data
  ) a
  GROUP BY GROUPING SETS (
    (entity_id, has_dps_discount)
    , (entity_id, has_dps_discount, is_base_delivery_fee_higher_than_incentivized)
  )
  ORDER BY 1,2,3




