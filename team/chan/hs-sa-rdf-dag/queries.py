STAGING_QUERY = """

DECLARE start_date, end_date DATE;

SET end_date = "{0}";
SET start_date = "{1}";

SELECT 
    order_id as platform_order_code
    , operation_day
    , order_created_at_sa
    , branch_id
    , branch_name_en
    , OD_delivery_fee
    , is_acquisition
    , rdf_offer_applied
    , rdf_offer_restaurant_max_charge
    , rdf_offer_type
    , is_subscribed
    , is_user_subscribed
    , delivery_fee_discount
    , subscribed_discount_amount

FROM `dhub-hungerstation.reporting_prod.orders_fact_non_pii` 
WHERE operation_day BETWEEN start_date AND end_date
AND rdf_offer_applied = 1;
"""



MERGE_QUERY = """
MERGE INTO `dh-logistics-product-ops.pricing.{0}` prd
  USING  `dh-logistics-product-ops.pricing.{1}` stg
    ON prd.platform_order_code = stg.platform_order_code
  WHEN MATCHED THEN
    UPDATE SET
        platform_order_code = stg.platform_order_code
        , operation_day = stg.operation_day
        , order_created_at_sa = stg.order_created_at_sa
        , branch_id = stg.branch_id
        , branch_name_en = stg.branch_name_en
        , OD_delivery_fee = stg.OD_delivery_fee
        , rdf_offer_applied = stg.rdf_offer_applied
        , rdf_offer_restaurant_max_charge = stg.rdf_offer_restaurant_max_charge
        , rdf_offer_type = stg.rdf_offer_type
        , is_subscribed = stg.is_subscribed
        , is_user_subscribed = stg.is_user_subscribed
        , delivery_fee_discount = stg.delivery_fee_discount
        , subscribed_discount_amount = stg.subscribed_discount_amount
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
"""