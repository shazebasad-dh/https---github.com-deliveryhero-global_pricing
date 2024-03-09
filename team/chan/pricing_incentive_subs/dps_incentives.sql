########################################## INPUTS

  ########## DECLARE VARIABLES

  DECLARE from_date_filter, to_date_filter DATE;
  DECLARE backfill BOOL;


  ########## SET RUN MODE
  SET backfill = TRUE;

  # SET END DATE
  SET to_date_filter = CURRENT_DATE();

  # SET PARTITION DATE
  IF backfill THEN
  SET from_date_filter = DATE_SUB("2023-01-01", interval 0 DAY);
  ELSE
  SET from_date_filter = DATE_TRUNC(DATE_SUB(to_date_filter, interval 1 MONTH), MONTH);
  END IF;
##########################################


##########################################

  WITH load_orders AS (
    SELECT orders.entity_id 
      , platform_order_code
      , order_placed_at
      , created_date_local
      , order_placed_at_local
      , scheme_id
      , vendor_price_scheme_type
      , assignment_id
      , dps_session_id
      , has_subscription
      --- Incentive related field

        /*
        Ideally, there should not be "Other" but we would rather
        catch unexpected system/data producer behavior than ignoring them. 
        
        It also higlights improvement to our curation logic such as the
        ongoing thread here https://deliveryhero.slack.com/archives/C01903CCRU7/p1708094671467919
        */
        , CASE
          WHEN has_subscription_discount THEN TRUE
          WHEN vendor_price_scheme_type = "Campaign" THEN TRUE
          WHEN has_new_customer_condition THEN TRUE
          WHEN ABS(dps_incentive_discount_local) > 0  THEN TRUE
          ELSE FALSE
        END AS has_dps_discount

        , CASE
          WHEN has_subscription_discount THEN "Subscription"
          WHEN vendor_price_scheme_type = "Campaign" THEN "Campaign"
          WHEN has_new_customer_condition THEN "FDNC"
          WHEN ABS(dps_incentive_discount_local) > 0  THEN "Other"
          ELSE NULL
        END AS incentive_type

        /*
        There is an ongoing DQI case where a discount is given, the incentivized delivery fee
        is higher than before incentives (we use the standard fee as a proxy, see below of its definition)
        but there's no discount amount. In this case, we can recover the amount by discount amount taking
        the difference of dps_standard_fee_local - dps_delivery_fee_local

        https://deliveryhero.slack.com/archives/C04NASPRM3N/p1707733557720899
        */
        , CASE
            WHEN has_subscription_discount THEN ABS(dps_basket_value_fee_local)
            WHEN dps_incentive_discount_local IS NULL AND dps_standard_fee_local > dps_delivery_fee_local THEN dps_standard_fee_local - dps_delivery_fee_local
          ELSE ABS(dps_incentive_discount_local)
        END AS total_incentives_local

        , dps_travel_time_fee_local -- distance base delivery fee
        , dps_surge_fee_local
          ----- basket value discount are not discount except for subs. I want this column to reflect non-discount basket value discounts. 
        , IF(has_subscription_discount, 0, dps_basket_value_fee_local) AS dps_basket_value_fee_local
        , dps_delivery_fee_local -- fee sent by DPS, it can be incentivized or not. see next CTE
        , delivery_fee_local -- fee logged at the checkout, source is data-stream
        , exchange_rate
        /*
        -- fee used for COMMUNICATION purposes (Strikethrough Prices), it's not exactly the delivery fee before incentive as it only cover the
        distance base delivery fee and ignore surge pricing but distance based is usually the biggest contributor to the final delivery fee.
        */
        , dps_standard_fee_local 
    
    FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` orders
    WHERE created_date BETWEEN DATE_SUB(from_date_filter, INTERVAL 2 DAY) AND DATE_ADD(to_date_filter, INTERVAL 2 DAY)
      AND is_sent
      AND is_own_delivery
      AND dps_delivery_fee_local is not null
      AND created_date_local BETWEEN from_date_filter AND to_date_filter
  )

  , calculate_base_delivery_fee AS (
      SELECT 

      entity_id
      , platform_order_code
      , created_date_local
      , has_subscription
      , has_dps_discount
      , incentive_type 
        /*
        When there is a DPS discount, dps_delivery_fee_local is the incentivized delivery fee. 
        Add the incentive amount to get the base delivery fee. 
        */
      , CASE
          WHEN has_dps_discount THEN dps_delivery_fee_local + total_incentives_local
          ELSE dps_delivery_fee_local
        END AS base_delivery_fee_local
      , dps_delivery_fee_local
      , delivery_fee_local
      , total_incentives_local 
      , dps_travel_time_fee_local
      , dps_surge_fee_local
      , dps_basket_value_fee_local


      FROM load_orders
  )

  SELECT *
  FROM calculate_base_delivery_fee