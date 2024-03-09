#################################################### improved experiment configuration versions 


  CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_experiment_configuration_versions` AS
    with load_vendor_group_id AS (
      SELECT DISTINCT
      entity_id
      , test_id
      , priority
      , vendor_group_id
      FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
      GROUP BY 1,2,3,4
    )
    , load_experiment_versions AS (

    SELECT test_name
    , test_id 
    , entity_id
    , test_sub_period_from
    , IFNULL(test_sub_period_to, "2099-01-01") AS test_sub_period_to
    -- , test_price_config_hash
    , priority
    , vendor_group_id
    , time_condition_id
    , customer_condition_id
    , IF(ARRAY_LENGTH(customer_area_ids) > 0
        , ARRAY(SELECT CAST(x.id AS STRING) FROM UNNEST(customer_area_ids) x ORDER BY x.id)
        , NULL
    ) AS customer_area_ids
    , variation_group
    , scheme_id
    , scheme_config_hash
    , scheme_component_hashes
    , scheme_price_mechanisms
    , scheme_component_configs
    , scheme_component_ids

    FROM `fulfillment-dwh-production.cl._dps_experiment_configuration_versions` 
    LEFT JOIN UNNEST(test_price_config) 
    LEFT JOIN load_vendor_group_id USING(entity_id, test_id, priority)
    WHERE already_executed = TRUE
    )

    , add_flag_changes AS (
      SELECT *
      -- more than one config across variants implies at least two are difference
      -- we use N/A to also capture introduction of new components
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.travel_time_config_hash), "N/A")) OVER(test_partition) > 1 AS  travel_time_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.dbmov_config_hash), "N/A")) OVER(test_partition) > 1 AS  dbmov_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.surge_mov_config_hash), "N/A")) OVER(test_partition) > 1 AS  surge_mov_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.small_order_fee_config_hash), "N/A")) OVER(test_partition) > 1 AS  small_order_fee_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.fleet_delay_config_hash), "N/A")) OVER(test_partition) > 1 AS  fleet_delay_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.basket_value_config_hash), "N/A")) OVER(test_partition) > 1 AS  basket_value_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.service_fee_config_hash), "N/A")) OVER(test_partition) > 1 AS  service_fee_differs_across_variants
      , COUNT(DISTINCT IFNULL(TO_BASE64(scheme_component_hashes.priority_fee_config_hash), "N/A")) OVER(test_partition) > 1 AS  priority_fee_differs_across_variants
      FROM load_experiment_versions
      WINDOW
      test_partition AS (PARTITION BY entity_id, test_id, test_sub_period_from, priority)
    )

    , remove_scheme_granularity AS (
      /*
      REMOVE scheme granularity but keep
      the relevant info, i.e., which component changed
      */
      SELECT
      test_name
      , test_id 
      , entity_id
      , test_sub_period_from
      , test_sub_period_to 
      , priority 
      , vendor_group_id
      , CAST(time_condition_id AS STRING) AS time_condition_id
      , CAST(customer_condition_id AS STRING) AS customer_condition_id
      , ANY_VALUE(customer_area_ids) AS customer_area_ids
      , ARRAY_AGG(TO_BASE64(scheme_config_hash) ORDER BY variation_group) as _priority_scheme_config_hashes
      , ARRAY_AGG(CAST(scheme_id AS STRING) ORDER BY variation_group) _priority_schemes
      , STRUCT(
          MAX(travel_time_differs_across_variants) AS travel_time_differs_across_variants
        , MAX(dbmov_differs_across_variants) AS dbmov_differs_across_variants
        , MAX(surge_mov_differs_across_variants) AS surge_mov_differs_across_variants
        , MAX(small_order_fee_differs_across_variants) AS small_order_fee_differs_across_variants
        , MAX(fleet_delay_differs_across_variants) AS fleet_delay_differs_across_variants
        , MAX(basket_value_differs_across_variants) AS basket_value_differs_across_variants
        , MAX(service_fee_differs_across_variants) AS service_fee_differs_across_variants
        , MAX(priority_fee_differs_across_variants) AS priority_fee_differs_across_variants
      ) AS target_group_component_changes
      , STRUCT(
        MAX(scheme_price_mechanisms.is_dbdf) AS is_dbdf
        , MAX(scheme_price_mechanisms.is_dbmov) AS is_dbmov
        , MAX(scheme_price_mechanisms.is_surge_mov) AS is_surge_mov
        , MAX(scheme_price_mechanisms.is_small_order_fee) AS is_small_order_fee
        , MAX(scheme_price_mechanisms.is_fleet_delay) AS is_fleet_delay
        , MAX(scheme_price_mechanisms.is_basket_value_deal) AS is_basket_value_deal
        , MAX(scheme_price_mechanisms.is_service_fee) AS is_service_fee
        , MAX(scheme_price_mechanisms.is_priority_fee) AS is_priority_fee
      ) as target_group_scheme_price_mechanisms
      , ARRAY_AGG(
        STRUCT(
          priority
        , variation_group
        , scheme_id
        , scheme_config_hash
        , scheme_component_hashes
        , scheme_price_mechanisms
        , scheme_component_configs
        , scheme_component_ids
        )
        ORDER BY variation_group
      ) AS priority_price_config
      FROM add_flag_changes
      GROUP BY 1,2,3,4,5,6,7,8,9
    )

    , add_priority_price_config_hash AS (
      SELECT * EXCEPT(_priority_scheme_config_hashes, _priority_schemes)
      , SHA256(
        CONCAT(
          IFNULL(time_condition_id, "")
          , IFNULL(customer_condition_id, "")
          , IFNULL(ARRAY_TO_STRING(customer_area_ids, ","), "")
          , ARRAY_TO_STRING(_priority_scheme_config_hashes, "")
          , ARRAY_TO_STRING(_priority_schemes, "")
        )
      ) as priority_price_config_hash
      FROM remove_scheme_granularity
    )

    , vendor_group_updates_clean AS (
      SELECT * EXCEPT(next_updated_at)
      , IFNULL(next_updated_at, "2099-01-01") AS next_updated_at
      FROM `logistics-data-storage-staging.long_term_pricing.pricing_vendor_group_versions`
    )

    , combine_price_and_vendor_history AS (
      SELECT
      test_name
      , test_id
      , lev.entity_id 
      , GREATEST(lev.test_sub_period_from, vgu.vendor_group_updated_at) AS test_sub_period_from
      , LEAST(lev.test_sub_period_to, vgu.next_updated_at) AS test_sub_period_to
      FROM add_priority_price_config_hash lev
      LEFT JOIN vendor_group_updates_clean vgu
        ON lev.entity_id = vgu.entity_id 
        AND lev.vendor_group_id = vgu.vendor_group_id
      WHERE TRUE
          AND lev.test_sub_period_from < vgu.next_updated_at  -- For cases when vendor config was activated while price config was already active.
          AND lev.test_sub_period_to > vgu.vendor_group_updated_at  -- For cases when price config was activated while vendor config was already active.
      -- GROUP BY 1,2,3,4,5
    )

    , aggregate_timestamps AS (
      /*
      We are forced to do this as vendor_group_id updates are independents but
      we want to reflect the specific vendor_group_id versions for a test who may have
      X number of vendor_group_ids. 

      Therefore, we need to first compute a clean list of timestamp reflecting any change
      of the experiment configs or vendor_group_ids
      */
      SELECT
      test_name
      , test_id
      , entity_id
      , ARRAY_AGG(test_sub_period_from) as test_sub_period_from_agg
      , ARRAY_AGG(test_sub_period_to) as test_sub_period_to_agg
      FROM combine_price_and_vendor_history
      GROUP BY 1,2,3
    )

    , get_ordered_timestamps AS (
      SELECT
        entity_id
        , test_id
        , test_name
        , ARRAY(
          SELECT DISTINCT x
          FROM UNNEST(ARRAY_CONCAT(test_sub_period_from_agg, test_sub_period_to_agg)) x ORDER BY x
        ) AS ordered_timestamps
      FROM aggregate_timestamps
    )

    , add_new_active_to AS (
      SELECT
        * EXCEPT(ordered_timestamps)
        , LEAD(test_sub_period_from, 1) OVER (PARTITION BY entity_id, test_id, test_name ORDER BY test_sub_period_from) AS test_sub_period_to
      FROM get_ordered_timestamps
      LEFT JOIN UNNEST(ordered_timestamps) test_sub_period_from
    )

    , test_new_versioning AS (
      /*
      Priorities and vendor_group_id per tests are generally fixed
      by joining back the vendor group id gives us a complete list of 
      priorities for each timestamp where a versioning change happened
      */
      SELECT *
      FROM add_new_active_to a
      LEFT JOIN load_vendor_group_id b USING(entity_id, test_id)
      WHERE TRUE
        -- remove unnecessary last row that is generated with new_active_from = '2099-01-01'
        AND test_sub_period_to IS NOT NULL
        AND test_sub_period_from < test_sub_period_to
    )

    , add_back_price_config AS (
      SELECT tnv.*
      , ppc.* EXCEPT(test_sub_period_from, test_sub_period_to, priority, vendor_group_id, test_name, entity_id, test_id, customer_area_ids)
      , ARRAY_TO_STRING(customer_area_ids, ", ") AS customer_area_ids
      FROM test_new_versioning tnv
      LEFT JOIN add_priority_price_config_hash ppc
        ON tnv.entity_id = ppc.entity_id
        AND tnv.test_id = ppc.test_id
        AND tnv.priority = ppc.priority
      WHERE TRUE
      AND  tnv.test_sub_period_from < ppc.test_sub_period_to
      AND tnv.test_sub_period_to > ppc.test_sub_period_from
    )

    , add_back_vendor_group_id_history AS (
        SELECT tnv.*
      , ppc.* EXCEPT(vendor_group_updated_at, next_updated_at, vendor_group_id, entity_id)
      FROM add_back_price_config tnv
      LEFT JOIN vendor_group_updates_clean ppc
        ON tnv.entity_id = ppc.entity_id
        AND tnv.vendor_group_id = ppc.vendor_group_id
      WHERE TRUE
      AND  tnv.test_sub_period_from < ppc.next_updated_at
      AND tnv.test_sub_period_to > ppc.vendor_group_updated_at
    )

    SELECT *
    FROM add_back_vendor_group_id_history