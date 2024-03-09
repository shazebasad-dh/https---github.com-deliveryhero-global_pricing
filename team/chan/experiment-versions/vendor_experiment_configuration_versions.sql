-- #################################################### vendor group versions
--   CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_vendor_group_versions` AS 
--   WITH vendor_group_updates AS (
--       SELECT
--       global_entity_id AS entity_id
--       , vendor_group_id
--       /*these are database timestamp with a high time-unit granularity
--       by truncating to minutes we make it more relatable
--       to human interaction timestamp and reduce table size
--       */
--       , TIMESTAMP_TRUNC(updated_at, MINUTE) AS vendor_group_updated_at
--       -- convert the vendor_id array to string and sort it so that we can compare it to the former and the next one:
--       , SHA256(ARRAY_TO_STRING(ARRAY(SELECT id FROM UNNEST(matching_vendor_ids) id ORDER BY id), '')) AS matching_vendor_ids_hash
--       , ARRAY_LENGTH(matching_vendor_ids) AS matching_vendor_id_count
--       , ARRAY(SELECT id FROM UNNEST(matching_vendor_ids) id ORDER BY id) as sorted_vendor_id
--     FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group`
--     WHERE created_date >= '2022-01-01'

--     ), vendor_group_updates_lag AS (
--     SELECT
--       *
--       , LAG(matching_vendor_ids_hash) OVER (PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) AS lag_matching_vendor_ids
--     FROM vendor_group_updates
--   ), vendor_group_updates_remove_duplicate AS (
--     -- This CTE will filter out updates where the vendor list was not changed
--     SELECT
--       *
--       , LEAD(vendor_group_updated_at) OVER (PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) AS next_updated_at
--     FROM vendor_group_updates_lag
--     WHERE lag_matching_vendor_ids IS NULL -- take the first update for every vendor_group_id
--       OR matching_vendor_ids_hash != lag_matching_vendor_ids  -- take updates that are different than the former update
--   --check the changes inside each subperiod
--   )
--   , vendor_group_updates_clean AS (
--     SELECT
--       entity_id
--       , vendor_group_id
--       , vendor_group_updated_at
--       , LEAD(vendor_group_updated_at, 1) OVER(PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) next_updated_at
--       -- , IFNULL(
--       --   LEAD(vendor_group_updated_at, 1) OVER(PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at)
--       --   , "2099-01-01"
--       -- ) as next_updated_at
--       , matching_vendor_ids_hash
--       , sorted_vendor_id
--       , matching_vendor_id_count
--     FROM vendor_group_updates_remove_duplicate
--   )

--     SELECT entity_id
--   , vendor_group_id
--   , vendor_id
--   , MIN(vendor_group_updated_at) as active_from
--   , MAX(next_updated_at) as active_to
--   -- FROM `logistics-data-storage-staging.long_term_pricing.pricing_vendor_group_versions`
--   FROM vendor_group_updates_clean
--   LEFT JOIN UNNEST(sorted_vendor_id) as vendor_id
--   WHERE vendor_id is not null
--   GROUP BY 1,2,3;


--   -- SELECT *
--   -- FROM vendor_group_updates_clean;
-- ####################################################

#################################################### improved experiment configuration versions 


  -- CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_experiment_configuration_versions` AS
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
      , ARRAY_TO_STRING(ANY_VALUE(customer_area_ids), ", ") AS customer_area_ids
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
          , IFNULL(customer_area_ids, "")
          , ARRAY_TO_STRING(_priority_scheme_config_hashes, "")
          , ARRAY_TO_STRING(_priority_schemes, "")
        )
      ) as priority_price_config_hash
      FROM remove_scheme_granularity
    )

    , vendor_group_updates_clean AS (
        SELECT entity_id
      , vendor_group_id
      , vendor_id
      , MIN(vendor_group_updated_at) as active_from
      , IFNULL(MAX(next_updated_at), "2099-01-01") as active_to
      FROM `logistics-data-storage-staging.long_term_pricing.pricing_vendor_group_versions`
      -- FROM vendor_group_updates_clean
      LEFT JOIN UNNEST(sorted_vendor_id) as vendor_id
      WHERE vendor_id is not null
      GROUP BY 1,2,3
    )

    , combine_price_and_vendor_history AS (
      SELECT lev.* EXCEPT(test_sub_period_from, test_sub_period_to)
      , vendor_id
      , GREATEST(lev.test_sub_period_from, vgu.active_from) AS active_from
      , LEAST(lev.test_sub_period_to, vgu.active_to) AS active_to
      FROM add_priority_price_config_hash lev
      LEFT JOIN vendor_group_updates_clean vgu
        ON lev.entity_id = vgu.entity_id 
        AND lev.vendor_group_id = vgu.vendor_group_id
      WHERE TRUE
          AND lev.test_sub_period_from < vgu.active_to  -- For cases when vendor config was activated while price config was already active.
          AND lev.test_sub_period_to > vgu.active_from  -- For cases when price config was activated while vendor config was already active.
      -- GROUP BY 1,2,3,4,5
    )

    , get_previous_version AS (
      SELECT *
        , LAG(priority_price_config_hash) OVER(PARTITION BY entity_id, test_id, vendor_id, priority ORDER BY active_from) as prev_hash
      FROM combine_price_and_vendor_history
    )

    , deduplicate_versions AS (
      SELECT *
      FROM get_previous_version
      WHERE (
          CASE
            WHEN prev_hash IS NULL THEN TRUE --we keep the first version
            WHEN (priority_price_config_hash = prev_hash) THEN FALSE -- we remove version when the current one is equal to the previous one
            ELSE TRUE
          END
      )
    )

    , get_latest_test_sub_period AS (
      SELECT 
      entity_id
      , test_id
      , IFNULL(MAX(test_end_date), "2099-01-01") as latest_test_sub_period_to
      FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
      GROUP BY 1,2
    )

    , add_sub_period_test AS (
      /*
      The "last" version is always bounded by the test_end_date
      */
      SELECT a.* EXCEPT(active_to)
      , IFNULL(
        LEAD(active_from) OVER(PARTITION BY entity_id, test_id, vendor_id, priority ORDER BY active_from)
        , active_to
      ) AS active_to
      FROM deduplicate_versions a
      LEFT JOIN get_latest_test_sub_period USING(entity_id, test_id)
    )

    ############################## FIND OVERLAPPING TESTS

    -- Next, we find all overlapping experiments by doing a self-join on the same entity_id and vendor_id
    -- and keep only those rows that have overlapping active periods. Overlapping periods are defined as:
    --   - Test A starts before Test B ends
    --   - Test A ends after Test B starts
    -- Left join is used to keep those vendors that are only part of one Test.
    , get_overlap_experiments AS (
      SELECT
        a.* EXCEPT(active_to, active_from)
        , a.active_from AS initial_from
        , IF(b.active_from IS NULL, a.active_from, GREATEST(a.active_from, b.active_from)) AS active_from
        , IF(b.active_to IS NULL, a.active_to, LEAST(a.active_to, b.active_to)) AS active_to
      FROM add_sub_period_test a
      LEFT JOIN add_sub_period_test b
        ON a.entity_id = b.entity_id
        AND a.vendor_id = b.vendor_id
        AND a.test_id < b.test_id
        AND a.active_from < b.active_to
        AND a.active_to > b.active_from
    )

    -- From the previous CTE we obtain all "important" timestamps for each Test.
    -- We need to aggregate them and create a unique list of timestamps for each vendor.
    -- These timestamps will mean that a new Test version is created, because a Test starts, ends, or changes its active status.
    -- The initial_from aggregration keeps track of versions where a vendor is only part of a single Test.

    , aggregate_by_versioning AS (
      SELECT
        entity_id
        , vendor_id
        , ARRAY_AGG(initial_from) initial_from_agg
        , ARRAY_AGG(active_from) AS active_from_agg
        , ARRAY_AGG(active_to) AS active_to_agg
      FROM get_overlap_experiments
      GROUP BY 1, 2
    )

    , ordered_timestamp AS (
      SELECT
        entity_id
        , vendor_id
        , ARRAY(
            SELECT DISTINCT ts
            FROM UNNEST(ARRAY_CONCAT(initial_from_agg, active_from_agg, active_to_agg)) ts
            ORDER BY 1
        ) AS ordered_timestamp_array
      FROM aggregate_by_versioning
    )

    , new_experiment_timestamps AS (
      SELECT 
        * EXCEPT(ordered_timestamp_array)
        , LEAD(active_from) OVER(PARTITION BY entity_id, vendor_id ORDER BY active_from) AS active_to
      FROM ordered_timestamp
      LEFT JOIN UNNEST(ordered_timestamp_array) active_from
    )

    , remove_unnecesary_versions AS (
      SELECT *
      FROM new_experiment_timestamps
      WHERE active_to IS NOT NULL
        AND active_from < active_to
    )

    , add_back_experiment_info AS (
      SELECT a.*
      , b.* EXCEPT(entity_id, vendor_id, active_from, active_to)
      FROM remove_unnecesary_versions a
      INNER JOIN add_sub_period_test b
      ON a.entity_id = b.entity_id
      AND a.vendor_id = b.vendor_id
      AND a.active_from >= b.active_from
      AND a.active_to <= b.active_to
    )

    , remove_priority_granularity AS (
      SELECT
      entity_id
      , test_id
      , test_name
      , active_from
      , active_to
      , vendor_id
        --- a vendor can be part of multiple target groups
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TO_BASE64(priority_price_config_hash) ORDER BY TO_BASE64(priority_price_config_hash)), "")) as priority_price_config_hash
        -- these are to keep track of the unique set of conditions used in the test setup
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT time_condition_id IGNORE NULLS ORDER BY time_condition_id), " | ") ) as time_condition_hash
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") ) as customer_condition_hash
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") ) as customer_area_hash
      , MAX(target_group_component_changes.travel_time_differs_across_variants) AS travel_time_differs_across_variants
      , MAX(target_group_component_changes.dbmov_differs_across_variants) AS dbmov_differs_across_variants
      , MAX(target_group_component_changes.surge_mov_differs_across_variants) AS surge_mov_differs_across_variants
      , MAX(target_group_component_changes.small_order_fee_differs_across_variants) AS small_order_fee_differs_across_variants
      , MAX(target_group_component_changes.fleet_delay_differs_across_variants) AS fleet_delay_differs_across_variants
      , MAX(target_group_component_changes.basket_value_differs_across_variants) AS basket_value_differs_across_variants
      , MAX(target_group_component_changes.priority_fee_differs_across_variants) AS priority_fee_differs_across_variants
      , MAX(target_group_scheme_price_mechanisms.is_dbdf) AS vendor_has_dbdf
      , MAX(target_group_scheme_price_mechanisms.is_dbmov) AS vendor_has_dbmov
      , MAX(target_group_scheme_price_mechanisms.is_surge_mov) AS vendor_has_surge_mov
      , MAX(target_group_scheme_price_mechanisms.is_small_order_fee) AS vendor_has_small_order_fee
      , MAX(target_group_scheme_price_mechanisms.is_fleet_delay) AS vendor_has_fleet_delay
      , MAX(target_group_scheme_price_mechanisms.is_basket_value_deal) AS vendor_has_basket_value
      , MAX(target_group_scheme_price_mechanisms.is_priority_fee) AS vendor_has_priority_fee

      --- these are for debugging
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT time_condition_id IGNORE NULLS ORDER BY time_condition_id), " | ") as time_condition_ids
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") customer_condition_ids
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids ignore nulls ORDER BY customer_area_ids), " | ") AS customer_area_ids
      , ARRAY_CONCAT_AGG(priority_price_config ORDER BY priority) as priority_price_configs
      FROM add_back_experiment_info
      GROUP BY 1,2,3,4,5,6
    )

    
  ##############################

  ############################## GET FLAG FOR TESTING CONDITIONS AGGREGATE DATA

    /*
    We now aim to get columns that indicate us if for a given test we're trying a new set of conditions.
    We need to compare the conditions used in the experiment with those in the ASA where the vendor are part of.
    */

    , load_asa_mechanisms AS (
        --  Load current Data
        -- ASA component mechanisms
        SELECT
          entity_id
          , vendor_code as vendor_id
          , active_from
          , asa_id
          , IFNULL(active_to, CURRENT_TIMESTAMP()) AS active_to
          , CAST(customer_condition_id AS STRING) customer_condition_id
          , CAST(schedule_id AS STRING) schedule_id
          , IF(ARRAY_LENGTH(area_configs) > 0
              , ARRAY_TO_STRING(ARRAY(SELECT CAST(x.area_id AS STRING) FROM UNNEST(area_configs) x ORDER BY x.area_id),", ")
              , NULL
          ) AS customer_area_ids

        FROM `fulfillment-dwh-production.cl.vendor_asa_sfo_subscription_configuration_versions`
        LEFT JOIN UNNEST(dps_asa_configuration_history) asa
        LEFT JOIN UNNEST(asa.asa_price_config) apc
        -- FROM `{{ params.project_id }}.cl.vendor_asa_sfo_subscription_configuration_versions`
        WHERE type = 'BASIC'
    )

    , asa_condition_hashes AS (

      SELECT 
      entity_id
      , vendor_id
      , active_from
      , active_to
      , asa_id
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") asa_customer_condition_ids
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") asa_time_condition_ids
      , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") asa_customer_area_ids
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") ) asa_customer_condition_hash
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") ) asa_time_condition_hash
      , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") ) asa_customer_area_hash
      FROM load_asa_mechanisms
      GROUP BY 1,2,3,4,5
    )

    , join_asa_mechanisms AS (
      SELECT exper.* EXCEPT(active_from, active_to)
      , asa.* EXCEPT(entity_id, vendor_id, active_from, active_to)
      , GREATEST(exper.active_from, asa.active_from) active_from
      , LEAST(exper.active_to, asa.active_to) active_to
      FROM remove_priority_granularity exper
      LEFT JOIN asa_condition_hashes asa
        ON exper.entity_id = asa.entity_id 
        AND exper.vendor_id = asa.vendor_id 
      WHERE exper.active_from < asa.active_to 
        AND exper.active_to > asa.active_from
    )

    , add_condition_flags AS (
      SELECT *
      , CASE
          WHEN time_condition_hash IS NULL AND asa_time_condition_hash IS NULL THEN FALSE 
          WHEN time_condition_hash IS NULL OR asa_time_condition_hash IS NULL THEN TRUE 
          ELSE time_condition_hash <> asa_time_condition_hash
        END AS vendor_has_different_time_condition_than_asa

      , CASE
          WHEN customer_condition_hash IS NULL AND asa_customer_condition_hash IS NULL THEN FALSE 
          WHEN customer_condition_hash IS NULL OR asa_customer_condition_hash IS NULL THEN TRUE 
          ELSE customer_condition_hash <> asa_customer_condition_hash
        END AS vendor_has_different_customer_condition_than_asa

      , CASE
          WHEN customer_area_hash IS NULL AND asa_customer_area_hash IS NULL THEN FALSE 
          WHEN customer_area_hash IS NULL OR asa_customer_area_hash IS NULL THEN TRUE 
          ELSE customer_area_hash <> asa_customer_area_hash
        END AS vendor_has_different_customer_area_than_asa

      FROM join_asa_mechanisms
    )
  ##############################

  ############################## AGGREGATE DATA

    , aggregate_values AS (
        SELECT
        "EXPERIMENT" AS type  -- hardcoded to match format of `type` field in cl.vendor_asa_sfo_subscription_configuration_versions, which comes from `hl.dynamic_pricing_vendor_group_assignment`
        , entity_id
        , vendor_id
        , active_from
        , IF(active_to = "2099-01-01", NULL, active_to) AS active_to
        , ARRAY_AGG(DISTINCT test_name) as test_names
        , STRUCT(
              MAX(travel_time_differs_across_variants) AS has_travel_time_change
          ,   MAX(dbmov_differs_across_variants) AS has_dbmov_change
          ,   MAX(surge_mov_differs_across_variants) AS has_surge_mov_change
          ,   MAX(small_order_fee_differs_across_variants) AS has_small_order_fee_change
          ,   MAX(fleet_delay_differs_across_variants) AS has_fleet_delay_change
          ,   MAX(basket_value_differs_across_variants) AS has_basket_value_change
          ,   MAX(priority_fee_differs_across_variants) AS has_priority_fee_change
          ,   MAX(vendor_has_different_time_condition_than_asa) AS has_time_condition_change
          ,   MAX(vendor_has_different_customer_condition_than_asa) AS has_customer_condition_change
          ,   MAX(vendor_has_different_customer_area_than_asa) AS has_customer_area_change
        ) as vendor_change_flags
        , STRUCT(
            MAX(vendor_has_dbdf) AS vendor_has_dbdf
          , MAX(vendor_has_dbmov) AS vendor_has_dbmov
          , MAX(vendor_has_surge_mov) AS vendor_has_surge_mov
          , MAX(vendor_has_small_order_fee) AS vendor_has_small_order_fee
          , MAX(vendor_has_fleet_delay) AS vendor_has_fleet_delay
          , MAX(vendor_has_basket_value) AS vendor_has_basket_value
          , MAX(vendor_has_priority_fee) AS vendor_has_priority_fee
          , MAX(time_condition_hash IS NOT NULL) AS vendor_time_condition
          , MAX(customer_condition_hash IS NOT NULL) AS vendor_customer_condition
          , MAX(customer_area_hash IS NOT NULL) AS vendor_customer_area
        ) AS vendor_price_mechanisms
        , ARRAY_AGG(
          STRUCT(
            test_id
            , test_name
            , CURRENT_TIMESTAMP() BETWEEN active_from AND active_to AS is_test_active_today
            , priority_price_config_hash AS test_price_config_hash -- unique hash to identify all price configuration across target groups
            , STRUCT(
              travel_time_differs_across_variants
              , dbmov_differs_across_variants
              , surge_mov_differs_across_variants
              , small_order_fee_differs_across_variants
              , fleet_delay_differs_across_variants
              , basket_value_differs_across_variants
              , priority_fee_differs_across_variants
              , vendor_has_different_time_condition_than_asa
              , vendor_has_different_customer_condition_than_asa
              , vendor_has_different_customer_area_than_asa
            ) as test_change_flags
            , STRUCT(
              vendor_has_dbdf
              , vendor_has_dbmov
              , vendor_has_surge_mov
              , vendor_has_small_order_fee
              , vendor_has_fleet_delay
              , vendor_has_basket_value
              , vendor_has_priority_fee
              , time_condition_hash IS NOT NULL AS vendor_time_condition
              , customer_condition_hash IS NOT NULL AS vendor_customer_condition
              , customer_area_hash IS NOT NULL AS vendor_customer_area
            ) AS test_price_mechanisms
            , STRUCT(
              time_condition_hash
              , customer_condition_hash
              , customer_area_hash
              , time_condition_ids
              , customer_condition_ids
              , customer_area_ids
              --- for debugging
              , asa_customer_condition_ids
              , asa_time_condition_ids
              , asa_customer_area_ids
            ) AS conditions
            , priority_price_configs
          )
          ORDER BY test_id
        ) AS dps_configuration_history
        , STRUCT(
          ANY_VALUE(asa_id) as asa_id
          , ANY_VALUE(asa_customer_condition_ids) as asa_customer_condition_ids
          , ANY_VALUE(asa_time_condition_ids) as asa_time_condition_ids
          , ANY_VALUE(asa_customer_area_ids) as asa_customer_area_ids
        ) AS asa_conditions
        FROM add_condition_flags
        GROUP BY 1, 2, 3, 4, 5
    )

  ##############################
  
  ############################## FINAL TABLE


    -- SELECT * EXCEPT(dps_configuration_history)
    SELECT *
    FROM aggregate_values
    WHERE TRUE
    -- AND vendor_id = "35045"
    -- AND entity_id = "TB_AE"
    -- ORDER BY active_from
    -- LIMIT 1000;
    ;
####################################################

-- #################################################### Vendor Experiment configuration versions
--     /*
--     The next CTEs changes the granularity to a vendor level, i.e, 
--     each row represents a versioning in which a vendor was part of an experiment

--     We'll focus first on deduplicating consecutives versioning. Then, we'll take into account
--     that vendor could be part of more than one experiment at a time. 
--     Our aim will be to have a table where each row represent the periods
--     where a vendor was part of at least one test. 
--     */

--     CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.vendor_experiment_configuration_versions` AS
--     WITH 
--     -- unnest_vendors AS (
--     --   SELECT * EXCEPT(sorted_vendor_id)
--     --   FROM `logistics-data-storage-staging.long_term_pricing.pricing_experiment_configuration_versions`
--     --   LEFT JOIN UNNEST(sorted_vendor_id) as vendor_id
--     --   WHERE vendor_id IS NOT NULL
--     --   -- GROUP BY 1,2,3,4,5,6
--     -- )
    
--     unnest_vendors AS (
--       SELECT * EXCEPT(sorted_vendor_id)
--       FROM `logistics-data-storage-staging.long_term_pricing.pricing_experiment_configuration_versions`
--       LEFT JOIN UNNEST(sorted_vendor_id) as vendor_id
--       WHERE vendor_id IS NOT NULL
--     )

--     , get_previous_version AS (
--       SELECT *
--         , LAG(priority_price_config_hash) OVER(PARTITION BY entity_id, test_id, vendor_id, priority ORDER BY test_sub_period_from) as prev_hash
--       FROM unnest_vendors
--     )

--     , deduplicate_versions AS (
--       SELECT *
--       FROM get_previous_version
--       WHERE (
--           CASE
--             WHEN prev_hash IS NULL THEN TRUE --we keep the first version
--             WHEN (priority_price_config_hash = prev_hash) THEN FALSE -- we remove version when the current one is equal to the previous one
--             ELSE TRUE
--           END
--       )
--     )

--     , get_latest_test_sub_period AS (
--       SELECT 
--       entity_id
--       , test_id
--       , IFNULL(MAX(test_end_date), "2099-01-01") as latest_test_sub_period_to
--       FROM `fulfillment-dwh-production.cl.dps_experiment_setups`
--       GROUP BY 1,2
--     )

--     , add_sub_period_test AS (
--       /*
--       The "last" version is always bounded by the test_end_date
--       */
--       SELECT a.* EXCEPT(test_sub_period_to)
--       , IFNULL(
--         LEAD(test_sub_period_from) OVER(PARTITION BY entity_id, test_id, vendor_id ORDER BY test_sub_period_from)
--         , latest_test_sub_period_to
--       ) AS test_sub_period_to
--       FROM deduplicate_versions a
--       LEFT JOIN get_latest_test_sub_period USING(entity_id, test_id)
--     )
--   ##############################

--   ############################## FIND OVERLAPPING TESTS

--     -- Next, we find all overlapping experiments by doing a self-join on the same entity_id and vendor_id
--     -- and keep only those rows that have overlapping active periods. Overlapping periods are defined as:
--     --   - Test A starts before Test B ends
--     --   - Test A ends after Test B starts
--     -- Left join is used to keep those vendors that are only part of one Test.
--     , get_overlap_experiments AS (
--       SELECT
--         a.* EXCEPT(test_sub_period_to, test_sub_period_from)
--         , a.test_sub_period_from AS initial_from
--         , IF(b.test_sub_period_from IS NULL, a.test_sub_period_from, GREATEST(a.test_sub_period_from, b.test_sub_period_from)) AS active_from
--         , IF(b.test_sub_period_to IS NULL, a.test_sub_period_to, LEAST(a.test_sub_period_to, b.test_sub_period_to)) AS active_to
--       FROM add_sub_period_test a
--       LEFT JOIN add_sub_period_test b
--         ON a.entity_id = b.entity_id
--         AND a.vendor_id = b.vendor_id
--         AND a.test_id < b.test_id
--         AND a.test_sub_period_from < b.test_sub_period_to
--         AND a.test_sub_period_to > b.test_sub_period_from
--     )

--     -- From the previous CTE we obtain all "important" timestamps for each Test.
--     -- We need to aggregate them and create a unique list of timestamps for each vendor.
--     -- These timestamps will mean that a new Test version is created, because a Test starts, ends, or changes its active status.
--     -- The initial_from aggregration keeps track of versions where a vendor is only part of a single Test.

--     , aggregate_by_versioning AS (
--       SELECT
--         entity_id
--         , vendor_id
--         , ARRAY_AGG(initial_from) initial_from_agg
--         , ARRAY_AGG(active_from) AS active_from_agg
--         , ARRAY_AGG(active_to) AS active_to_agg
--       FROM get_overlap_experiments
--       GROUP BY 1, 2
--     )

--     , ordered_timestamp AS (
--       SELECT
--         entity_id
--         , vendor_id
--         , ARRAY(
--             SELECT DISTINCT ts
--             FROM UNNEST(ARRAY_CONCAT(initial_from_agg, active_from_agg, active_to_agg)) ts
--             ORDER BY 1
--         ) AS ordered_timestamp_array
--       FROM aggregate_by_versioning
--     )

--     , new_experiment_timestamps AS (
--       SELECT 
--         * EXCEPT(ordered_timestamp_array)
--         , LEAD(active_from) OVER(PARTITION BY entity_id, vendor_id ORDER BY active_from) AS active_to
--       FROM ordered_timestamp
--       LEFT JOIN UNNEST(ordered_timestamp_array) active_from
--     )

--     , remove_unnecesary_versions AS (
--       SELECT *
--       FROM new_experiment_timestamps
--       WHERE active_to IS NOT NULL
--         AND active_from < active_to
--     )

--     , add_back_experiment_info AS (
--       SELECT a.*
--       , b.* EXCEPT(entity_id, vendor_id)
--       FROM remove_unnecesary_versions a
--       INNER JOIN add_sub_period_test b
--       ON a.entity_id = b.entity_id
--       AND a.vendor_id = b.vendor_id
--       AND a.active_from >= b.test_sub_period_from
--       AND a.active_to <= b.test_sub_period_to
--     )

--     , remove_priority_granularity AS (
--       SELECT
--       entity_id
--       , test_id
--       , test_name
--       , active_from
--       , active_to
--       , vendor_id
--         --- a vendor can be part of multiple target groups
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT TO_BASE64(priority_price_config_hash) ORDER BY TO_BASE64(priority_price_config_hash)), "")) as priority_price_config_hash
--         -- these are to keep track of the unique set of conditions used in the test setup
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT time_condition_id IGNORE NULLS ORDER BY time_condition_id), " | ") ) as time_condition_hash
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") ) as customer_condition_hash
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") ) as customer_area_hash
--       , MAX(target_group_component_changes.travel_time_differs_across_variants) AS travel_time_differs_across_variants
--       , MAX(target_group_component_changes.dbmov_differs_across_variants) AS dbmov_differs_across_variants
--       , MAX(target_group_component_changes.surge_mov_differs_across_variants) AS surge_mov_differs_across_variants
--       , MAX(target_group_component_changes.small_order_fee_differs_across_variants) AS small_order_fee_differs_across_variants
--       , MAX(target_group_component_changes.fleet_delay_differs_across_variants) AS fleet_delay_differs_across_variants
--       , MAX(target_group_component_changes.basket_value_differs_across_variants) AS basket_value_differs_across_variants
--       , MAX(target_group_component_changes.priority_fee_differs_across_variants) AS priority_fee_differs_across_variants
--       , MAX(target_group_scheme_price_mechanisms.is_dbdf) AS vendor_has_dbdf
--       , MAX(target_group_scheme_price_mechanisms.is_dbmov) AS vendor_has_dbmov
--       , MAX(target_group_scheme_price_mechanisms.is_surge_mov) AS vendor_has_surge_mov
--       , MAX(target_group_scheme_price_mechanisms.is_small_order_fee) AS vendor_has_small_order_fee
--       , MAX(target_group_scheme_price_mechanisms.is_fleet_delay) AS vendor_has_fleet_delay
--       , MAX(target_group_scheme_price_mechanisms.is_basket_value_deal) AS vendor_has_basket_value
--       , MAX(target_group_scheme_price_mechanisms.is_priority_fee) AS vendor_has_priority_fee

--       --- these are for debugging
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT time_condition_id IGNORE NULLS ORDER BY time_condition_id), " | ") as time_condition_ids
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") customer_condition_ids
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids ignore nulls ORDER BY customer_area_ids), " | ") AS customer_area_ids
--       , ARRAY_CONCAT_AGG(priority_price_config ORDER BY priority) as priority_price_configs
--       FROM add_back_experiment_info
--       GROUP BY 1,2,3,4,5,6
--     )

    


--   ##############################

--   ############################## GET FLAG FOR TESTING CONDITIONS AGGREGATE DATA

--     /*
--     We now aim to get columns that indicate us if for a given test we're trying a new set of conditions.
--     We need to compare the conditions used in the experiment with those in the ASA where the vendor are part of.
--     */

--     , load_asa_mechanisms AS (
--         --  Load current Data
--         -- ASA component mechanisms
--         SELECT
--           entity_id
--           , vendor_code as vendor_id
--           , active_from
--           , asa_id
--           , IFNULL(active_to, CURRENT_TIMESTAMP()) AS active_to
--           , CAST(customer_condition_id AS STRING) customer_condition_id
--           , CAST(schedule_id AS STRING) schedule_id
--           , IF(ARRAY_LENGTH(area_configs) > 0
--               , ARRAY_TO_STRING(ARRAY(SELECT CAST(x.area_id AS STRING) FROM UNNEST(area_configs) x ORDER BY x.area_id),", ")
--               , NULL
--           ) AS customer_area_ids

--         FROM `fulfillment-dwh-production.cl.vendor_asa_sfo_subscription_configuration_versions`
--         LEFT JOIN UNNEST(dps_asa_configuration_history) asa
--         LEFT JOIN UNNEST(asa.asa_price_config) apc
--         -- FROM `{{ params.project_id }}.cl.vendor_asa_sfo_subscription_configuration_versions`
--         WHERE type = 'BASIC'
--     )

--     , asa_condition_hashes AS (

--       SELECT 
--       entity_id
--       , vendor_id
--       , active_from
--       , active_to
--       , asa_id
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") asa_customer_condition_ids
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") asa_time_condition_ids
--       , ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") asa_customer_area_ids
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_condition_id IGNORE NULLS ORDER BY customer_condition_id), " | ") ) asa_customer_condition_hash
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT schedule_id IGNORE NULLS ORDER BY schedule_id), " | ") ) asa_time_condition_hash
--       , SHA256(ARRAY_TO_STRING(ARRAY_AGG(DISTINCT customer_area_ids IGNORE NULLS ORDER BY customer_area_ids), " | ") ) asa_customer_area_hash
--       FROM load_asa_mechanisms
--       GROUP BY 1,2,3,4,5
--     )

--     , join_asa_mechanisms AS (
--       SELECT exper.* EXCEPT(active_from, active_to)
--       , asa.* EXCEPT(entity_id, vendor_id, active_from, active_to)
--       , GREATEST(exper.active_from, asa.active_from) active_from
--       , LEAST(exper.active_to, asa.active_to) active_to
--       FROM remove_priority_granularity exper
--       LEFT JOIN asa_condition_hashes asa
--         ON exper.entity_id = asa.entity_id 
--         AND exper.vendor_id = asa.vendor_id 
--       WHERE exper.active_from < asa.active_to 
--         AND exper.active_to > asa.active_from
--     )

--     , add_condition_flags AS (
--       SELECT *
--       , CASE
--           WHEN time_condition_hash IS NULL AND asa_time_condition_hash IS NULL THEN FALSE 
--           WHEN time_condition_hash IS NULL OR asa_time_condition_hash IS NULL THEN TRUE 
--           ELSE time_condition_hash <> asa_time_condition_hash
--         END AS vendor_has_different_time_condition_than_asa

--       , CASE
--           WHEN customer_condition_hash IS NULL AND asa_customer_condition_hash IS NULL THEN FALSE 
--           WHEN customer_condition_hash IS NULL OR asa_customer_condition_hash IS NULL THEN TRUE 
--           ELSE customer_condition_hash <> asa_customer_condition_hash
--         END AS vendor_has_different_customer_condition_than_asa

--       , CASE
--           WHEN customer_area_hash IS NULL AND asa_customer_area_hash IS NULL THEN FALSE 
--           WHEN customer_area_hash IS NULL OR asa_customer_area_hash IS NULL THEN TRUE 
--           ELSE customer_area_hash <> asa_customer_area_hash
--         END AS vendor_has_different_customer_area_than_asa

--       FROM join_asa_mechanisms
--     )
--   ##############################

--   ############################## AGGREGATE DATA

--     , aggregate_values AS (
--         SELECT
--         "EXPERIMENT" AS type  -- hardcoded to match format of `type` field in cl.vendor_asa_sfo_subscription_configuration_versions, which comes from `hl.dynamic_pricing_vendor_group_assignment`
--         , entity_id
--         , vendor_id
--         , active_from
--         , IF(active_to = "2099-01-01", NULL, active_to) AS active_to
--         , ARRAY_AGG(DISTINCT test_name) as test_names
--         , STRUCT(
--               MAX(travel_time_differs_across_variants) AS has_travel_time_change
--           ,   MAX(dbmov_differs_across_variants) AS has_dbmov_change
--           ,   MAX(surge_mov_differs_across_variants) AS has_surge_mov_change
--           ,   MAX(small_order_fee_differs_across_variants) AS has_small_order_fee_change
--           ,   MAX(fleet_delay_differs_across_variants) AS has_fleet_delay_change
--           ,   MAX(basket_value_differs_across_variants) AS has_basket_value_change
--           ,   MAX(priority_fee_differs_across_variants) AS has_priority_fee_change
--           ,   MAX(vendor_has_different_time_condition_than_asa) AS has_time_condition_change
--           ,   MAX(vendor_has_different_customer_condition_than_asa) AS has_customer_condition_change
--           ,   MAX(vendor_has_different_customer_area_than_asa) AS has_customer_area_change
--         ) as vendor_change_flags
--         , STRUCT(
--             MAX(vendor_has_dbdf) AS vendor_has_dbdf
--           , MAX(vendor_has_dbmov) AS vendor_has_dbmov
--           , MAX(vendor_has_surge_mov) AS vendor_has_surge_mov
--           , MAX(vendor_has_small_order_fee) AS vendor_has_small_order_fee
--           , MAX(vendor_has_fleet_delay) AS vendor_has_fleet_delay
--           , MAX(vendor_has_basket_value) AS vendor_has_basket_value
--           , MAX(vendor_has_priority_fee) AS vendor_has_priority_fee
--           , MAX(time_condition_hash IS NOT NULL) AS vendor_time_condition
--           , MAX(customer_condition_hash IS NOT NULL) AS vendor_customer_condition
--           , MAX(customer_area_hash IS NOT NULL) AS vendor_customer_area
--         ) AS vendor_price_mechanisms
--         , ARRAY_AGG(
--           STRUCT(
--             test_id
--             , test_name
--             , CURRENT_TIMESTAMP() BETWEEN active_from AND active_to AS is_test_active_today
--             , priority_price_config_hash AS test_price_config_hash -- unique hash to identify all price configuration across target groups
--             , STRUCT(
--               travel_time_differs_across_variants
--               , dbmov_differs_across_variants
--               , surge_mov_differs_across_variants
--               , small_order_fee_differs_across_variants
--               , fleet_delay_differs_across_variants
--               , basket_value_differs_across_variants
--               , priority_fee_differs_across_variants
--               , vendor_has_different_time_condition_than_asa
--               , vendor_has_different_customer_condition_than_asa
--               , vendor_has_different_customer_area_than_asa
--             ) as test_change_flags
--             , STRUCT(
--               vendor_has_dbdf
--               , vendor_has_dbmov
--               , vendor_has_surge_mov
--               , vendor_has_small_order_fee
--               , vendor_has_fleet_delay
--               , vendor_has_basket_value
--               , vendor_has_priority_fee
--               , time_condition_hash IS NOT NULL AS vendor_time_condition
--               , customer_condition_hash IS NOT NULL AS vendor_customer_condition
--               , customer_area_hash IS NOT NULL AS vendor_customer_area
--             ) AS test_price_mechanisms
--             , STRUCT(
--               time_condition_hash
--               , customer_condition_hash
--               , customer_area_hash
--               , time_condition_ids
--               , customer_condition_ids
--               , customer_area_ids
--               --- for debugging
--               , asa_customer_condition_ids
--               , asa_time_condition_ids
--               , asa_customer_area_ids
--             ) AS conditions
--             , priority_price_configs
--           )
--           ORDER BY test_id
--         ) AS dps_configuration_history
--         , STRUCT(
--           ANY_VALUE(asa_id) as asa_id
--           , ANY_VALUE(asa_customer_condition_ids) as asa_customer_condition_ids
--           , ANY_VALUE(asa_time_condition_ids) as asa_time_condition_ids
--           , ANY_VALUE(asa_customer_area_ids) as asa_customer_area_ids
--         ) AS asa_conditions
--         FROM add_condition_flags
--         GROUP BY 1, 2, 3, 4, 5
--     )

--   ##############################
  
--   ############################## FINAL TABLE

--     SELECT *
--     FROM aggregate_values
--     WHERE TRUE
--     -- LIMIT 100

--   ##############################

-- ####################################################


-- AND test_name = "AE_20231213_R_B_R_Al_Ain_DBDF_Calibration"
-- AND vendor_id = "35045"
-- AND entity_id = "TB_AE"
-- AND entity_id = "PY_BO"
-- AND vendor_id = "230633"
-- ORDER BY active_from
-- ORDER BY test_sub_period_from
-- QUALIFY SUM(1) OVER(PARTITION BY entity_id, vendor_id, active_from) > 1
-- ORDER BY entity
-- LIMIT 100
-- AND test_name = "BH_20240214_R_B_O_Area_Based_DF_Decrease_B/A_exp"
-- AND entity_id = "TB_BH"
-- AND vendor_group_id in (1416,1417,1418,1419,1429,1421,1422)
-- ORDER BY test_sub_period_from, priority, vendor_group_id
-- ORDER BY vendor_id, test_sub_period_from
-- ORDER BY test_sub_period_from_new, priority, vendor_group_id
-- ORDER BY vendor_group_updated_at, vendor_group_id