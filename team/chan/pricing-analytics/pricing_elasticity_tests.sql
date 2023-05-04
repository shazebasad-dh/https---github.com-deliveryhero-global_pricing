
################################### DPS EXPERIMENT SETUPS ENRICHED
  /*
  The "enriched" refers to add the price configuration to the
  original experiment setups. Therefore, it also introduces "versioning" in the case the config was changed while the test was active.

  The granularity of this table is at the test level configuration, in contrast to the test - variant - target group granularity of
  the original dps_experiment_setups table. If a test had a constant configuration throughout the test, it will have only one row. The
  price configuration is a nested field.

  At the end, there's a "quality" check that defaults to true if the price change did not change, e.g.,
  the scheme name changed but the price config was the same OR if the there's a specific version that was active
  at least 95% of the time the test was active.
  */
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_experiment_setups_enriched` AS
  with region_data as (
  SELECT DISTINCT
  region
  , country_code
  from `fulfillment-dwh-production.cl.countries`
  )

  , load_test as (
    select region
    , test_id
    , test_name
    , entity_id
    , d.country_code
    , DATETIME_TRUNC(test_start_date, MINUTE) as test_start_date
    , ifnull(DATETIME_TRUNC(test_end_date, MINUTE), CURRENT_TIMESTAMP()) as test_end_date
    , priority
    , variation_group
    , price_scheme_id as scheme_id
    , schedule.id as time_condition_id
    , customer_condition.id  as customer_condition_id
    , customer_areas as customer_area_ids


  from `fulfillment-dwh-production.cl.dps_experiment_setups` d
  LEFT JOIN region_data
    USING(country_code)
  WHERE TRUE
  and misconfigured = FALSE -- don't use misconfigured
  and test_start_date > "2022-01-01"
  )
  ############ JOIN SCHEME CONFIGS

  , load_schemes as (
    SELECT * except(scheme_active_from, scheme_active_to)
    , DATETIME_TRUNC(scheme_active_from, MINUTE) AS scheme_active_from
    , IFNULL(DATETIME_TRUNC(scheme_active_to,MINUTE), "2099-01-01") AS scheme_active_to
    FROM `dh-logistics-product-ops.pricing.dps_config_versions_v2`
    -- We omit versions with less than 1 minute lifetime
    WHERE (
      (TIMESTAMP_DIFF(scheme_active_to, scheme_active_from, MINUTE) > 1)
      OR scheme_active_to IS NULL
    ) 
  )

  , join_scheme_config as (
    select td.*
    , ls.* EXCEPT(entity_id, scheme_id, scheme_active_from, scheme_active_to)
    , GREATEST(scheme_active_from, test_start_date) AS test_scheme_window_from -- create new window from the schemes that overlap
    , LEAST(scheme_active_to, test_end_date) AS test_scheme_window_to
    FROM load_test td
    LEFT JOIN load_schemes ls
      ON td.entity_id = ls.entity_id
      AND td.scheme_id = ls.scheme_id
    WHERE TRUE
    --- overlapping filter to overlapping tests and schemes versions
    AND ( (test_start_date < scheme_active_to)
        AND (test_end_date > scheme_active_from))
    
  )

  ############ new timestamp

    , agg_by_row_id as (
      SELECT 
        entity_id
        , test_name
        , test_start_date
        , ARRAY_AGG(test_scheme_window_from) test_scheme_window_from_agg
        , ARRAY_AGG(test_scheme_window_to) test_scheme_window_to_agg
      FROM join_scheme_config
      GROUP BY 1,2, 3
    )

    , get_orderred_timestamp as (
      SELECT 
        entity_id
      , test_name
      , test_start_date
      , ARRAY(
        SELECT DISTINCT
        x
        FROM UNNEST(ARRAY_CONCAT(test_scheme_window_from_agg,test_scheme_window_to_agg)) x ORDER BY x
      ) as ordered_timestamp
      FROM agg_by_row_id
    )

    , add_new_active_to AS (
      SELECT * EXCEPT(ordered_timestamp)
        , LEAD(new_test_scheme_window_from) OVER(PARTITION BY entity_id, test_name, test_start_date ORDER BY new_test_scheme_window_from) as new_test_scheme_window_to
        FROM get_orderred_timestamp
        LEFT JOIN UNNEST(ordered_timestamp) new_test_scheme_window_from
    )

    , remove_last_version as (
      SELECT *
      FROM add_new_active_to
      WHERE new_test_scheme_window_to IS NOT NULL
    )


    , add_original_config as (
      SELECT region
        , test_id
        , rlv.test_name
        , entity_id
        , country_code
        , ld.test_start_date
        , test_end_date
        , new_test_scheme_window_from as test_sub_period_from
        , new_test_scheme_window_to as test_sub_period_to
        , priority
        , variation_group
        , scheme_id
        , time_condition_id
        , customer_condition_id
        , customer_area_ids
      FROM remove_last_version rlv
      LEFT JOIN load_test ld
        USING(entity_id, test_name)
      WHERE TRUE
      AND ld.test_start_date < new_test_scheme_window_to
      AND ld.test_end_date > new_test_scheme_window_from
    )

    , join_scheme_configs_again as (
      select td.*
        , ls.* EXCEPT(entity_id, scheme_id, scheme_active_from, scheme_active_to, country_code)
    FROM add_original_config td
    LEFT JOIN load_schemes ls
      ON td.entity_id = ls.entity_id
      AND td.scheme_id = ls.scheme_id
    WHERE TRUE
    --- overlapping filter to overlapping tests and schemes versions
    AND ( (test_sub_period_from < scheme_active_to)
        AND (test_sub_period_to > scheme_active_from))
    )

    , agg_test_price_config as (
      SELECT region
      , test_id
      , test_name
      , entity_id 
      , country_code
      , test_start_date
      , test_end_date
      , test_sub_period_from
      , test_sub_period_to
      , ARRAY_AGG(
        CONCAT(priority, variation_group, scheme_id, TO_BASE64(scheme_config_hash))
        ORDER BY priority, variation_group
      ) as _test_price_config_hash
      , ARRAY_AGG(
        STRUCT(
          priority
          , variation_group
          , scheme_name
          , scheme_id 
          , time_condition_id
          , customer_condition_id
          , customer_area_ids
          , scheme_config_hash
          , scheme_price_mechanisms
          , scheme_component_ids
          , scheme_component_hashes
          , scheme_component_configs
        )
        ORDER BY priority, variation_group
      ) AS test_price_config
      FROM join_scheme_configs_again
      GROUP BY 1,2,3,4,5,6,7,8,9
    )

    , add_test_hash as (
      SELECT *
      , sha256(
        ARRAY_TO_STRING(_test_price_config_hash, "")
      ) AS test_price_config_hash
      FROM agg_test_price_config
    )

  ############ DEDUPLICATION LAYER
    , get_past_hash as (
      SELECT * EXCEPT(test_sub_period_to)
      ,  LAG(test_price_config_hash) OVER(PARTITION BY entity_id, test_name ORDER BY test_sub_period_from) as prev_hash
      FROM add_test_hash
    )

    , deduplicate_versions as (
      SELECT *
      FROM get_past_hash
      WHERE (
        CASE
          WHEN prev_hash IS NULL THEN TRUE
          WHEN test_price_config_hash = prev_hash THEN FALSE
          ELSE TRUE
        END 
      )
    )

    , enriched_test_setup as (
      SELECT region
        , entity_id
        , test_name
        , test_start_date
        , test_end_date
        , test_sub_period_from
        , IFNULL(LEAD(test_sub_period_from) OVER(PARTITION BY entity_id, test_name ORDER BY test_sub_period_from), test_end_date) AS test_sub_period_to
        , test_price_config_hash
        , test_price_config
      FROM deduplicate_versions
    )


  ############ ADD CHANGE BETWEEN EACH SUBPERIODS

        , add_prev_hashes AS (
          SELECT * except(test_price_config)
            , LAG(scheme_component_hashes) OVER(partition by entity_id, scheme_id, test_name, variation_group, priority ORDER BY test_sub_period_from) as prev_hash
            , LAG(scheme_component_configs) OVER(partition by entity_id, scheme_id, test_name, variation_group, priority ORDER BY test_sub_period_from) as prev_config
            , ROW_NUMBER() OVER(partition by entity_id, scheme_id, test_name, variation_group, priority ORDER BY test_sub_period_to) as n_version
          FROM enriched_test_setup
          LEFT JOIN UNNEST(test_price_config)
        )

        , hashes_changes_flag AS (
          SELECT
          *
          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.travel_time_config_hash IS NULL AND prev_hash.travel_time_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.travel_time_config_hash IS NOT NULL AND prev_hash.travel_time_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.travel_time_config_hash <> prev_hash.travel_time_config_hash THEN 1
            ELSE 0
          END travel_time_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.dbmov_config_hash IS NULL AND prev_hash.dbmov_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.dbmov_config_hash IS NOT NULL AND prev_hash.dbmov_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.dbmov_config_hash <> prev_hash.dbmov_config_hash THEN 1
            ELSE 0
          END dbmov_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.surge_mov_config_hash IS NULL AND prev_hash.surge_mov_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.surge_mov_config_hash IS NOT NULL AND prev_hash.surge_mov_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.surge_mov_config_hash <> prev_hash.surge_mov_config_hash THEN 1
            ELSE 0
          END surgemov_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.small_order_fee_config_hash IS NULL AND prev_hash.small_order_fee_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.small_order_fee_config_hash IS NOT NULL AND prev_hash.small_order_fee_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.small_order_fee_config_hash <> prev_hash.small_order_fee_config_hash THEN 1
            ELSE 0
          END small_order_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.fleet_delay_config_hash IS NULL AND prev_hash.fleet_delay_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.fleet_delay_config_hash IS NOT NULL AND prev_hash.fleet_delay_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.fleet_delay_config_hash <> prev_hash.fleet_delay_config_hash THEN 1
            ELSE 0
          END fleet_delay_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.basket_value_config_hash IS NULL AND prev_hash.basket_value_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.basket_value_config_hash IS NOT NULL AND prev_hash.basket_value_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.basket_value_config_hash <> prev_hash.basket_value_config_hash THEN 1
            ELSE 0
          END basket_value_change_flag

          , CASE 
              WHEN  n_version > 1 AND scheme_component_hashes.service_fee_config_hash IS NULL AND prev_hash.service_fee_config_hash IS NOT NULL THEN 1 
              WHEN  n_version > 1 AND scheme_component_hashes.service_fee_config_hash IS NOT NULL AND prev_hash.service_fee_config_hash IS NULL THEN 1
              WHEN  n_version > 1 AND scheme_component_hashes.service_fee_config_hash <> prev_hash.service_fee_config_hash THEN 1
            ELSE 0
          END service_fee_change_flag

          FROM add_prev_hashes
        )

        , add_total_changes as (
          SELECT * EXCEPT(prev_hash)
          , travel_time_change_flag
              + dbmov_change_flag
              + surgemov_change_flag
              + small_order_change_flag
              + fleet_delay_change_flag
              + basket_value_change_flag
              + service_fee_change_flag
              AS total_config_changes
          , STRUCT(
              travel_time_change_flag
                , dbmov_change_flag
                , surgemov_change_flag
                , small_order_change_flag
                , fleet_delay_change_flag
                , basket_value_change_flag
                , service_fee_change_flag
          ) AS component_changes_from_prev_version_flags
          , STRUCT(
              IF(travel_time_change_flag = 1, prev_config.travel_time_config, NULL) as prev_travel_time_config
            , IF(dbmov_change_flag + surgemov_change_flag + small_order_change_flag > 1 , prev_config.mov_config, NULL) as prev_mov_config
            , IF(fleet_delay_change_flag = 1, prev_config.fleet_delay_config, NULL) as prev_fleet_delay_config
            , IF(basket_value_change_flag = 1, prev_config.basket_value_config, NULL) as prev_basket_value_config
            , IF(service_fee_change_flag = 1, prev_config.service_fee_config, NULL) as prev_service_fee_config
          ) as prev_config_if_changed
          FROM hashes_changes_flag
        )


      , regroup_schema as (
        SELECT region
        , test_name
        , entity_id 
        , test_start_date
        , test_end_date
        , test_sub_period_from
        , test_sub_period_to
        , test_price_config_hash
        , total_config_changes as total_config_changes_from_prev_version
        , SAFE_DIVIDE(
          TIMESTAMP_DIFF(test_sub_period_to, test_sub_period_from, MINUTE)
          , TIMESTAMP_DIFF(test_end_date, test_start_date, MINUTE)
        ) as test_sub_period_duration_percentage
        , ARRAY_AGG(
          STRUCT(
            priority
            , variation_group
            , scheme_name
            , scheme_id 
            , time_condition_id
            , customer_condition_id
            , customer_area_ids
            , scheme_config_hash
            , scheme_price_mechanisms
            , scheme_component_ids
            , scheme_component_hashes
            , component_changes_from_prev_version_flags
            , scheme_component_configs
            , prev_config_if_changed
          )
          ORDER BY priority, variation_group
        ) AS test_price_config
        FROM add_total_changes
        GROUP BY 1,2,3,4,5,6,7,8,9,10
      )

      , subperiods_per_test_with_config_changes as (
      SELECT region
        , entity_id 
        , test_name
        , COUNT(DISTINCT test_sub_period_from) as n_subperiods
        , MAX(test_sub_period_duration_percentage) as  max_period_duration_percentage
        , MAX(total_config_changes_from_prev_version) as max_total_config_changes_from_prev_version
      FROM regroup_schema
      WHERE TRUE
      -- AND total_config_changes > 0 -- only changes in configs
      GROUP BY 1,2,3
      -- HAVING n_subperiods > 1 -- 
    )

    , add_is_good_config_column as (
      SELECT
      region
        , test_name
        , entity_id 
        , test_start_date
        , test_end_date
        , test_sub_period_from
        , test_sub_period_to
        , test_price_config_hash
        , total_config_changes_from_prev_version
        , test_sub_period_duration_percentage
        , CASE 
            WHEN max_period_duration_percentage >= 0.95 then TRUE 
            WHEN max_total_config_changes_from_prev_version = 0 then TRUE 
          ELSE FALSE
          END AS is_test_config_good
        , STRUCT(
          n_subperiods
          , max_period_duration_percentage
          , max_total_config_changes_from_prev_version
        ) as test_subperiods_quality_checks
        , test_price_config
      FROM regroup_schema
      LEFT JOIN subperiods_per_test_with_config_changes
        USING(region, entity_id, test_name)
    )

  ---- final 

    select  *
    from add_is_good_config_column
    WHERE TRUE;
###################################

################################### PRICING TESTING INTENSITY DETAIL V2
  /*
  This table provides the about what was tested in a given test. It does so by comparing
  the control configuration to each variant, at the priority level. It currently only provides
  info about component changes and not about yet conditions mechanisms such as Customer conditions, Time conditions
  and customer location.

  It also includes the control configuration as a column if there was a change. For example,
  if variation1 differs from control in the travel time component, the control travel time configuration will have
  a non null value for the variation1 row.
  */


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.pricing_mechanism_testing_intensity_detail_v2` as

  with load_good_tests as (
    SELECT * except(test_price_config, component_changes_from_prev_version_flags, prev_config_if_changed)
    FROM `dh-logistics-product-ops.pricing.dps_experiment_setups_enriched` 
    LEFT JOIN UNNEST(test_price_config)
    WHERE is_test_config_good
    QUALIFY DENSE_RANK() OVER(PARTITION BY entity_id, test_name ORDER BY test_sub_period_duration_percentage DESC) = 1
  )

      , control_config as (
        SELECT 
        entity_id 
        , test_name
        , priority
        , scheme_id as control_scheme_id
        , scheme_config_hash as control_scheme_config_hash
        , scheme_component_hashes as control_scheme_component_hashes
        , scheme_component_configs as control_scheme_component_configs
        FROM load_good_tests
        WHERE variation_group = "Control"
      )

      , join_control_config as (
        SELECT *
        FROM load_good_tests
        LEFT JOIN control_config
          using(entity_id, test_name, priority)
      )

      , control_changes_flag as (
          SELECT
          *
          , CASE 
              WHEN scheme_component_hashes.travel_time_config_hash IS NULL AND control_scheme_component_hashes.travel_time_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.travel_time_config_hash IS NOT NULL AND control_scheme_component_hashes.travel_time_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.travel_time_config_hash <> control_scheme_component_hashes.travel_time_config_hash THEN 1
            ELSE 0
          END is_travel_time_different_from_control

          , CASE 
              WHEN  scheme_component_hashes.dbmov_config_hash IS NULL AND control_scheme_component_hashes.dbmov_config_hash IS NOT NULL THEN 1 
              WHEN  scheme_component_hashes.dbmov_config_hash IS NOT NULL AND control_scheme_component_hashes.dbmov_config_hash IS NULL THEN 1
              WHEN  scheme_component_hashes.dbmov_config_hash <> control_scheme_component_hashes.dbmov_config_hash THEN 1
            ELSE 0
          END is_dbmov_different_from_control

          , CASE 
              WHEN scheme_component_hashes.surge_mov_config_hash IS NULL AND control_scheme_component_hashes.surge_mov_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.surge_mov_config_hash IS NOT NULL AND control_scheme_component_hashes.surge_mov_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.surge_mov_config_hash <> control_scheme_component_hashes.surge_mov_config_hash THEN 1
            ELSE 0
          END is_surge_mov_different_from_control

          , CASE 
              WHEN scheme_component_hashes.small_order_fee_config_hash IS NULL AND control_scheme_component_hashes.small_order_fee_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.small_order_fee_config_hash IS NOT NULL AND control_scheme_component_hashes.small_order_fee_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.small_order_fee_config_hash <> control_scheme_component_hashes.small_order_fee_config_hash THEN 1
            ELSE 0
          END is_small_order_fee_different_from_control

          , CASE 
              WHEN scheme_component_hashes.fleet_delay_config_hash IS NULL AND control_scheme_component_hashes.fleet_delay_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.fleet_delay_config_hash IS NOT NULL AND control_scheme_component_hashes.fleet_delay_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.fleet_delay_config_hash <> control_scheme_component_hashes.fleet_delay_config_hash THEN 1
            ELSE 0
          END is_fleet_delay_different_from_control

          , CASE 
              WHEN scheme_component_hashes.basket_value_config_hash IS NULL AND control_scheme_component_hashes.basket_value_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.basket_value_config_hash IS NOT NULL AND control_scheme_component_hashes.basket_value_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.basket_value_config_hash <> control_scheme_component_hashes.basket_value_config_hash THEN 1
            ELSE 0
          END is_basket_value_different_from_control

          , CASE 
              WHEN scheme_component_hashes.service_fee_config_hash IS NULL AND control_scheme_component_hashes.service_fee_config_hash IS NOT NULL THEN 1 
              WHEN scheme_component_hashes.service_fee_config_hash IS NOT NULL AND control_scheme_component_hashes.service_fee_config_hash IS NULL THEN 1
              WHEN scheme_component_hashes.service_fee_config_hash <> control_scheme_component_hashes.service_fee_config_hash THEN 1
            ELSE 0
          END is_service_fee_different_from_control

          FROM join_control_config
      )

      , add_total_changes_from_control as (
          SELECT * 
          , is_travel_time_different_from_control
              + is_dbmov_different_from_control
              + is_surge_mov_different_from_control
              + is_small_order_fee_different_from_control
              + is_fleet_delay_different_from_control
              + is_basket_value_different_from_control
              + is_service_fee_different_from_control
            AS total_changes_from_control

          , STRUCT(
              IF(is_travel_time_different_from_control = 1,TRUE,FALSE) AS is_travel_time_different_from_control
                , IF(is_dbmov_different_from_control = 1, TRUE, FALSE) AS is_dbmov_different_from_control
                , IF(is_surge_mov_different_from_control = 1, TRUE, FALSE) AS is_surge_mov_different_from_control
                , IF(is_small_order_fee_different_from_control = 1, TRUE, FALSE) AS is_small_order_fee_different_from_control
                , IF(is_fleet_delay_different_from_control = 1, TRUE, FALSE) AS is_fleet_delay_different_from_control
                , IF(is_basket_value_different_from_control = 1, TRUE, FALSE) AS is_basket_value_different_from_control
                , IF(is_service_fee_different_from_control = 1, TRUE, FALSE) AS is_service_fee_different_from_control
          ) AS component_change_from_control_flags


          , STRUCT(
              IF(is_travel_time_different_from_control = 1, control_scheme_component_configs.travel_time_config, NULL) as control_travel_time_config
            , IF(is_dbmov_different_from_control + is_surge_mov_different_from_control + is_small_order_fee_different_from_control > 1 , control_scheme_component_configs.mov_config, NULL) as control_mov_config
            , IF(is_fleet_delay_different_from_control = 1, control_scheme_component_configs.fleet_delay_config, NULL) as control_fleet_delay_config
            , IF(is_basket_value_different_from_control = 1, control_scheme_component_configs.basket_value_config, NULL) as control_basket_value_config
            , IF(is_service_fee_different_from_control = 1, control_scheme_component_configs.service_fee_config, NULL) as control_service_fee_config
          ) as control_config_if_changed
          FROM control_changes_flag
      )

      , aggregate_changes_test_level as (

        select 
        region
        , entity_id
        , test_name
        , MAX(component_change_from_control_flags.is_travel_time_different_from_control) AS test_is_travel_time_different_from_control
        , MAX(component_change_from_control_flags.is_dbmov_different_from_control) AS test_is_dbmov_different_from_control
        , MAX(component_change_from_control_flags.is_surge_mov_different_from_control) AS test_is_surge_mov_different_from_control
        , MAX(component_change_from_control_flags.is_small_order_fee_different_from_control) AS test_is_small_order_fee_different_from_control
        , MAX(component_change_from_control_flags.is_fleet_delay_different_from_control) AS test_is_fleet_delay_different_from_control
        , MAX(component_change_from_control_flags.is_basket_value_different_from_control) AS test_is_basket_value_different_from_control
        , MAX(component_change_from_control_flags.is_service_fee_different_from_control) AS test_is_service_fee_different_from_control

        from add_total_changes_from_control
        GROUP BY 1,2,3
      )

      , regroup_schema as (
        SELECT
        a.region
        , a.entity_id
        , a.test_name
        , test_start_date
        , test_end_date

        , IF(test_is_travel_time_different_from_control,1,0)
          + IF(test_is_dbmov_different_from_control,1,0)
          + IF(test_is_surge_mov_different_from_control,1,0)
          + IF(test_is_small_order_fee_different_from_control,1,0)
          + IF(test_is_fleet_delay_different_from_control,1,0)
          + IF(test_is_basket_value_different_from_control,1,0)
          + IF(test_is_service_fee_different_from_control,1,0)
        AS test_total_changes_from_control

        , test_is_travel_time_different_from_control
        , test_is_dbmov_different_from_control
        , test_is_surge_mov_different_from_control
        , test_is_small_order_fee_different_from_control
        , test_is_fleet_delay_different_from_control
        , test_is_basket_value_different_from_control
        , test_is_service_fee_different_from_control

        , ARRAY_AGG(
          STRUCT(
          priority
          , variation_group
          , scheme_id
          , total_changes_from_control
          , component_change_from_control_flags
          , time_condition_id
          , customer_condition_id
          , customer_area_ids
          , scheme_component_ids
          , scheme_component_configs
          , control_config_if_changed
          ) 
          ORDER BY priority, variation_group
        ) test_price_config_with_control_changes

        FROM add_total_changes_from_control a
        LEFT JOIN aggregate_changes_test_level b
        ON a.region = b.region
        AND a.entity_id = b.entity_id
        AND a.test_name = b.test_name
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
    )
  select *
  FROM regroup_schema;
###################################

#################################### PRICING ELASTICITY TESTS SUMMARY
  /*
  This table provides information about elasticity tests done so far.

  An elasticity test is that one where the Travel Time tiers are kept constant across the variants.
  Only the price is changed by, ideally, a fixed value. To identify them, we compare the travel time thresholds by creating a hash from them.

  The table provides informationa about the range of differences in local currency the test covers.
  */
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.pricing_elasticity_tests_summary` AS

  #################################### FILTER TO RELEVANT TESTS

    with load_tt_tests as (
      SELECT
        region
        , entity_id
        , test_name
        , test_start_date
        , test_end_date
        , config.variation_group
        , config.priority
        , config.scheme_component_configs.travel_time_config
        , config.control_config_if_changed.control_travel_time_config
        , SHA256(
          ARRAY_TO_STRING(
            ARRAY(SELECT CAST(travel_time_threshold AS STRING) FROM UNNEST(scheme_component_configs.travel_time_config))
            , ""
          )) travel_time__tier_hash
        , SHA256(
          ARRAY_TO_STRING(
            ARRAY(SELECT CAST(travel_time_threshold AS STRING) FROM UNNEST(control_config_if_changed.control_travel_time_config))
            , ""
          )) as control_travel_time__tier_hash
      FROM `dh-logistics-product-ops.pricing.pricing_mechanism_testing_intensity_detail_v2`
      LEFT JOIN UNNEST(test_price_config_with_control_changes) config
      WHERE test_is_travel_time_different_from_control = TRUE 
      and variation_group <> "Control"
      -- and test_start_date >= "2022-07-01"
    ) 

    , test_with_equal_tt as (
        select 
          region
          , entity_id
          , test_name
          , test_start_date
          , test_end_date
          , ARRAY_LENGTH(travel_time_config) AS n_travel_time_tiers
          , COUNT(DISTINCT variation_group) as n_variations
        from load_tt_tests
        where travel_time__tier_hash = control_travel_time__tier_hash
        -- AND ARRAY_LENGTH(travel_time_config) > 1
        -- at least 2 weeks of data
        -- AND TIMESTAMP_DIFF(test_end_date, test_start_date, DAY) > 14
        GROUP BY 1,2,3,4,5,6
        HAVING n_variations > 1
    )

    -- , orders_per_test as (
    --     SELECT
    --       a.test_name
    --       , n_travel_time_tiers
    --       , n_variations
    --       , COUNT(DISTINCT platform_order_code) as n_orders
    --       , COUNT(DISTINCT case when treatment then platform_order_code end) as n_treatment_orders
    --     FROM test_with_equal_tt a
    --     LEFT JOIN `fulfillment-dwh-production.cl.dps_ab_test_orders_v2` b
    --       ON a.test_name = b.test_name
    --       AND b.created_date >= "2022-06-01"
    --       AND b.is_sent
    --       AND b.variant <> "Original" 
    --       AND b.variant is not null
    --     GROUP BY 1,2,3
    -- )

  ####################################
  #################################### CALCULATE DIFFERENCES PER TIER 

    , calculate_diff_per_tier as (
        select 
          test_name
          , variation_group
          , priority
          , scheme_id
          , tier
          , tt.travel_time_threshold
          , tt.travel_time_fee
          , ctt.travel_time_fee as control_travel_time_fee
          , tt.travel_time_fee - ctt.travel_time_fee as tier_diff
        from `dh-logistics-product-ops.pricing.pricing_mechanism_testing_intensity_detail_v2` a
        LEFT JOIN UNNEST(test_price_config_with_control_changes)
        LEFT JOIN UNNEST(scheme_component_configs.travel_time_config) tt
          WITH OFFSET as tier
        LEFT JOIN UNNEST(control_config_if_changed.control_travel_time_config) ctt
          WITH OFFSET as control_tier
        WHERE TRUE
        -- where test_name = "TH_20230313_Maha Sarakham_price sensitivity1"
        and variation_group <> "Control"
        and tier = control_tier
        -- order by variation_group
    )

    , aggregate_per_at_target_group as (
      /*
      In a pure elasticity test, no the difference is constant across all
      target groups? 
      */

      select
      test_name
      , variation_group
      , priority
      , AVG(tier_diff) as avg_diff_across_tier
      , AVG(tier_diff) = MIN(tier_diff) as is_diff_constant
      FROM calculate_diff_per_tier
      GROUP BY 1,2,3
    )

    , aggregate_at_variant as (
      SELECT
      test_name
      , variation_group
      , MIN(is_diff_constant)  as is_diff_constant_in_variant
      , AVG(avg_diff_across_tier) as avg_diff_across_variant -- we use to define the range of values the test covers
      , ARRAY_AGG(
        STRUCT(
          priority
          , avg_diff_across_tier
          , is_diff_constant
        ) 
      ) as target_group_changes
      FROM aggregate_per_at_target_group
      GROUP BY 1,2
    )

    , aggregate_at_test_level as (
      select 
      test_name
      , MIN(avg_diff_across_variant) as min_diff_per_tier
      , MAX(avg_diff_across_variant) as max_diff_per_tier
      , ARRAY_AGG(avg_diff_across_variant ORDER BY avg_diff_across_variant) as differences_per_each_variant
      , MIN(is_diff_constant_in_variant) as is_diff_constant_in_test
      , ARRAY_AGG(
        STRUCT(
        variation_group
        , target_group_changes
        )
      ) as variant_differences
      FROM aggregate_at_variant
      GROUP BY 1
      -- ORDER BY 1
    )


  ####################################
    , join_to_relevant_test AS (
        SELECT a.*
          , b.* EXCEPT(test_name)
          -- , a.variant_differences
        FROM test_with_equal_tt a
        INNER JOIN aggregate_at_test_level b -- test we're interested in
          ON a.test_name = b.test_name
    )

    SELECT *
    FROM join_to_relevant_test
    ORDER BY 1,2,3,4,5;
####################################