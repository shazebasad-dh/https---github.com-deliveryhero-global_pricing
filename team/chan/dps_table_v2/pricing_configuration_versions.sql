----------------------------------------------------------------------------------------------------------------------------
--                NAME: pricing_configuration_versions.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: 2022-10-24
--         DESCRIPTION: This table contains all historical information about DPS price scheme configurations, on version level.
--
--        QUERY OUTPUT: Every version of every configuration within a pricing scheme can be obtained.
--               NOTES: Dynamic Pricing Schemes are made up of components such has Travel Time Fees, MOV Fees, Delay Fees, Basket Value Fees and Service Fees. From 2022-10 onwards, all components are optional, i.e, a scheme can only contain a basket value fee.
--                      Within MOV components there could be Flat schemes that are hard or soft. Hard means the user cant order if the order value is less than the MOV. Soft means that the user can order by paying a fee to compensate
--                      MOV components can also be distance based, delay fee based or both. Distance based is similar to travel time schemes, and depends on a fixed distance between the user and the vendor. Delay Fees depend on fleet delay, and can be different for the same combination of user and vendor location, depending on rider supply and order demand .
--             UPDATED: 2022-11-24 | | | Add support for partial schemes, add small order fee and add component hashes
--                      ----------------------------------


  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_config_versions_v2`
  CLUSTER BY country_code, entity_id, scheme_id
  AS
  WITH 
  -- Surge MOV -- The following CTEs get the data for Surge MOV (or fleet delay MOV) and aggregate it based on active_from.
  get_surge_mov_data AS (
    SELECT country_code
      , mov_configuration_row_id
      , delay_threshold
      , minimum_order_value AS surge_mov_value
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_fleet_delay_mov_configuration`
    WHERE deleted = FALSE
  )

  , surge_configuration AS (
    SELECT country_code
      , mov_configuration_row_id
      , active_from
      , ARRAY_AGG(surge_mov_value) AS _delay_fees
      , ARRAY_AGG(
          STRUCT(delay_threshold
            , surge_mov_value
          )
        ) AS surge_mov_row_config
    FROM get_surge_mov_data
    GROUP BY 1, 2, 3
  )

  , surge_sort_configuration AS (
    SELECT country_code
      , mov_configuration_row_id
      , active_from
      , _delay_fees
      , ARRAY(
          SELECT AS STRUCT
          delay_threshold
          , surge_mov_value
          FROM UNNEST(surge_mov_row_config) 
          ORDER BY delay_threshold NULLS LAST
        ) AS surge_mov_row_config
    FROM surge_configuration
  )

  , surge_mov_hash AS (
    SELECT *
      , SHA256(
        CONCAT(
            ARRAY_TO_STRING( 
            ARRAY(SELECT CAST(delay_threshold AS STRING) FROM UNNEST(surge_mov_row_config))
            , "")
          , ARRAY_TO_STRING( 
            ARRAY (SELECT CAST(surge_mov_value AS STRING) FROM UNNEST(surge_mov_row_config))
            , "")
          )
      ) AS surge_mov_row_config_hash
    FROM surge_sort_configuration
  )

  , select_unique_fees AS (
    SELECT * EXCEPT(_delay_fees)
      , ARRAY(SELECT DISTINCT x from UNNEST(_delay_fees) x WHERE x <> 0) AS unique_surge_mov_fees -- For cleaning purposes. Sometimes local and regional team-members update lines and put the same delay fee for more than one tier instead of deleting them. To understand whether there actually was a delay fee in this version, we need to have distinct non-zero fees. Delay fees can be positive or negative.
    FROM surge_mov_hash
  )

  , surge_final_fees AS (
    SELECT country_code
      , mov_configuration_row_id
      , active_from
      , surge_mov_row_config
      , surge_mov_row_config_hash
      , ARRAY_LENGTH(unique_surge_mov_fees) >= 1 AS is_surge_mov -- If there is one or more non-zero delay fee, it means that there is an active surge mov configuration.
    FROM select_unique_fees

  )
  ######### SMALL ORDER FEE -- The following CTE's get the data for SOF and aggregate it based on active_from.
  , get_small_order_fee AS (
      SELECT country_code
        , configuration_id AS mov_config_id
        , flexible AS is_small_order_fee
        , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from
        , STRUCT(
          hard_mov
          , max_top_up
        ) AS small_order_fee_config
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_mov_configuration` 
    WHERE deleted = FALSE 
    AND flexible
  )

  ######### MOV -- The following CTE's get the data for MOVs and aggregate it based on active_from.
  , get_mov_data AS (
    SELECT country_code
      , configuration_id AS mov_config_id
      , row_id AS mov_configuration_row_id
      , minimum_order_value
      , travel_time_threshold
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_mov_configuration_row`
    WHERE deleted = FALSE
  ) 

  , join_surge_mov AS (
    SELECT * EXCEPT(is_surge_mov)
      , IFNULL(is_surge_mov, FALSE) AS is_surge_mov
    FROM get_mov_data
    LEFT JOIN surge_final_fees USING (country_code, mov_configuration_row_id, active_from)
  )

  , aggregate_versioning_mov AS (
    SELECT country_code
      , mov_config_id
      , active_from
      , is_surge_mov
      , ARRAY_AGG(
          surge_mov_row_config_hash
      ) AS _surge_mov_row_config_hash
      , ARRAY_AGG(
          STRUCT(minimum_order_value
            , travel_time_threshold
            , surge_mov_row_config
          )
        ) AS mov_config
    FROM join_surge_mov
    GROUP BY 1, 2, 3, 4
  )

  , sort_configuration_mov AS (
    SELECT * EXCEPT(mov_config)
      , ARRAY(
          SELECT AS STRUCT
          travel_time_threshold
          , minimum_order_value
          , surge_mov_row_config
          from UNNEST(mov_config) 
          ORDER BY travel_time_threshold NULLS LAST
        ) AS mov_config
      FROM aggregate_versioning_mov
  )

  , join_small_order_fee AS (
    SELECT * EXCEPT(is_small_order_fee) 
      , IFNULL(is_small_order_fee, FALSE) AS is_small_order_fee
      , SHA256(
        CONCAT(IFNULL(CAST(small_order_fee_config.hard_mov AS STRING),"") 
        , IFNULL(CAST(small_order_fee_config.max_top_up AS STRING),"")
        )
      ) AS small_order_fee_config_hash --no need to sort, only one hard mov is allowed
    FROM sort_configuration_mov mov
    LEFT JOIN get_small_order_fee USING(country_code, mov_config_id, active_from)
  )

  , add_hashes_mov as (
    SELECT *
      , SHA256(
        CONCAT(
          ARRAY_TO_STRING( 
          ARRAY(SELECT CAST(travel_time_threshold AS STRING) FROM UNNEST(mov_config)) 
          , ""
          )
        , ARRAY_TO_STRING( 
          ARRAY (SELECT CAST(minimum_order_value AS STRING) FROM UNNEST(mov_config))
          , ""
          )
        )
      ) AS dbmov_config_hash
      , SHA256( 
        TO_BASE64(ARRAY_TO_STRING(_surge_mov_row_config_hash, CAST("" AS BYTES)))
      ) AS surge_mov_config_hash
    FROM join_small_order_fee
  )

  , add_component_hash_mov as (
    SELECT *
      , SHA256(
        CONCAT(
          TO_BASE64(dbmov_config_hash)
          , TO_BASE64(surge_mov_config_hash)
          , TO_BASE64(small_order_fee_config_hash)
        )
      ) AS mov_config_hash
    FROM add_hashes_mov
  )

  , get_past_version_mov AS (
    SELECT *
      , LAG(mov_config_hash, 1) OVER(tt_partition) AS prev_hash
    FROM add_component_hash_mov
    WINDOW tt_partition AS(PARTITION BY country_code, mov_config_id ORDER BY active_from)
  )

  , deduplicate_versions_mov as (
    SELECT *
      , LEAD(active_from)  OVER(PARTITION BY country_code, mov_config_id ORDER BY active_from) AS active_to --versioning without duplicates
    FROM get_past_version_mov
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN mov_config_hash = prev_hash THEN FALSE -- remove as long current is equal to previous
        ELSE TRUE
        END
    )
  )

  , mov_final_fees AS (
    SELECT country_code 
      , mov_config_id
      , active_from
      , IFNULL(active_to, "2099-01-01") AS active_to -- useful for later joins
      , ARRAY_LENGTH(mov_config) > 1 AS is_dbmov
      , is_surge_mov 
      , is_small_order_fee
      , CASE
            WHEN ARRAY_LENGTH(mov_config) > 1
              THEN "Variable"
            WHEN (SELECT MAX(tt.minimum_order_value) FROM UNNEST(mov_config) tt) > 0 -- using MAX() to avoid Scalar subquery error
              THEN "Flat_non_zero"
            ELSE "Flat_zero"
      END AS mov_type
      , mov_config_hash
      , dbmov_config_hash
      , IF(SHA256("") = surge_mov_config_hash, NULL, surge_mov_config_hash) AS surge_mov_config_hash
      , IF(SHA256("") = small_order_fee_config_hash, NULL, small_order_fee_config_hash) AS small_order_fee_config_hash
      , mov_config
      , small_order_fee_config
    FROM deduplicate_versions_mov
  )
  -- Travel Times -- The following CTE's get the data for Travel Times and aggregate it based on active_from.
  , get_travel_times AS (
    SELECT country_code
      , configuration_id AS travel_time_config_id
      , travel_time_fee
      , travel_time_threshold
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time.
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_travel_time_fee_configuration_row`
    WHERE deleted = FALSE
  )

  , aggregate_versioning_tt AS (
    SELECT country_code
      , travel_time_config_id
      , active_from
      , ARRAY_AGG(
          STRUCT(travel_time_fee
            ,travel_time_threshold
          )
      ) AS travel_time_config
    FROM get_travel_times
    GROUP BY 1, 2, 3
  )

  , sort_configuration_tt as (
    SELECT country_code
      , travel_time_config_id
      , active_from
      , ARRAY(
          SELECT AS STRUCT
          travel_time_threshold
          , travel_time_fee
          FROM UNNEST(travel_time_config) 
          ORDER BY travel_time_threshold NULLS LAST
      ) AS travel_time_config
    FROM aggregate_versioning_tt
  )

  , add_hash_tt AS (
    SELECT *
      , SHA256(
        CONCAT(
          ARRAY_TO_STRING( 
          ARRAY(SELECT CAST(travel_time_threshold AS STRING) FROM UNNEST(travel_time_config))
          , "")
        , ARRAY_TO_STRING( 
          ARRAY (SELECT CAST(travel_time_fee AS STRING) FROM UNNEST(travel_time_config))
          , "")
        )
      ) AS travel_time_config_hash
    FROM sort_configuration_tt
  )

  , get_past_version_tt AS (
    SELECT *
      , LAG(travel_time_config_hash, 1) OVER(tt_partition) AS prev_hash
    FROM add_hash_tt
    WINDOW tt_partition AS(PARTITION BY country_code, travel_time_config_id ORDER BY active_from)
  )

  , deduplicate_versions_tt AS (
    SELECT * 
      , LEAD(active_from)  OVER(PARTITION BY country_code, travel_time_config_id ORDER BY active_from) as active_to --versioning without duplicates
    FROM get_past_version_tt
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN ( travel_time_config_hash = prev_hash) THEN FALSE -- remove duplicates
        ELSE TRUE
        END
    )
  )

  , tt_final_fees AS (
    SELECT country_code 
      , travel_time_config_id
      , active_from
      , IFNULL(active_to, "2099-01-01") AS active_to
      , ARRAY_LENGTH(travel_time_config) > 1 AS is_dbdf
      , CASE
            WHEN ARRAY_LENGTH(travel_time_config) > 1
              THEN "Variable"
            WHEN (SELECT MAX(tt.travel_time_fee) from UNNEST(travel_time_config) tt) > 0 -- non-null means there are tiers
              THEN "Flat_non_zero"
            ELSE "Flat_zero"
      END AS travel_time_type
      , travel_time_config_hash
      , travel_time_config
    FROM deduplicate_versions_tt 
  )
  -- Fleet Delay -- The following CTE's get the data for Fleet Delays and aggregate it based on active_from.
  ,  get_fleet_delay AS (
    SELECT country_code
      , configuration_id as delay_config_id
      , travel_time_threshold
      , delay_threshold
      , delay_fee
      , TIMESTAMP_TRUNC(updated_at, MINUTE) as active_from
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_delay_fee_configuration_row`  
    WHERE deleted = FALSE
    -- using hl table as it contains all component history
    -- dl layer only has the last version. Is this expected?
    )

  , fleet_delay_configuration AS (
    SELECT country_code
      , delay_config_id
      , active_from
      , travel_time_threshold
      , ARRAY_AGG(delay_fee) AS _delay_fees
      , ARRAY_AGG(
        STRUCT(delay_threshold
          , delay_fee
        )
      ) AS delay_config
    FROM get_fleet_delay
    GROUP BY 1,2,3,4
    )

  , sort_fleet_delay_configuration AS (
    SELECT country_code
      , delay_config_id
      , active_from
      , travel_time_threshold
      , _delay_fees
      , ARRAY(
          SELECT AS STRUCT
          delay_threshold
          , delay_fee
          FROM UNNEST(delay_config) 
          ORDER BY delay_threshold NULLS LAST
      ) AS delay_config
    FROM fleet_delay_configuration
  )

  , whole_fleet_delay_configuration AS (
    SELECT country_code
      , delay_config_id
      , active_from
      , ARRAY_CONCAT_AGG(_delay_fees) AS delay_fees
      , ARRAY_AGG(
        SHA256(
          CONCAT(
              ARRAY_TO_STRING( 
              ARRAY(SELECT CAST(delay_threshold AS STRING) FROM UNNEST(delay_config))
              , "")
            , ARRAY_TO_STRING( 
              ARRAY (SELECT CAST(delay_fee AS STRING) FROM UNNEST(delay_config))
              , "")
            )
        )
      ) AS _fleet_delay_config_hash
      , ARRAY_AGG(
        STRUCT(
          travel_time_threshold
          , delay_config
        )
      ) AS fleet_delay_config
    FROM fleet_delay_configuration
    GROUP BY 1,2,3
  )

  , sort_whole_fleet_delay_configuration AS (
    SELECT country_code
      , delay_config_id
      , active_from
      , delay_fees
      , _fleet_delay_config_hash
      , ARRAY(
        SELECT AS STRUCT
        travel_time_threshold
        , delay_config
        FROM UNNEST(fleet_delay_config)
        ORDER BY travel_time_threshold NULLS LAST
      ) AS fleet_delay_config
    FROM whole_fleet_delay_configuration

  )

  , fleet_delay_add_hash AS (
    SELECT *
      , SHA256(
        CONCAT(
          ARRAY_TO_STRING(
            ARRAY(SELECT CAST(travel_time_threshold AS STRING) FROM UNNEST(fleet_delay_config))
          , "")
          , TO_BASE64(ARRAY_TO_STRING(_fleet_delay_config_hash, CAST("" AS BYTES))) 
        )
      ) AS fleet_delay_config_hash
    FROM sort_whole_fleet_delay_configuration
  )

  , fleet_delay_past_version AS (
    SELECT *
      , LAG(fleet_delay_config_hash) OVER(fleet_delay_partition) AS prev_hash
    FROM fleet_delay_add_hash
    WINDOW fleet_delay_partition AS(PARTITION BY country_code, delay_config_id ORDER BY active_from)
  )

  , fleet_delay_deduplicate_versions AS (
    SELECT *
      , LEAD(active_from) OVER(PARTITION BY country_code, delay_config_id ORDER BY active_from) as active_to
      , ARRAY(SELECT DISTINCT x FROM UNNEST(delay_fees) x WHERE x <> 0) AS unique_delay_fees
    FROM fleet_delay_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN ( fleet_delay_config_hash = prev_hash) THEN FALSE -- remove duplicates
        ELSE TRUE
        END
    )
  )

  , fleet_delay_final_fees AS (
    SELECT country_code 
      , delay_config_id
      , active_from
      , IFNULL(active_to, "2099-01-01") AS active_to
      , ARRAY_LENGTH(unique_delay_fees) > 0 AS is_fleet_delay  -- If there is one or more non-zero delay fee, it means that there is an active surgeconfiguration.
      , fleet_delay_config_hash
      , fleet_delay_config
    FROM fleet_delay_deduplicate_versions 
  )
  -- Service Fee -- The following CTE's get the data for Service Fee and aggregate it based on active_from.
  , get_service_fee AS (
    SELECT country_code
      , configuration_id AS service_fee_config_id
      , service_fee_type
      , service_fee
      , min_service_fee
      , max_service_fee
      , deleted
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_service_fee_configuration`
  WHERE deleted = FALSE
  )

  , aggregate_versioning_sf AS (
    -- SF components don't have a need for aggregation
    SELECT country_code
      , service_fee_config_id
      , service_fee_type
      , active_from
      , SHA256(
          CONCAT(
          service_fee_type
          , service_fee
          , IFNULL(min_service_fee, 0) -- just to have a value to avoid a null hash
          , IFNULL(max_service_fee, 0) -- just to have a value to avoid a null hash
          )
      ) AS service_fee_config_hash
      , STRUCT(service_fee
            , min_service_fee
            , max_service_fee
      ) AS service_fee_config
    FROM get_service_fee
  )

  , get_next_and_past_version_sf AS (
    SELECT *
      , LAG(service_fee_config_hash) OVER(sf_versioning) as prev_hash
    FROM aggregate_versioning_sf
    WINDOW sf_versioning AS(PARTITION BY country_code, service_fee_config_id ORDER BY active_from)
  )

  , deduplicate_versions_sf AS (
    SELECT *
      , LEAD(active_from, 1) OVER(sf_versioning) AS active_to 
    FROM get_next_and_past_version_sf
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN ( service_fee_config_hash = prev_hash) THEN FALSE -- remove duplicates
        ELSE TRUE
        END
    )
    WINDOW sf_versioning AS(PARTITION BY country_code, service_fee_config_id ORDER BY active_from)
  )

  , service_fee_final_fees AS (
    SELECT country_code
    , service_fee_config_id
    , active_from
    , IFNULL(active_to, "2099-01-01") AS active_to
    , service_fee_type
    , service_fee_config_hash
    , service_fee_config 
    FROM deduplicate_versions_sf
  )
  -- Basket Value Deals -- The following CTE's get the data for Basket Value deals and aggregate it based on active_from.
  , get_bv_data AS (
    SELECT country_code
      , configuration_id AS basket_value_config_id
      , basket_value_fee
      , basket_value_threshold
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from  -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time
    FROM `fulfillment-dwh-production.hl.dynamic_pricing_basket_value_fee_configuration_row`
    WHERE deleted = FALSE
  )

  , bv_aggregate_versioning AS (
    SELECT country_code
      , basket_value_config_id
      , active_from
      , ARRAY_AGG(
          STRUCT(basket_value_fee
            , basket_value_threshold
          )
      ) AS basket_value_config
    FROM get_bv_data
    GROUP BY 1, 2, 3
  )

  , bv_sort_configuration AS (
    SELECT country_code
      , basket_value_config_id
      , active_from
      , ARRAY(
          SELECT AS STRUCT
          basket_value_threshold
          , basket_value_fee
          from UNNEST(basket_value_config) 
          ORDER BY basket_value_threshold NULLS LAST
      ) AS basket_value_config
    FROM bv_aggregate_versioning
  )


  , bv_add_hash as (
    SELECT *
    , SHA256(
      CONCAT(
        ARRAY_TO_STRING( 
        ARRAY(SELECT CAST(basket_value_threshold AS STRING) FROM UNNEST(basket_value_config))
        , "")
      , ARRAY_TO_STRING( 
        ARRAY (SELECT CAST(basket_value_fee AS STRING) FROM UNNEST(basket_value_config))
        , "")
      )
    ) AS basket_value_config_hash
    FROM bv_sort_configuration
  )

  , bv_get_next_and_past_version AS (
    SELECT *
      , LAG(basket_value_config_hash, 1) OVER(bv_partition) AS prev_hash
    FROM bv_add_hash
    WINDOW bv_partition AS(PARTITION BY country_code, basket_value_config_id ORDER BY active_from)
  )

  , bv_deduplicate_versions AS (
    SELECT *
      , LEAD(active_from)  OVER(PARTITION BY country_code, basket_value_config_id ORDER BY active_from) AS active_to --versioning without duplicates
    FROM bv_get_next_and_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN ( basket_value_config_hash = prev_hash) THEN FALSE -- remove duplicates
        ELSE TRUE
        END
    )
  )

  , basket_value_final_fees AS (
    SELECT country_code 
    , basket_value_config_id
    , active_from
    , IFNULL(active_to, "2099-01-01") AS active_to
    , basket_value_config_hash
    , basket_value_config
  FROM bv_deduplicate_versions
  )
  -- Scheme History -- This CTE gets all the scheme_ids with its corresponding configuration/component ids (such as travel time config id, mov config id, etc) and the dates when the schemes were active.
  , scheme_history AS (
    SELECT entity_id
      , country_code
      , scheme_id
      , psh.scheme_name
      , DATETIME_TRUNC(psh.active_from, MINUTE) AS scheme_active_from
      , IFNULL(DATETIME_TRUNC(psh.active_to, MINUTE), "2099-01-01") AS scheme_active_to
      , psh.travel_time_fee_configuration_id AS travel_time_config_id -- Travel Time is a mandatory component.
      , psh.mov_configuration_id AS mov_config_id -- MOV is a mandatory component. It can be Flat, Distance Based, Delay Fee based or both. When it is Delay Fee based it has a Surge MOV component which is an optional component.
      , psh.delay_fee_configuration_id AS delay_config_id -- Delay Fee is a non-mandatory component.
      , psh.basket_value_fee_configuration_id AS basket_value_config_id -- Basket Value Fee is a non-mandatory component.
      , psh.service_fee_configuration_id AS service_fee_config_id -- Service Fee is a non-mandatory component.
    FROM `fulfillment-dwh-production.cl._dynamic_pricing_price_scheme_versions`
    LEFT JOIN UNNEST (price_scheme_history) psh
  )
  --  The next CTEs JOIN together all components based on configuration ids
  , add_mov_config AS (
    SELECT * EXCEPT (scheme_active_from, scheme_active_to, active_from, active_to)
      , CASE
          WHEN mov_config_id IS NULL
            THEN scheme_active_from
          ELSE GREATEST(scheme_active_from, active_from) -- last active_from
        END AS scheme_active_from
      , CASE
          WHEN mov_config_id IS NULL
            THEN scheme_active_to
          ELSE LEAST(scheme_active_to, active_to) -- first active_to
        END AS scheme_active_to
    FROM scheme_history
    LEFT JOIN mov_final_fees USING (country_code, mov_config_id)
  --The component needs to be active within the timeframe of the scheme.
    WHERE ((mov_config_id IS NULL)
    OR (scheme_active_from < active_to  -- For cases when scheme was activated while component was already active.
      AND scheme_active_to > active_from))  -- For cases where component is activated while scheme is still active.
  ) 

  , add_travel_time_config AS (
    SELECT * EXCEPT(scheme_active_from, scheme_active_to, active_from, active_to)
      , CASE
          WHEN travel_time_config_id IS NULL
            THEN scheme_active_from
          ELSE GREATEST(scheme_active_from, active_from) -- last active_from
        END AS scheme_active_from
      , CASE
          WHEN travel_time_config_id IS NULL
            THEN scheme_active_to
          ELSE LEAST(scheme_active_to, active_to) -- first active_to
        END AS scheme_active_to
    FROM add_mov_config
    LEFT JOIN tt_final_fees USING (country_code, travel_time_config_id)
  --The component needs to be active within the timeframe of the scheme.
    WHERE ((travel_time_config_id IS NULL) 
    OR (scheme_active_from < active_to  -- For cases when scheme was activated while component was already active.
      AND scheme_active_to > active_from))  -- For cases where component is activated while scheme is still active.
  )

  , add_fleet_delay_config AS (
    SELECT * EXCEPT(scheme_active_from, scheme_active_to, active_from, active_to)
  -- As fleet delay is a non-mandatory component, when it is NULL we dont create a new line and version of the scheme, but when it exists we use the same logic as in the mandatory components.
      , CASE
          WHEN delay_config_id IS NULL
            THEN scheme_active_from
          ELSE GREATEST(scheme_active_from, active_from) -- last active_from
        END AS scheme_active_from
      , CASE
          WHEN delay_config_id IS NULL
            THEN scheme_active_to
          ELSE LEAST(scheme_active_to, active_to) -- first active_to
        END AS scheme_active_to
    FROM add_travel_time_config
    LEFT JOIN fleet_delay_final_fees USING (country_code, delay_config_id)
  --The component needs to be active within the timeframe of the scheme.
    WHERE ((delay_config_id IS NULL) -- When it is in fact NULL, the JOIN doesnt generate a new version (line). This applies only for Non-mandatory components.
      OR (scheme_active_from < active_to -- For cases when scheme was activated while component was already active.
      AND scheme_active_to > active_from)) -- For cases where component is activated while scheme is still active.
  )

  , add_service_fee AS (
    SELECT * EXCEPT(scheme_active_from, scheme_active_to, active_from, active_to)
  -- As service fee is a non-mandatory component, when it is NULL we dont create a new line and version of the scheme, but when it exists we use the same logic as in the mandatory components.
      , CASE
          WHEN service_fee_config_id IS NULL
            THEN scheme_active_from
          ELSE GREATEST(scheme_active_from, active_from) -- last active_from
        END AS scheme_active_from
      , CASE
          WHEN service_fee_config_id IS NULL
            THEN scheme_active_to
          ELSE LEAST(scheme_active_to, active_to) -- first active_to
        END AS scheme_active_to
    FROM add_fleet_delay_config
    LEFT JOIN service_fee_final_fees USING (country_code, service_fee_config_id)
  --The component needs to be active within the timeframe of the scheme.
    WHERE ((service_fee_config_id IS NULL) -- When it is in fact NULL, the JOIN doesnt generate a new version (line). This applies only for Non-mandatory components.
      OR (scheme_active_from < active_to -- For cases when scheme was activated while component was already active.
      AND scheme_active_to > active_from)) -- For cases where component is activated while scheme is still active.
  )

  , add_basket_value_deal AS (
    SELECT * EXCEPT(scheme_active_from, scheme_active_to, active_from, active_to)
  -- As basket value is a non-mandatory component, when it is NULL we dont create a new line and version of the scheme, but when it exists we use the same logic as in the mandatory components.
      , CASE
          WHEN basket_value_config_id IS NULL
            THEN scheme_active_from
          ELSE GREATEST(scheme_active_from, active_from) -- last active_from
        END as scheme_active_from
      , CASE
          WHEN basket_value_config_id IS NULL
            THEN scheme_active_to
          ELSE LEAST(scheme_active_to, active_to) -- first active_to
        END AS scheme_active_to
    FROM add_service_fee
    LEFT JOIN basket_value_final_fees USING (country_code, basket_value_config_id)
  --The component needs to be active within the timeframe of the scheme.
    WHERE ((basket_value_config_id IS NULL) -- When it is in fact NULL, the JOIN doesnt generate a new version (line). This applies only for Non-mandatory components.
      OR (scheme_active_from < active_to -- For cases when scheme was activated while component was already active.
      AND scheme_active_to > active_from)) -- For cases where component is activated while scheme is still active.
  )

  , scheme_whole_config AS (
    SELECT entity_id
      , country_code
      , scheme_id
      , scheme_name
      , scheme_active_from
      , CASE WHEN scheme_active_to = "2099-01-01" THEN NULL ELSE scheme_active_to END AS scheme_active_to
      , SHA256(
        CONCAT(
          scheme_name
          , IFNULL(TO_BASE64(travel_time_config_hash),"")
          , IFNULL(TO_BASE64(mov_config_hash),"")
          , IFNULL(TO_BASE64(fleet_delay_config_hash),"")
          , IFNULL(TO_BASE64(basket_value_config_hash),"")
          , IFNULL(TO_BASE64(service_fee_config_hash),"")
        )
      ) AS scheme_config_hash
      , STRUCT(IFNULL(is_dbdf, FALSE) as is_dbdf
          , IFNULL(is_dbmov, FALSE) as is_dbmov
          , IFNULL(is_surge_mov, FALSE) AS is_surge_mov
          , IFNULL(is_small_order_fee, FALSE) as is_small_order_fee
          , IFNULL(is_fleet_delay, FALSE) AS is_fleet_delay
          , basket_value_config_id IS NOT NULL AS is_basket_value_deal
          , service_fee_config_id IS NOT NULL AS is_service_fee
          , mov_type
          , travel_time_type
          , service_fee_type
      ) AS scheme_price_mechanisms
      , STRUCT(travel_time_config_id
          , mov_config_id
          , delay_config_id
          , basket_value_config_id
          , service_fee_config_id
      ) AS scheme_component_ids
      , STRUCT(travel_time_config_hash
          , mov_config_hash
          , dbmov_config_hash
          , surge_mov_config_hash
          , small_order_fee_config_hash
          , fleet_delay_config_hash
          , basket_value_config_hash
          , service_fee_config_hash
      ) AS scheme_component_hashes
      , STRUCT(travel_time_config
            , mov_config
            , small_order_fee_config
            , fleet_delay_config
            , service_fee_config
            , basket_value_config
      ) AS scheme_component_configs
    FROM add_basket_value_deal
  )

  , scheme_get_next_and_past_version AS (
    SELECT *
      , LAG(scheme_config_hash) OVER(scheme_partition) AS prev_hash
    FROM scheme_whole_config
    WINDOW scheme_partition AS(PARTITION BY country_code, scheme_id ORDER BY scheme_active_from)
  )

  , scheme_deduplicate_versions AS (
    SELECT *
    FROM scheme_get_next_and_past_version
    WHERE (
      CASE 
        WHEN prev_hash IS NULL THEN TRUE --keep first version
        WHEN ( scheme_config_hash = prev_hash) THEN FALSE -- remove duplicates
        ELSE TRUE
        END
    )
    -- filter duplicates that occurs when we have the final version of a hash and the first hash version of another
  )

  , set_active_to AS (
    SELECT 
    entity_id
        , country_code
        , scheme_id
        , scheme_name
        , scheme_active_from
        , LEAD(scheme_active_from)  OVER(PARTITION BY country_code, scheme_id ORDER BY scheme_active_from) AS scheme_active_to --versioning without duplicates
        , scheme_config_hash
        , scheme_price_mechanisms
        , scheme_component_ids
        , scheme_component_hashes
        , scheme_component_configs
    FROM scheme_deduplicate_versions
  )

  SELECT *
  FROM set_active_to;

