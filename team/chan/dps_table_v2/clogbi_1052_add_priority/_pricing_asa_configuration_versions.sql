----------------------------------------------------------------------------------------------------------------------------
--                NAME: _pricing_asa_configuration_versions.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: 2023-03-20
--         DESCRIPTION: This table contains all historical information about ASA price scheme configurations, on version level.
--        QUERY OUTPUT: Every version of every pricing configuration within an ASA can be obtained.
--               NOTES: ASAs can contain multiple schemes.
--             UPDATED:
--                      2023-06-12 | Elena Fedotova     | CLOGBI-745    | Add counting_method to customer_condition_config
--                      2023-10-31 | David Gallo        | CLOGBI-1164   | Update n_schemes to count distinct asa_price_config_ids instead of distinct schemes
--                      2023-11-01 | David Gallo        | CLOGBI-1171   | Added condition_end_at to time condition struct, which is the effective end timestamp of the condition
--                      2024-02-01 | Oren Blumenfeld    | RASD-4934     | Add SAFE to ST_GEOGFROMTEXT to avoid error
--                      2024-02-21 | Sebastian Lafaurie | CLOGBI-1052   | Add priority fee
--                      ----------------------------------

CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._pricing_asa_configuration_versions`
CLUSTER BY country_code, entity_id, asa_id
AS
-- Load ASA -- Get the ASA ids and vendor_group_ids for the base of the table.
WITH load_asa AS (
  SELECT
    country_code
    , region
    , global_entity_id AS entity_id
    , vendor_group_price_config_id AS asa_price_config_id
    , vendor_group_assignment_id AS asa_id
    , priority
    , variant
    , price_scheme_id AS scheme_id
    , customer_condition_id
    , schedule_id
    , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- makes it easier for later joins.
  FROM `{{ params.project_id }}.hl.dynamic_pricing_vendor_group_price_config`
  WHERE deleted = FALSE
    AND (variant IS NULL OR variant = 'Original') -- with this condition we remove experiments.
-- Set active to -- For each version of an asa, we set as active_to as the next consecutive active_from ordered by timestamp. If it is NULL we assign '2099-01-01'
), set_active_to AS (
  SELECT DISTINCT
    entity_id
    , asa_id
    , active_from
    , IFNULL(LEAD(active_from) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from), '2099-01-01') AS active_to
  FROM load_asa
-- Add active_to
), add_active_to_asa AS (
  SELECT *
  FROM load_asa
  LEFT JOIN set_active_to USING (entity_id, asa_id, active_from)
-- Load Scheme Configuration -- This CTE gets all the scheme_ids with its corresponding configuration/component ids (such as travel time config id, mov config id, etc) and the dates when the schemes were active.
), load_scheme_config AS (
  SELECT
    entity_id
    , scheme_id
    , scheme_name
    , scheme_active_from AS scheme_active_from
    , IFNULL(scheme_active_to, '2099-01-01') AS scheme_active_to -- remove NULLs so it can be used in window functions later.
    , scheme_config_hash
    , scheme_price_mechanisms
    , scheme_component_ids
    , scheme_component_hashes
    , scheme_component_configs
  FROM `{{ params.project_id }}.cl.pricing_configuration_versions`
  -- We omit versions with 1 minute lifetime or less, assuming they were a human error setup.
  WHERE TIMESTAMP_DIFF(scheme_active_to, scheme_active_from, MINUTE) > 1
    OR scheme_active_to IS NULL
-- Add Scheme Config -- Join ASA configuration to scheme configuration.
), add_scheme_config AS (
  SELECT
    asa.* EXCEPT(active_from, active_to)
    , scheme.* EXCEPT(scheme_active_from, scheme_active_to, scheme_id, entity_id)
    , active_from AS initial_asa_versioning -- maintain the original asa timestamp version
    , GREATEST(scheme_active_from, active_from) AS active_from -- create new window from the schemes that overlap. last active_from
    , LEAST(scheme_active_to, active_to) AS active_to -- first active_to
  FROM add_active_to_asa asa
  LEFT JOIN load_scheme_config scheme ON asa.entity_id = scheme.entity_id
    AND asa.scheme_id = scheme.scheme_id
  WHERE TRUE -- keep only schemes and asa that overlap in time, otherwise version wouldn't be active.
    AND scheme_active_from < active_to -- For cases when scheme was activated while asa was already active.
    AND scheme_active_to > active_from -- For cases where component is activated while scheme is still active.
-- Aggregate by version -- get a list off all the timestamp that are present within each asa original timestamp
), agg_by_asa_version AS (
  SELECT
    entity_id
    , asa_id
    , initial_asa_versioning -- the original asa timestamp only takes into account changes of the ASA price config
    , ARRAY_AGG(active_from) AS active_from_agg
    , ARRAY_AGG(active_to) AS active_to_agg
  FROM add_scheme_config
  GROUP BY 1, 2, 3
-- Get Ordered Timestamps -- Sort timestamps so that instead of having an active from and an active to, we just have one column with all of them ordered
-- The point of this is that each active_to should be the active_from of the next consecutive version of an asa.
), get_ordered_timestamps AS (
  SELECT
    entity_id
    , asa_id
    , initial_asa_versioning
    , ARRAY(
      SELECT DISTINCT DATETIME_TRUNC(x, MINUTE) AS x
      FROM UNNEST(ARRAY_CONCAT(active_from_agg, active_to_agg)) x ORDER BY x
    ) AS ordered_timestamps
  FROM agg_by_asa_version
-- Add New active_to -- Each active_to is the active_from of the next consecutive version of an asa.
), add_new_active_to AS (
  SELECT
    * EXCEPT(ordered_timestamps)
    , LEAD(new_active_from, 1) OVER (PARTITION BY entity_id, asa_id, initial_asa_versioning ORDER BY new_active_from) AS new_active_to
  FROM get_ordered_timestamps
  LEFT JOIN UNNEST(ordered_timestamps) new_active_from
-- ASA New Versioning -- CTE to clean.
), asa_new_versioning AS (
  SELECT *
  FROM add_new_active_to
  WHERE TRUE
    -- remove unnecessary last row that is generated with new_active_from = '2099-01-01'
    AND new_active_to IS NOT NULL
    AND new_active_from < new_active_to
-- Add Asa Configuration -- We combine the versioning CTE with the original configuration of the ASA.
), add_asa_config_at_the_time AS (
  SELECT
    aaa.region
    , aaa.country_code
    , anv.entity_id
    , anv.asa_id
    , anv.new_active_from
    , anv.new_active_to
    , aaa.asa_price_config_id
    , aaa.priority
    , aaa.scheme_id
    , aaa.schedule_id
    , aaa.customer_condition_id
  FROM asa_new_versioning anv
  LEFT JOIN add_active_to_asa aaa USING (entity_id, asa_id)
  WHERE TRUE
    -- Bring THE asa version within the new windows
    AND aaa.active_from < anv.new_active_to
    AND aaa.active_to > anv.new_active_from
-- Load Schedule Configurations
), load_schedule_config AS (
  SELECT
    country_code
    , schedule_id
    , STRUCT(start_at
      , end_at
      , timezone
      , recurrence
      , active_days
      , is_all_day
      , recurrence_end_at
      , IF(recurrence = "NONE", end_at, recurrence_end_at) AS condition_end_at
    ) AS schedule_config
  FROM `{{ params.project_id }}.cl._dynamic_pricing_schedule_versions`
  LEFT JOIN UNNEST(schedule_config_history)
  WHERE active_to IS NULL -- When an ASA is updated, it generates a duplicate row of the old configuration with deleted = TRUE. We add this condition to filter out those duplicates.
-- Add Schedule Configuration -- Add the schedule configuration to our current asa versioning CTE
), add_schedule_config AS (
  SELECT
    asa.*
    , sch.schedule_config
  FROM add_asa_config_at_the_time asa
  LEFT JOIN load_schedule_config sch ON asa.country_code = sch.country_code
    AND asa.schedule_id = sch.schedule_id
-- Load Customer Condition Configuration
), load_customer_condition_config AS (
  SELECT
    country_code
    , customer_condition_id
    , STRUCT(description
      , orders_number_less_than
      , days_since_first_order_less_than
      , counting_method
    ) AS customer_condition_config
  FROM `{{ params.project_id }}.cl._dynamic_pricing_customer_condition_versions`
  LEFT JOIN UNNEST(customer_condition_config_history)
  WHERE active_to IS NULL -- although it might seem like we need to join conditions based in the active_to/active_from, in practice, when a condition is updated in DPS, it actually generates a new condition ID. So we just get the most recent "version" of the condition ID (there's generally only one).
-- Add Customer Condition Configuration -- Add the customer condition configuration to our current asa versioning CTE
), add_customer_condition_config AS (
  SELECT
    asa.*
    , ccc.customer_condition_config
  FROM add_schedule_config asa
  LEFT JOIN load_customer_condition_config ccc ON asa.country_code = ccc.country_code
    AND asa.customer_condition_id = ccc.customer_condition_id
-- Load Area Shapes
), load_area_shapes AS (
  SELECT
    country_code
    , area_id
    , city_id
    , name
    , SAFE.ST_GEOGFROMTEXT(polygon_wkt) AS polygon
  FROM `{{ params.project_id }}.hl.dynamic_pricing_customer_area`
  WHERE deleted = FALSE
-- Load Customer Areas Configuration
), load_customer_area AS (
  SELECT
    area.country_code
    , vendor_group_price_config_id
    , ARRAY_AGG(
      STRUCT(
        area.area_id
        , name
        , city_id
        , polygon
      )
      ORDER BY area.area_id
    ) AS area_configs
  FROM `{{ params.project_id }}.hl.dynamic_pricing_vendor_group_price_config_customer_area` area
  LEFT JOIN load_area_shapes gts ON area.country_code = gts.country_code
    AND area.area_id = gts.area_id
  WHERE deleted = FALSE
  GROUP BY 1, 2
-- Add Customer Areas Configuration -- Add the customer areas configuration to our current asa versioning CTE
), add_customer_area AS (
  SELECT
    asa.*
    , gca.area_configs
  FROM add_customer_condition_config asa
  LEFT JOIN load_customer_area gca
    ON asa.country_code = gca.country_code
      AND asa.asa_price_config_id = gca.vendor_group_price_config_id
-- Add Scheme Configuration -- Add the scheme configuration to our current asa versioning CTE
), add_scheme_config_at_the_time AS (
  SELECT
    asa.*
    , scheme.* EXCEPT(scheme_active_from, scheme_active_to, scheme_id, entity_id)
  FROM add_customer_area asa
  LEFT JOIN load_scheme_config scheme ON asa.entity_id = scheme.entity_id
    AND asa.scheme_id = scheme.scheme_id
  WHERE TRUE
    --- Bring THE scheme version within the new windows
    AND scheme_active_from < new_active_to -- For cases when scheme was activated while asa was already active.
    AND scheme_active_to > new_active_from -- For cases where asa was activated while scheme is already active.
-- Clean Schemes -- Organizing the data as we will want it to appear in the final table.
), clean_schemes AS (
  SELECT
    region
    , country_code
    , entity_id
    , asa_id
    , new_active_from AS active_from
    , IF(new_active_to = '2099-01-01', NULL, new_active_to) AS active_to
    , COUNT(DISTINCT asa_price_config_id) AS n_schemes
    , COUNT(DISTINCT CASE WHEN schedule_id IS NOT NULL THEN asa_price_config_id END) AS n_schemes_with_time_condition
    , COUNT(DISTINCT CASE WHEN customer_condition_id IS NOT NULL THEN asa_price_config_id END) AS n_schemes_with_customer_condition
    , COUNT(DISTINCT CASE WHEN area_configs IS NOT NULL THEN asa_price_config_id END) AS n_schemes_with_customer_area
    , ARRAY_AGG(
      CONCAT(asa_price_config_id, TO_BASE64(scheme_config_hash))
      ORDER BY asa_price_config_id
    ) AS _asa_price_config_hash
    , ARRAY_AGG(
      STRUCT(
        asa_price_config_id
        , scheme_id
        , scheme_config_hash
        , priority
        , CASE
          WHEN customer_condition_id IS NULL
            AND schedule_id IS NULL
            AND area_configs IS NULL
            THEN TRUE
          ELSE FALSE
        END AS is_default_scheme
        , customer_condition_id
        , customer_condition_config
        , schedule_id
        , schedule_config
        , ARRAY_LENGTH(area_configs) AS n_areas
        , area_configs
        , scheme_price_mechanisms
        , scheme_component_ids
        , scheme_component_hashes
        , scheme_component_configs
      )
      ORDER BY priority
    )
    AS asa_price_config
  FROM add_scheme_config_at_the_time
  GROUP BY 1, 2, 3, 4, 5, 6
-- Add hash -- Hashing creates a unique identifier for the asa price configuration version based in this case on _asa_price_config_hash (asa_price_config_id and scheme_config_hash).
), add_hash AS (
  SELECT
    *
    , SHA256(
      ARRAY_TO_STRING(_asa_price_config_hash, '')
    ) AS asa_price_config_hash
  FROM clean_schemes
-- Get Previous Hash -- This will be necessary in order to deduplicate when 2 consecutive rows have the same configuration.
), get_previous_hash AS (
  SELECT
    *
    , LAG(asa_price_config_hash) OVER(asa_price_partition) AS prev_hash
  FROM add_hash
  WINDOW asa_price_partition AS (PARTITION BY entity_id, asa_id ORDER BY active_from)
-- Deduplication -- When 2 consecutive rows have the same configurations, we need to delete de duplicate.
), deduplicate_versions AS (
  SELECT *
  FROM get_previous_hash
  WHERE (
    CASE
      WHEN prev_hash IS NULL THEN TRUE --we keep the first version
      WHEN asa_price_config_hash = prev_hash THEN FALSE -- we remove version when the current one is equal to the previous one
      ELSE TRUE
    END
  )
)
SELECT
  region
  , country_code
  , entity_id
  , asa_id
  , active_from
  , LEAD(active_from) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS active_to
  , n_schemes
  , STRUCT(
    IF(n_schemes_with_time_condition > 0, TRUE, FALSE) AS asa_has_time_condition
    , IF(n_schemes_with_customer_condition > 0, TRUE, FALSE) AS asa_has_customer_condition
    , IF(n_schemes_with_customer_area > 0, TRUE, FALSE) AS asa_has_customer_area
  ) AS asa_condition_mechanisms
  , asa_price_config_hash
  , asa_price_config
FROM deduplicate_versions