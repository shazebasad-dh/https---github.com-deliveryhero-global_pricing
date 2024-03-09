
WITH get_priority_fee AS (
  SELECT DISTINCT
    country_code
    , configuration_id AS priority_fee_config_id
    , percentage_fee
    , min_fee
    , max_fee
    , TIMESTAMP_TRUNC(updated_at, MINUTE) AS active_from -- Truncate updated_at field to be able to group fields from the same configuration that were updated at the same time
  -- FROM `{{ params.project_id }}.hl.dynamic_pricing_priority_fee_configuration`
  FROM `fulfillment-dwh-production.dl.dynamic_pricing_priority_fee_configuration`
  WHERE deleted = FALSE
-- Priority Fee Hash -- Hashing creates a unique identifier for the configuration version based on it's component values, in this case percentage_gee, max_fee and min_fee.
), aggregate_versioning_prio AS (
  SELECT
    country_code
    , priority_fee_config_id
    , active_from
    , SHA256(
      CONCAT(
        percentage_fee
        , IFNULL(min_fee, 0) -- just to have a value to avoid a null hash
        , IFNULL(max_fee, 0) -- just to have a value to avoid a null hash
      )
    ) AS priority_fee_config_hash
    , STRUCT(percentage_fee
      , min_fee
      , max_fee
    ) AS priority_fee_config
  FROM get_priority_fee
), get_next_and_past_version_prio AS (
  SELECT
    *
    , LAG(priority_fee_config_hash) OVER (prio_versioning) AS prev_hash
  FROM aggregate_versioning_prio
  WINDOW prio_versioning AS (PARTITION BY country_code, priority_fee_config_id ORDER BY active_from)-- WINDOW clause defines the frame of the sf versioning partition
), deduplicate_versions_prio AS (
  SELECT
    *
    , LEAD(active_from, 1) OVER (prio_versioning) AS active_to
  FROM get_next_and_past_version_prio
  WHERE (
      CASE
        WHEN prev_hash IS NULL THEN TRUE --we keep the first version
        WHEN (priority_fee_config_hash = prev_hash) THEN FALSE -- we remove version when the current one is equal to the previous one
        ELSE TRUE
      END
    )
  WINDOW prio_versioning AS(PARTITION BY country_code, priority_fee_config_id ORDER BY active_from) -- WINDOW clause defines the frame of the sf_versioning partition
), priority_fee_final_fees AS (
  SELECT
    country_code
    , priority_fee_config_id
    , active_from
    , IFNULL(active_to, '2099-01-01') AS active_to
    , priority_fee_config_hash
    , priority_fee_config
  FROM deduplicate_versions_prio
)

SELECT *
FROM priority_fee_final_fees
ORDER BY 1,2,3