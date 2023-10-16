CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_campaign_configuration_versions` AS
with load_campaigns as (
  SELECT
    campaign_id
  , name as campaign_name
  , global_entity_id as entity_id
  , country_code
  , price_scheme_id as scheme_id
  , customer_condition_id
  , schedule_id as time_condition_id
  , active
  , event = "DELETE" as deleted -- this to use the same ASA logic
  , TIMESTAMP_TRUNC(created_at, MINUTE) AS created_at
  , TIMESTAMP_TRUNC(updated_at, MINUTE) AS updated_at
  , ARRAY((SELECT x FROM UNNEST(vendor_ids) x ORDER BY x)) AS sorted_assigned_vendor_ids
  , ROW_NUMBER() OVER(PARTITION BY global_entity_id, campaign_id ORDER BY campaign_history_id) AS _row_number

  FROM `fulfillment-dwh-production.hl.dynamic_pricing_campaign_history` 
  -- where global_entity_id = "FP_SG"
  -- AND event <> "DELETE"
)

, set_active_from AS (
  SELECT
    * EXCEPT(updated_at)
    , IF(_row_number = 1, created_at, updated_at) AS active_from
  FROM load_campaigns
)

-- Generate Hash -- This CTE creates a hash per vendor group. If a new vendor is added, the hash changes as it is considered a new version.
, generate_vendor_hash AS (
  SELECT
    *
    , SHA256(ARRAY_TO_STRING(sorted_assigned_vendor_ids, '')) AS assigned_vendor_hash
    , ARRAY_LENGTH(sorted_assigned_vendor_ids) AS assigned_vendors_count
  FROM set_active_from
)

-- Generate Primary Key -- Create a primary key of the asa --> vendor hash(from previous CTE) + vendor_group_id + pirority + deleted
, generate_pk_version AS (
  SELECT
    *
    , SHA256(
      CONCAT(
        CAST(campaign_id AS STRING)
        , CAST(campaign_name AS STRING)
        , CAST(deleted AS STRING)
        , CAST(active AS STRING)
        , CAST(scheme_id AS STRING)
        , IFNULL(CAST(customer_condition_id AS STRING), "")
        , IFNULL(CAST(time_condition_id AS STRING), "")
        -- take from https://stackoverflow.com/questions/49660672/what-to-try-to-get-bigquery-to-cast-bytes-to-string
        , TO_BASE64(assigned_vendor_hash)
      )
    ) AS campaign_id_hash
  FROM generate_vendor_hash
)

, get_previous_version AS (
  SELECT
    *
    , LAG(campaign_id_hash) OVER (campaign_partition) AS previous_pk
  FROM generate_pk_version
  WINDOW campaign_partition AS (PARTITION BY entity_id, campaign_id ORDER BY active_from)
)

-- Deduplication -- When 2 consecutive rows have the same configurations, we need to delete de duplicate.
, deduplicate_versions AS (
  SELECT
    *
    , LEAD(active_from) OVER (PARTITION BY entity_id, campaign_id ORDER BY active_from) AS active_to
  FROM get_previous_version
  WHERE (
      CASE
        WHEN previous_pk IS NULL THEN TRUE --we keep the first version
        WHEN (campaign_id_hash = previous_pk) THEN FALSE -- we remove version when the current one is equal to the previous one
        ELSE TRUE
      END
    )

)

, load_schedule_config AS (
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
    ) AS schedule_config
  FROM `fulfillment-dwh-production.cl._dynamic_pricing_schedule_versions`
  LEFT JOIN UNNEST(schedule_config_history)
  WHERE active_to IS NULL
)

, load_customer_condition_config AS (
    SELECT 
    country_code
    , customer_condition_id
    , STRUCT(description
      , orders_number_less_than
      , days_since_first_order_less_than
      , counting_method
    ) AS customer_condition_config
  FROM `fulfillment-dwh-production.cl._dynamic_pricing_customer_condition_versions`
  LEFT JOIN UNNEST(customer_condition_config_history)
  WHERE active_to IS NULL
)

, final_table as (
SELECT
  entity_id
  , dv.country_code
  , campaign_id
  , campaign_name
  , created_at
  , active_from
  , active_to
  , active
  , campaign_id_hash
  , scheme_id
  , dv.customer_condition_id
  , customer_condition_config
  , time_condition_id
  , schedule_config
  , assigned_vendor_hash
  , assigned_vendors_count
  , sorted_assigned_vendor_ids
FROM deduplicate_versions dv
LEFT JOIN load_schedule_config  ls
  ON dv.country_code = ls.country_code
  AND dv.time_condition_id = ls.schedule_id
LEFT JOIN load_customer_condition_config ccc
    ON dv.country_code = ccc.country_code
   AND dv.customer_condition_id = ccc.customer_condition_id 
WHERE deleted = FALSE
AND ( (active_to IS NULL)
OR ( TIMESTAMP_DIFF(active_to, active_from, MINUTE) > 1)
)


)

SELECT *
FROM final_table;