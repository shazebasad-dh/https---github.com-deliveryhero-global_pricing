----------------------------------------------------------------------------------------------------------------------------
--                NAME: XXXXX.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Fatima Rodriguez
--       CREATION DATE: XXXXX
--         DESCRIPTION: This table contains all historical information about DPS ASA vendor assignment, on version level.
--
--        QUERY OUTPUT: Every version of every vendor assigned to an ASA can be obtained.
--               NOTES: XXXXX
--                      ----------------------------------

CREATE TEMP FUNCTION parse_vendor_filter(json STRING)
RETURNS ARRAY<
  STRUCT<
    key STRING,
    clause STRING,
    value ARRAY<STRING>
  >
>
LANGUAGE js AS """
  const filterRaw = JSON.parse(json);
  if (filterRaw) {
    const criteria = filterRaw['criteria'];
    Object.keys(criteria).forEach(k => {
        criteria[k]['key'] = k;
        values = criteria[k]['value']
        if (!Array.isArray(values)) {
            criteria[k]['value'] = [values]
        }
    });
    return Object.values(criteria);
  } else {
      return [];
  }
"""
;


############################## DPS ASA VENDOR CONFIG ##############################

  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_asa_vendor_assignments` 
  CLUSTER BY entity_id, asa_id
  AS
  WITH load_asa_vendor AS (
    SELECT 
      global_entity_id AS entity_id
      , country_code
      , vendor_group_assignment_id AS asa_id
      , vendor_group_id
      , name AS asa_name
      , TIMESTAMP_TRUNC(created_at, MINUTE) AS created_at
      , TIMESTAMP_TRUNC(updated_at, MINUTE) AS updated_at
      , ROW_NUMBER() OVER(PARTITION BY global_entity_id, vendor_group_assignment_id ORDER BY updated_at) AS _row_number
      , deleted
      , priority
      , ARRAY((SELECT x FROM UNNEST(assigned_vendor_ids) x ORDER BY x)) AS sorted_assigned_vendor_ids
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group_assignment` 
  WHERE ( (type = "BASIC") OR (type IS NULL) )
  )
  ---- SET CORRECT UPDATED_AT
  , set_active_from AS (
  SELECT * EXCEPT(created_at, updated_at)
    , IF(_row_number = 1, created_at, updated_at) AS active_from
  from load_asa_vendor
  )
  ----- GENERATE HASH AND COMPOSITE PK
  , generate_vendor_hash AS (
    SELECT *
      -- create a hash to use later to keep info if a new vendor was added during a specific version
      , SHA256(ARRAY_TO_STRING(sorted_assigned_vendor_ids, "")) AS assigned_vendor_hash
      , ARRAY_LENGTH(sorted_assigned_vendor_ids) as assigned_vendors_count
    FROM set_active_from
  )

  , generate_pk_version AS (
    SELECT *
    -- create a PK to 
    , SHA256(
      CONCAT(
        CAST(vendor_group_id AS STRING)
        , CAST(priority AS STRING)
        , CAST(deleted AS STRING)
        -- take from https://stackoverflow.com/questions/49660672/what-to-try-to-get-bigquery-to-cast-bytes-to-string
        , TO_BASE64(assigned_vendor_hash)
        )
    ) AS asa_id_vendor_hash
    FROM generate_vendor_hash
  )

  , get_past_version as (
    SELECT *
      , LAG(asa_id_vendor_hash) OVER(asa_partition) AS previous_pk
    FROM generate_pk_version
    WINDOW asa_partition AS(PARTITION BY entity_id, asa_id ORDER BY active_from)
  )
  
  , deduplicate_versions AS (
    -- SELECT * EXCEPT(_row_number, next_pk, previous_pk, first_pk)
    SELECT *
      , LEAD(active_from) OVER(PARTITION BY entity_id, asa_id ORDER BY active_from) as active_to
    FROM get_past_version
    WHERE (
      CASE 
        WHEN previous_pk IS NULL THEN TRUE --keep first version
        WHEN ( asa_id_vendor_hash = previous_pk) THEN FALSE -- remove if current is equal to previous
        ELSE TRUE
        END
    )
  )

  , get_vendor_group_config as (
    SELECT DISTINCT global_entity_id as entity_id
      , vendor_group_id
      , vendor_filter
      from `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group`
  )

  , remove_deleted AS (
    SELECT dv.entity_id
      , country_code
      , asa_id
      , asa_name
      , asa_id_vendor_hash
      , dv.vendor_group_id
      , parse_vendor_filter(vendor_filter) as vendor_filters
      , priority
      , active_from
      , active_to
      , assigned_vendor_hash
      , assigned_vendors_count
      , sorted_assigned_vendor_ids
    FROM deduplicate_versions dv
    LEFT JOIN get_vendor_group_config vgc
      ON dv.entity_id = vgc.entity_id
      AND dv.vendor_group_id = vgc.vendor_group_id
    WHERE deleted = FALSE
  )

  SELECT *
  FROM remove_deleted;