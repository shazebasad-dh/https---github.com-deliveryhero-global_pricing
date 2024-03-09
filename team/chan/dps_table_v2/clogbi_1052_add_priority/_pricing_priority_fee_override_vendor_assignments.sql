----------------------------------------------------------------------------------------------------------------------------
--                NAME: _pricing_priority_fee_override_vendor_assignments.sql
--               OWNER: Logistics Data Analytics/Customer
--      INITIAL AUTHOR: Sebastian Lafaurie
--       CREATION DATE: 2024-02-21
--         DESCRIPTION: This table extends _pricing_asa_vendor_assignments to include all historical information about DPS Priority Fee Overrides vendor assignments, on version level.
--        QUERY OUTPUT: Every version of every vendor assigned to a Priority Fee Override can be obtained.
--               NOTES:
--             UPDATED: 2024-02-21 | Sebastian Lafaurie | CLOGBI-1052 | Create table
--                      ----------------------------------

CREATE TEMP FUNCTION PARSE_VENDOR_FILTER(json STRING)
RETURNS ARRAY<
  STRUCT<
    key STRING
    , clause STRING
    , value ARRAY<STRING>
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
-- CREATE OR REPLACE TABLE `{{ params.project_id }}.cl._pricing_priority_fee_override_vendor_assignments`
-- CLUSTER BY entity_id, asa_id
-- AS
-- Load ASA -- Get the ASA ids and vendor_group_ids for the base of the table.
WITH load_asa_vendor AS (
  SELECT
    type
    , global_entity_id AS entity_id
    , country_code
    , vendor_group_assignment_id AS asa_id
    , vendor_group_id
    , name AS asa_name
    , TIMESTAMP_TRUNC(created_at, MINUTE) AS created_at
    , TIMESTAMP_TRUNC(updated_at, MINUTE) AS updated_at
    , ROW_NUMBER() OVER(PARTITION BY global_entity_id, vendor_group_assignment_id ORDER BY updated_at) AS _row_number
    , deleted
    , priority
    , ARRAY((SELECT x FROM UNNEST(assigned_vendor_ids) x ORDER BY x)) AS sorted_assigned_vendor_ids -- arrange vendor_ids alphabetically
  -- FROM `{{ params.project_id }}.hl.dynamic_pricing_vendor_group_assignment`
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group_assignment`
  WHERE type = 'ASA_PRIORITY_FEE_OVERRIDE' -- here we filter for priority assignments only
-- Set Active From -- For the same ASA id, every time it is updated we are creating a new version, so we assign a new active_from
), set_active_from AS (
  SELECT
    * EXCEPT(created_at, updated_at)
    , IF(_row_number = 1, created_at, updated_at) AS active_from
  FROM load_asa_vendor
-- Generate Hash -- This CTE creates a hash per vendor group. If a new vendor is added, the hash changes as it is considered a new version.
), generate_vendor_hash AS (
  SELECT
    *
    , SHA256(ARRAY_TO_STRING(sorted_assigned_vendor_ids, '')) AS assigned_vendor_hash
    , ARRAY_LENGTH(sorted_assigned_vendor_ids) AS assigned_vendors_count
  FROM set_active_from
-- Generate Primary Key -- Create a primary key of the asa --> vendor hash(from previous CTE) + vendor_group_id + pirority + deleted
), generate_pk_version AS (
  SELECT
    *
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
-- Get the previous version -- This CTE is necessary because source data can sometimes have duplicated rows. When 2 consecutive rows have the same configurations, we need to delete de duplicate.
), get_previous_version AS (
  SELECT
    *
    , LAG(asa_id_vendor_hash) OVER (asa_partition) AS previous_pk
  FROM generate_pk_version
  WINDOW asa_partition AS (PARTITION BY entity_id, asa_id ORDER BY active_from)
-- Deduplication -- When 2 consecutive rows have the same configurations, we need to delete de duplicate.
), deduplicate_versions AS (
  SELECT
    *
    , LEAD(active_from) OVER (PARTITION BY entity_id, asa_id ORDER BY active_from) AS active_to
  FROM get_previous_version
  WHERE (
      CASE
        WHEN previous_pk IS NULL THEN TRUE --we keep the first version
        WHEN (asa_id_vendor_hash = previous_pk) THEN FALSE -- we remove version when the current one is equal to the previous one
        ELSE TRUE
      END
    )
-- Vendor Group Config -- Brings vendor_group_ids and characteristics about the vendor gruops.
), get_vendor_group_config AS (
  SELECT DISTINCT
    global_entity_id AS entity_id
    , vendor_group_id
    , vendor_filter
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group`
  -- FROM `{{ params.project_id }}.hl.dynamic_pricing_vendor_group`
)
--  Finally we join with the information about the vendor groups and we remove the versions that are deleted.
SELECT
  type
  , dv.entity_id
  , country_code
  , asa_id
  , asa_name
  , asa_id_vendor_hash
  , dv.vendor_group_id
  , PARSE_VENDOR_FILTER(vendor_filter) AS vendor_filters
  , priority
  , active_from
  , active_to
  , assigned_vendor_hash
  , assigned_vendors_count
  , sorted_assigned_vendor_ids
FROM deduplicate_versions dv
LEFT JOIN get_vendor_group_config vgc ON dv.entity_id = vgc.entity_id
    AND dv.vendor_group_id = vgc.vendor_group_id
WHERE deleted = FALSE