CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.pricing_vendor_group_versions` AS 
WITH vendor_group_updates AS (
    SELECT
    global_entity_id AS entity_id
    , vendor_group_id
    /*these are database timestamp with a high time-unit granularity
    by truncating to minutes we make it more relatable
    to human interaction timestamp and reduce table size
    */
    , TIMESTAMP_TRUNC(updated_at, MINUTE) AS vendor_group_updated_at
    -- convert the vendor_id array to string and sort it so that we can compare it to the former and the next one:
    , SHA256(ARRAY_TO_STRING(ARRAY(SELECT id FROM UNNEST(matching_vendor_ids) id ORDER BY id), '')) AS matching_vendor_ids_hash
    , ARRAY_LENGTH(matching_vendor_ids) AS matching_vendor_id_count
    , ARRAY(SELECT id FROM UNNEST(matching_vendor_ids) id ORDER BY id) as sorted_vendor_id
  FROM `fulfillment-dwh-production.hl.dynamic_pricing_vendor_group`
  WHERE created_date >= '2022-01-01'

  ), vendor_group_updates_lag AS (
  SELECT
    *
    , LAG(matching_vendor_ids_hash) OVER (PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) AS lag_matching_vendor_ids
  FROM vendor_group_updates
), vendor_group_updates_remove_duplicate AS (
  -- This CTE will filter out updates where the vendor list was not changed
  SELECT
    *
    , LEAD(vendor_group_updated_at) OVER (PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) AS next_updated_at
  FROM vendor_group_updates_lag
  WHERE lag_matching_vendor_ids IS NULL -- take the first update for every vendor_group_id
    OR matching_vendor_ids_hash != lag_matching_vendor_ids  -- take updates that are different than the former update
--check the changes inside each subperiod
)
, vendor_group_updates_clean AS (
  SELECT
    entity_id
    , vendor_group_id
    , vendor_group_updated_at
    , LEAD(vendor_group_updated_at, 1) OVER(PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at) next_updated_at
    -- , IFNULL(
    --   LEAD(vendor_group_updated_at, 1) OVER(PARTITION BY entity_id, vendor_group_id ORDER BY vendor_group_updated_at)
    --   , "2099-01-01"
    -- ) as next_updated_at
    , matching_vendor_ids_hash
    , sorted_vendor_id
    , matching_vendor_id_count
  FROM vendor_group_updates_remove_duplicate
)

SELECT *
FROM vendor_group_updates_clean