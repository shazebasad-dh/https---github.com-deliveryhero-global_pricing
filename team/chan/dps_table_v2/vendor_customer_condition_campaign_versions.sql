CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.vendor_customer_condition_campaign_versions`
CLUSTER BY entity_id, vendor_code
AS
WITH load_fdnc_active_campaigns AS (
  SELECT
    entity_id
    , campaign_id
    , campaign_name
    , vendor_id as vendor_code
    , customer_condition_id
    , time_condition_id
    , country_code
    , active
    , active_from
    , IFNULL(active_to, "2099-01-01") as active_to
    , SHA256(CONCAT(entity_id, vendor_id, campaign_id, active)) as campaign_status_hash
    , SHA256(CONCAT(entity_id, vendor_id, campaign_id)) as campaign_hash

  FROM `logistics-data-storage-staging.long_term_pricing.pricing_campaign_configuration_versions`
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) vendor_id
  WHERE TRUE
    AND vendor_id IS NOT NULL
    AND vendor_id <> ""
    /*
    We only want to consider campaigns with a customer condition.
    */
    AND customer_condition_id IS NOT NULL
    AND customer_condition_config.description IS NOT NULL 
)

/*
First step is to simplify the table by merging consecutive duplicate rows.
By duplicate rows, I mean those two consecutive rows that have the same campaign_status_hash.
The campaign status hash can be extensible to include more columns if we want to increase the versioning granularity.
*/

, get_previous_campaign_hash as (
  SELECT *
  , LAG(campaign_status_hash) OVER(PARTITION BY campaign_hash ORDER BY active_from) as prev_hash
  FROM load_fdnc_active_campaigns
)

/*
This will add a ID only if the previous hash is different from the current hash.
*/
, add_consecutive_row_id as (
  SELECT *
  , SUM(
    CASE WHEN campaign_status_hash = prev_hash THEN 0
    ELSE 1
    END
  ) OVER(PARTITION BY campaign_hash ORDER BY active_from) as _consecutive_id
FROM get_previous_campaign_hash
)


, merge_consecutive_rows as (
  SELECT
  entity_id
  , campaign_id
  , country_code
  , campaign_name
  , vendor_code
  , customer_condition_id
  , time_condition_id
  , active
  , _consecutive_id
  , MIN(active_from) as active_from
  , MAX(active_to) as active_to
  FROM add_consecutive_row_id
  GROUP BY 1,2,3,4,5,6,7,8,9
)

/*
Next big step is to find all overlapping campaigns. 
We find them by doing a self-join on the same entity_id and vendor_code and keep only those rows that have overlapping active periods.
Overlapping periods are defined as:
  - campaign A starts before campaign B ends
  - campaign A ends after campaign B starts
*/

, get_overlap_campaigns as (

SELECT 
a.* EXCEPT(active_from, active_to)
, a.active_from as initial_from
-- , b.campaign_id
, CASE
    WHEN b.active_from IS NULL THEN a.active_from
  ELSE GREATEST(a.active_from, b.active_from) 
  END as active_from
, CASE
    WHEN b.active_to IS NULL THEN a.active_to
  ELSE LEAST(a.active_to, b.active_to)  
  END AS active_to
FROM merge_consecutive_rows a
LEFT JOIN merge_consecutive_rows b
  ON a.entity_id = b.entity_id
  AND a.vendor_code = b.vendor_code
  AND a.campaign_id < b.campaign_id
  AND a.active_from < b.active_to
  AND a.active_to > b.active_from
-- GROUP BY 1,2,3
)

/*
From the previous CTE we obtain all "important" timestamps for each campaign.
We need to aggregate them and create a unique list of timestamps for each vendor. 
These timestamps will mean that such points, a new campaign version is created, either because a campaign starts, ends or changes its active status.
The granularity has already been established earlier by the campaign_status_hash.
*/
, aggregate_by_versioning as (
  SELECT
  entity_id
  , vendor_code
  , ARRAY_AGG(initial_from) initial_from_agg
  , ARRAY_AGG(active_from) as active_from_agg
  , ARRAY_AGG(active_to) AS active_to_agg
  FROM get_overlap_campaigns
  GROUP BY 1,2
)

, ordered_timestamp as (

  SELECT
  entity_id
  , vendor_code
  -- , initial_from
  , ARRAY(
    SELECT DISTINCT x 
    FROM UNNEST(ARRAY_CONCAT(initial_from_agg, active_from_agg, active_to_agg)) x
    ORDER BY x
  ) as ordered_campaign_timestamp
  FROM aggregate_by_versioning
)

, new_campaign_timestamps as (
  SELECT * EXCEPT(ordered_campaign_timestamp)
  , LEAD(active_from) OVER(PARTITION BY entity_id, vendor_code ORDER BY active_from) as active_to
  FROM ordered_timestamp
  LEFT JOIN UNNEST(ordered_campaign_timestamp) active_from
)

, remove_unnecesary_versions as (
  SELECT *
  FROM new_campaign_timestamps
  where active_to is not null
  and active_from < active_to
)

/*
Final steps is to add the campaign configuration for each campaign that was active at the timeframe
defined by the new campaign timestamps.
*/


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

  , add_config_info AS (
  SELECT
  a.entity_id 
  , a.vendor_code
  , a.active_from
  , IF(a.active_to = "2099-01-01" , NULL, a.active_to) as active_to
  , MIN(IF(schedule_config.start_at > a.active_from, schedule_config.start_at, NULL)) AS fdnc_valid_from
  , MAX(IF(schedule_config.recurrence = "NONE", schedule_config.end_at,schedule_config.recurrence_end_at)) AS fdnc_valid_to
  , ARRAY_AGG(
   STRUCT(
      campaign_id
      , campaign_name
      , active
      , b.customer_condition_id
      , time_condition_id
      , customer_condition_config
      , schedule_config
   )
  ) as campaign_configs


  FROM remove_unnecesary_versions a
  LEFT JOIN merge_consecutive_rows b 
    ON a.entity_id = b.entity_id 
    AND a.vendor_code = b.vendor_code
    AND a.active_from >= b.active_from 
    AND a.active_to <= b.active_to

  LEFT JOIN load_schedule_config  ls
    ON b.country_code = ls.country_code
    AND b.time_condition_id = ls.schedule_id

  LEFT JOIN load_customer_condition_config ccc
      ON b.country_code = ccc.country_code
    AND b.customer_condition_id = ccc.customer_condition_id 
  WHERE active = TRUE
  GROUP BY 1,2,3,4
  )

  , final_table AS (
    SELECT * EXCEPT(fdnc_valid_from, fdnc_valid_to)
    , CASE
        WHEN fdnc_valid_from IS NULL THEN active_from
        ELSE GREATEST(fdnc_valid_from, active_from)
      END AS fdnc_valid_from
    , CASE
        WHEN fdnc_valid_to IS NULL THEN active_to
        ELSE LEAST(fdnc_valid_to, active_to)
    END AS fdnc_valid_to
    FROM add_config_info
  )


SELECT  *
-- entity_id
-- , count(vendor_code)
FROM final_table