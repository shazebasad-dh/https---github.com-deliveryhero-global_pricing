-- CREATE OR REPLACE TEMP TABLE dps_vendor_history AS

########## DECLARE VARIABLES

DECLARE date_partition TIMESTAMP;
DECLARE run_date TIMESTAMP;
DECLARE backfill BOOL;

########## SET RUN MODE
SET backfill = TRUE;

# SET END DATE 
SET run_date = CURRENT_TIMESTAMP();

# SET PARTITION DATE
IF backfill THEN 
    SET date_partition = TIMESTAMP_SUB("2021-01-01", interval 7 DAY); 
ELSE
    SET date_partition = TIMESTAMP_SUB(run_date, interval 7 DAY);
END IF; 

CREATE TEMP TABLE staging_vendor_asa_config
AS
with unnest_vendor as (
  SELECT entity_id
    , vendor_id as vendor_code
    , asa_id
    , asa_name
    , active_from
    , LAG(asa_id) OVER(PARTITION BY entity_id, vendor_id ORDER BY active_from) as prev_asa_id
  FROM `dh-logistics-product-ops.pricing.dps_asa_vendor_assignments` 
  LEFT JOIN UNNEST(sorted_assigned_vendor_ids) AS vendor_id
  WHERE active_from BETWEEN date_partition AND run_date -- only new versions
  AND vendor_id IS NOT NULL
  AND vendor_id <> ""

)

, deduplicate_vendor_config as (

  SELECT *
    , IFNULL(LEAD(active_from) OVER(PARTITION BY entity_id, vendor_code ORDER BY active_from), "2099-01-01") as active_to
  FROM unnest_vendor
  WHERE (
    CASE 
      WHEN prev_asa_id IS NULL THEN TRUE 
      WHEN asa_id = prev_asa_id THEN FALSE
      ELSE TRUE
    END
  ) 
)

,  asa_price_config as ( 
  SELECT * EXCEPT(active_to)
    , IFNULL(active_to, "2099-01-01") AS active_to
  FROM `dh-logistics-product-ops.pricing.dps_asa_price_config_versions`
  WHERE active_from BETWEEN date_partition - interval 1 day AND run_date -- only new versions
)

, join_price_config AS (
  SELECT vendor_config.* EXCEPT(active_from, active_to)
  , asa_price_config.* EXCEPT(active_from, active_to, asa_id, entity_id, country_code)
  , GREATEST(vendor_config.active_from, asa_price_config.active_from) AS active_from
  , LEAST(vendor_config.active_to, asa_price_config.active_to) AS active_to
  FROM deduplicate_vendor_config vendor_config
  LEFT JOIN asa_price_config
    ON vendor_config.asa_id = asa_price_config.asa_id
    AND vendor_config.entity_id = asa_price_config.entity_id
  WHERE TRUE 
    AND vendor_config.active_from <> vendor_config.active_to
    AND vendor_config.active_from < asa_price_config.active_to
    AND vendor_config.active_to > asa_price_config.active_from  
)

, vendor_full_asa_config as (
    SELECT entity_id
    , vendor_code
    , active_from
    , IF(active_to = "2099-01-01", NULL, active_to) as active_to 
    , ARRAY_AGG(
        STRUCT(asa_id
          , asa_name
          , n_schemes
          , asa_condition_mechanisms
          , asa_price_config_hash
          , asa_price_config
      )
    ) as dps_asa_configuration_history
FROM join_price_config
GROUP BY 1, 2, 3, 4
)
SELECT *
FROM vendor_full_asa_config
-- WHERE entity_id = "PY_AR"
-- and vendor_code = "62168" 
-- order by active_from
;


###### UPSERT
IF backfill THEN 
  CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2`
  CLUSTER BY entity_id, vendor_code
  AS
  SELECT * FROM staging_vendor_asa_config;
ELSE
  MERGE INTO `dh-logistics-product-ops.pricing.dps_vendor_asa_config_versions_v2` dps_vendor
  USING staging_vendor_asa_config stg
    ON dps_vendor.entity_id = stg.entity_id
    AND dps_vendor.vendor_code = stg.vendor_code
    AND dps_vendor.active_from = stg.active_from

  WHEN MATCHED THEN
    UPDATE SET
      entity_id = stg.entity_id
      , vendor_code = stg.vendor_code
      , active_from = stg.active_from
      , active_to = stg.active_to
      , dps_asa_configuration_history = stg.dps_asa_configuration_history
  WHEN NOT MATCHED THEN
    INSERT ROW
  ;
end if;