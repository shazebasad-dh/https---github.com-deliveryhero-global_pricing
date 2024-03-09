########################################## INPUTS


##########################################

CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sl_pandora_impression_elasticity` 
AS
WITH perseus_events AS (
SELECT
partition_date --Equivalent to the date value of the ingestion_timestamp.
  , dh_platform
  , platform
  , country
  , session_key
  , created_date
  , entity_id
  , perseus_session_id
  , perseus_client_id
  , swimlane_request_id
  , vendor_code
  , vertical_type
  , event_action
  , event_time
  , transaction_id
  , locationLon
  , locationLat
  , ev.name
  , ev.value
FROM `logistics-data-storage-staging.temp_pricing.sl_pricing_perseus_events`
LEFT JOIN UNNEST(eventVariables) ev
WHERE TRUE
AND vendor_code is not null
-- WHERE partition_date BETWEEN start_date_filter AND end_date_filter
AND dh_platform IN ('pandora')
AND event_action IN (
'shop_impressions.loaded'
)
AND ev.name IN (
"vendorDeliveryTime"
, "vendorDeliveryFee"
, "vendorMinimumOrderValue"
)
)

, pivot_table AS (
SELECT *
FROM perseus_events
PIVOT(ANY_VALUE(value) FOR
name IN (
"vendorDeliveryTime"
, "vendorDeliveryFee"
, "vendorMinimumOrderValue"
  )
  )
)

, clean_attrs AS (
SELECT a.* EXCEPT(vendorDeliveryFee
, vendorMinimumOrderValue
, vendorDeliveryTime
)
, SAFE_CAST(vendorDeliveryTime AS FLOAT64) AS vendor_delivery_time
, SAFE_CAST(vendorDeliveryFee AS FLOAT64) AS vendor_delivery_fee
, SAFE_CAST(vendorMinimumOrderValue AS FLOAT64) AS vendor_mov
, b.zones
, b.has_order
FROM pivot_table a
INNER JOIN `fulfillment-dwh-production.cl._perseus_sessions_with_location` b
  ON a.session_key = b.session_key 
  AND b.created_date > "2024-01-01"
  AND ARRAY_LENGTH(b.zones) > 0
)

SELECT *
FROM clean_attrs