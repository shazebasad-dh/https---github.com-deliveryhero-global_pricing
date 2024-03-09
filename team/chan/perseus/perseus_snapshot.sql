########################################## INPUTS

########## DECLARE VARIABLES

DECLARE start_date_filter, end_date_filter DATE;

SET end_date_filter = "2024-02-20";
SET start_date_filter = "2024-02-13";

##########################################

CREATE OR REPLACE TABLE `logistics-data-storage-staging.temp_pricing.sl_pricing_perseus_events` AS
WITH perseus_events AS (
SELECT
partition_date --Equivalent to the date value of the ingestion_timestamp.
, DATE(ingestion_timestamp) AS created_date
, dh_platform
, platform
, country
, global_entity_id AS entity_id
, sessionid AS perseus_session_id
, clientId AS perseus_client_id
, session_key
, swimlaneRequestId AS swimlane_request_id
, shopId AS vendor_code
, shopType AS vertical_type
, eventAction AS event_action
, payload_timestamp_local AS event_time
, transactionId AS transaction_id
, expeditionType AS expedition_type
, locationLon
, locationLat
, eventVariables
-- , ev.name
-- , ev.value
FROM `fulfillment-dwh-production.curated_data_shared_coredata_tracking.perseus_events`
-- LEFT JOIN UNNEST(eventVariables) ev
WHERE partition_date BETWEEN start_date_filter AND end_date_filter
AND dh_platform IN ('pandora', 'pedidosya', 'talabat', 'hungerstation', 'efood')
AND global_entity_id IS NOT NULL
AND coredata_errors.is_valid
AND eventAction IN (
"shop_impressions.loaded"
, "shop_details.loaded"
, "category_details.loaded"
, "add_cart.clicked"
, "remove_cart.clicked"
, "cart.loaded"
, "checkout.clicked"
, "checkout.loaded"
, "transaction"
)
)
SELECT *
FROM perseus_events