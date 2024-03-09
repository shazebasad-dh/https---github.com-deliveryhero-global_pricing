WITH load_data AS (
  SELECT *
    , CASE  
    WHEN event_action = "shop_impressions.loaded" THEN CONCAT(1, "-", event_action)
    WHEN event_action = "shop_details.loaded" THEN CONCAT(2, "-", event_action)
    WHEN event_action = "category_details.loaded" THEN CONCAT(3, "-", event_action)
    WHEN event_action = "add_cart.clicked" THEN CONCAT(4, "-", event_action)
    WHEN event_action = "remove_cart.clicked" THEN CONCAT(5, "-", event_action)
    WHEN event_action = "cart.loaded" THEN CONCAT(6, "-", event_action)
    WHEN event_action = "checkout.clicked" THEN CONCAT(7, "-", event_action)
    WHEN event_action = "checkout.loaded" THEN CONCAT(8, "-", event_action)
    WHEN event_action = "transaction" THEN CONCAT(9, "-", event_action)
    END AS event_action_sorted
  FROM `logistics-data-storage-staging.temp_pricing.sl_pricing_perseus_events`
)

, aggregate_events AS (
SELECT event_action_sorted
, dh_platform
, IFNULL(ev.name, "NA") variable_name
, count(DISTINCT perseus_session_id) as n_records
FROM load_data
LEFT JOIN UNNEST(eventVariables) ev
GROUP BY 1,2,3
)

SELECT
event_action_sorted
, dh_platform
, variable_name
, n_records
, n_records_event
FROM aggregate_events
LEFT JOIN (
SELECT event_action_sorted, platform, COUNT(DISTINCT perseus_session_id) as n_records_event
FROM load_data
GROUP BY 1,2
) USING(event_action_sorted, platform)
WHERE variable_name IS NOT NULL