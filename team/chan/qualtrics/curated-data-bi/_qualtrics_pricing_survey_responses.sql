----------------------------------------------------------------------------------------------------------------------------
--                NAME: _qualtrics_pricing_survey_responses.sql
--      INITIAL AUTHOR: Tanmoy Porel
--       CREATION DATE: 2022-10-07
--         DESCRIPTION: The purpose of this script is to create a cl table that parses payload of responses from QUaltrics Survey configured by global-pricing
--                      parsing table: dl_gcc_service.qualtrics_survey_export
--                      - helper:         dl.analytics_pricing_qualtrics_helper
--                      - helper source:  https://docs.google.com/spreadsheets/d/1KRsm-mt21yuLzZwzc8k02B5Ou4DhmIhfIz9wZTA6jL0/edit?usp=sharing.
--                      - helper tab:     pricing_qualtrics_helper
--                      - helper config:  /dags/log/configuration/yaml/dwh_imports/apps/analytics.yaml
--                      - survey_ids:     dags/log/configuration/yaml/dwh_imports/apps/qualtrics.yaml
--        QUERY OUTPUT: The result of this query is to have each row for each response, structure similar to: dl.vw_responses_data_imported.
--               NOTES: This script is using Service dl dl_gcc_service, however we dont add dependency in yaml as cross BU DL dependencies aren't yet feasible
--             UPDATED: 2022-10-21 | Tanmoy Porel | CLOGBI-740 | Name changes and improvements (RASD-3512)
--             UPDATED: 2022-10-21 | Tanmoy Porel | CLOGBI-756 | Add basket value based on last order id
--             UPDATED: 2023-03-24 | |  | Major refactor to support backfill and older surveys
--                      ----------------------------------

DECLARE start_date, end_date DATE;

SET end_date = CURRENT_DATE();
SET start_date = "2020-01-01"; --whole history
-- SET start_date = DATE_SUB(end_date, interval 7 day); --whole history


/* For all surveys, there's a function that retrieves stadnard attributes.
A subset of surveys uses "Too Cheap" and "Bargain" while other (and new surveys) uses
"Too Inexpensive" and "Inexpensive" as keys
*/
CREATE TEMPORARY FUNCTION PARSE_STANDARD_ATTRIBUTES(value STRING) AS (
  (SELECT AS STRUCT
    JSON_EXTRACT_SCALAR(value, "$['startDate']") AS startdate
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['status']") AS INT64) AS status
    , JSON_EXTRACT_SCALAR(value, "$['duration']") AS duration
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['finished']") AS INT64) AS finished
    , JSON_EXTRACT_SCALAR(value, "$['cityDs']") AS city
    , JSON_EXTRACT_SCALAR(value, "$['lastOrderId']") AS lastorderid
  )
);


CREATE TEMPORARY FUNCTION PARSE_GROUP1_KEYS(value STRING) AS (
  (SELECT AS STRUCT
    SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Too Cheap']") AS FLOAT64) AS too_inexpensive --For keys with spaces/special chars
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Bargain']") AS FLOAT64) AS inexpensive
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Expensive']") AS FLOAT64) AS expensive
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Too Expensive']") AS FLOAT64) AS too_expensive
  )
);

CREATE TEMPORARY FUNCTION PARSE_GROUP2_KEYS(value STRING) AS (
  (SELECT AS STRUCT
    SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Too Inexpensive']") AS FLOAT64) AS too_inexpensive --For keys with spaces/special chars
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Inexpensive']") AS FLOAT64) AS inexpensive
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Expensive']") AS FLOAT64) AS expensive
    , SAFE_CAST(JSON_EXTRACT_SCALAR(value, "$['Too Expensive']") AS FLOAT64) AS too_expensive
  )
);


CREATE OR REPLACE TABLE `dh-logistics-product-ops.pricing._qualtrics_pricing_survey_responses_stg`
PARTITION BY wave_date
CLUSTER BY country_code, survey_id
AS
WITH location_data AS (

SELECT DISTINCT
    LOWER(c.country_code) as country_code
    , c.region
    , country_name
    , currency_code
    , p.entity_id as global_entity_id
  FROM `fulfillment-dwh-production.cl.countries` c
  LEFT JOIN UNNEST(platforms) p
  INNER JOIN `fulfillment-dwh-production.dl.dynamic_pricing_global_configuration`  dps
    ON p.entity_id = dps.global_entity_id
    AND c.country_code = dps.country_code
  WHERE TRUE
    -- remove ODR entities
    AND NOT CONTAINS_SUBSTR(entity_id, "ODR")

    -- remove irrelevant Europe entities
    AND entity_id NOT IN ("FO_FI", "FO_SE")
    AND NOT CONTAINS_SUBSTR(entity_id, "DN_")
    AND NOT CONTAINS_SUBSTR(entity_id, "_RO")

    -- remove irrelevant Europe entities
    AND NOT CONTAINS_SUBSTR(entity_id, "_JP")

  -- ORDER BY region
)

, orders_data as (
  SELECT
  order_id
  , dwh.global_entity_id
  , value.gbv_local AS basket_value
  , delivery_location.city
  FROM `fulfillment-dwh-production.curated_data_shared_central_dwh.orders` dwh
  INNER JOIN location_data ld
    ON dwh.global_entity_id = ld.global_entity_id
  WHERE placed_at_local >= DATE_SUB(start_date, interval 1 week)
)

, parsed_response_1 AS (
  SELECT
    created_date
    , created_at
    , response_id
    , ARRAY(SELECT PARSE_STANDARD_ATTRIBUTES(JSON_EXTRACT(payload, '$.values'))) AS response
    , ARRAY(SELECT PARSE_GROUP1_KEYS(JSON_EXTRACT(payload, '$.values'))) AS vw_response
    , survey_type
    , survey_id
  -- FROM `dh-logistics-product-ops.pricing._dl_pricing_qualtrics_survey_export`
  FROM `fulfillment-dwh-production.dl_gcc_service.qualtrics_survey_export`
  WHERE survey_type = 'global_pricing'
  AND created_date BETWEEN start_date AND end_date
)

, parsed_response_2 AS (
  SELECT
    created_date
    , created_at
    , response_id
    , ARRAY(SELECT PARSE_STANDARD_ATTRIBUTES(JSON_EXTRACT(payload, '$.values'))) AS response
    , ARRAY(SELECT PARSE_GROUP2_KEYS(JSON_EXTRACT(payload, '$.values'))) AS vw_response 
    , survey_type
    , survey_id
  -- FROM `dh-logistics-product-ops.pricing._dl_pricing_qualtrics_survey_export`
  FROM `fulfillment-dwh-production.dl_gcc_service.qualtrics_survey_export`
  WHERE survey_type = 'global_pricing'
  AND created_date BETWEEN start_date AND end_date

)
, append_responses as (
  SELECT *
  --- count if all VW answers are complete
  , (SELECT 
    IF(too_inexpensive IS NULL, 0, 1)
      + IF(inexpensive IS NULL, 0, 1)
      + IF(expensive IS NULL, 0, 1)
      + IF(too_expensive IS NULL, 0, 1)    
    FROM UNNEST(vw_response) as x
    )
    as n_parsed_answers
  FROM parsed_response_1

  UNION ALL 

  SELECT *
  , (SELECT 
    --- count if all VW answers are complete
    IF(too_inexpensive IS NULL, 0, 1)
      + IF(inexpensive IS NULL, 0, 1)
      + IF(expensive IS NULL, 0, 1)
      + IF(too_expensive IS NULL, 0, 1)    
    FROM UNNEST(vw_response) as x
    )
    as n_parsed_answers
  FROM parsed_response_2
)
, deduplicate_responses as (
  SELECT *
  FROM append_responses
  --- leave the parsing method that fetch the most number of VW answers for a given response.
  qualify row_number() over(partition by survey_id, response_id ORDER BY n_parsed_answers DESC) = 1
)

, filter_unvalid_responses as (
  SELECT * EXCEPT(response, vw_response)
  , DATE(DATE_TRUNC(created_at, MONTH)) as response_month
  FROM deduplicate_responses
  LEFT JOIN UNNEST(response) r
  LEFT JOIN UNNEST(vw_response)
  WHERE TRUE
  AND status = 0 -- keep only valid user responses
  -- finished and complete answers
  AND finished = 1 
  AND n_parsed_answers = 4 
 ) 


, filter_isolated_answers as (
  SELECT *
  FROM filter_unvalid_responses
  --- Remove answers done in weeks where there not enough answers
  --- meaning that such answers are "isolated"
  QUALIFY COUNT(response_id) OVER(
    PARTITION BY survey_id
    ORDER BY UNIX_DATE(DATE(created_at))
    RANGE BETWEEN 7 PRECEDING AND 7 FOLLOWING
  ) > 100
)

, enrich_data_from_helper as (
  SELECT parsed.*
  , helper.survey_name
  , helper.country_code AS country_code
  , helper.vertical_parent
  FROM filter_isolated_answers parsed
  LEFT JOIN `dh-logistics-product-ops.pricing._analytics_pricing_qualtrics_helper` helper 
  ON helper.survey_id = parsed.survey_id
)

, enrich_data_from_location as (
  SELECT parsed.*
  , ld.country_name
  , ld.region
  , ld.currency_code
  , ld.global_entity_id

  FROM enrich_data_from_helper parsed
  LEFT JOIN location_data ld
    ON parsed.country_code = ld.country_code
)

, enrich_data_from_orders as (
  SELECT parsed.* EXCEPT(city)
  , IFNULL(parsed.city, orders.city) as city
  , basket_value
  FROM enrich_data_from_location parsed
  LEFT JOIN orders_data orders
    ON parsed.lastOrderId = orders.order_id
    AND parsed.global_entity_id = orders.global_entity_id
)


 SELECT
  region
  , country_name
  , country_code 
  , currency_code
  , "Qualtrics" AS type
  , survey_id
  , survey_name
  , response_month AS wave_date
  , created_date
  , created_at
  , response_id
  , too_inexpensive
  , inexpensive
  , expensive
  , too_expensive
  , city
  , lastOrderId AS last_order_id
  , basket_value
  , vertical_parent


from enrich_data_from_orders;