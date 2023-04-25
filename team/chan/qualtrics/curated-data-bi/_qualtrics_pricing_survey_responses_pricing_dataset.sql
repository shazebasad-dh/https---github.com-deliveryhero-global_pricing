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
--             UPDATED: 2023-03-24 | Sebastian Lafaurie | CLOGBI - 783  | Major refactor to support backfill and older surveys
--             UPDATED: 2023-04-18 | Sebastian Lafaurie | CLOGBI - 783  | Change keys used to extract questions values

--                      ----------------------------------

DECLARE start_date, end_date DATE;

SET end_date = CURRENT_DATE();
SET start_date = "2020-01-01"; --whole history


/* For all surveys, there's a function that retrieves standard attributes.
Which are the same for all surveys
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

/* Qualtrics IDs (QID) are unique per surveys; some surveys have QID2,QID3,QID4,QID5
while other have QID7,QID8,QID9,QID10. It's not scalalabe to track each survey unique QID.
therefore, we take advantage of some surveys pattern design:
- All questions answered by users ends with _TEXT, e.g., QID2_TEXT, QID3_TEXT
- By design, VW questions are placed at the end; meaning that they're always the last 4 values
in a array of all question. 

*/
CREATE TEMP FUNCTION PARSE_VW_ANSWERS(json STRING)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>>
LANGUAGE js AS """
  const filterRaw = JSON.parse(json);
  const criteria = filterRaw['values'];
  const keys = Object.keys(criteria).filter(key => key.endsWith('_TEXT'));
  const pairs = keys.map(key => ({key, value: criteria[key]}));
  return pairs.length > 0 ? pairs:null;
""";


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

, parsed_response AS (
  SELECT
    created_date
    , created_at
    , response_id
    , ARRAY(SELECT PARSE_STANDARD_ATTRIBUTES(JSON_EXTRACT(payload, '$.values'))) AS response
    , PARSE_VW_ANSWERS(payload) AS vw_response
    , survey_type
    , survey_id
  FROM `dh-logistics-product-ops.pricing._dl_pricing_qualtrics_survey_export`
  -- FROM `fulfillment-dwh-production.dl_gcc_service.qualtrics_survey_export`
  WHERE survey_type = 'global_pricing'
  AND created_date BETWEEN start_date AND end_date
  -- AND survey_id = "SV_7TYmyrtMMR8jt9Y"

)

, filter_unvalid_responses as (
  SELECT * EXCEPT(response, vw_response)
    /*
    Filter only VW answers
    */
  , ARRAY(SELECT x.value
      FROM UNNEST(vw_response) x 
      WITH OFFSET as vw_answer_order 
      WHERE (CASE 
            WHEN ARRAY_LENGTH(vw_response) = 4 THEN TRUE
            /*
            We assume that VW answers 
            are always the last 4 an user answer; which is how
            all surveys so far has been designed
            */
            WHEN vw_answer_order >= ARRAY_LENGTH(vw_response) - 4 THEN TRUE 
            ELSE FALSE
          END
      )
    ) AS only_vw_answers
    /*
    Save other answers as an array
    */
    , ARRAY(SELECT x
      FROM UNNEST(vw_response) x 
      WITH OFFSET as vw_answer_order 
      WHERE (CASE 
            WHEN ARRAY_LENGTH(vw_response) = 4 THEN FALSE
            WHEN vw_answer_order < ARRAY_LENGTH(vw_response) - 4 THEN TRUE 
            ELSE FALSE
          END
      )
    ) AS other_answers
  
  , DATE(DATE_TRUNC(created_at, MONTH)) as response_month
  FROM parsed_response
  LEFT JOIN UNNEST(response) r
  WHERE TRUE
  AND status = 0 -- keep only valid user responses
  -- finished and complete answers
  AND finished = 1 
  AND array_length(vw_response) >= 4 
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

-- , sort_vw_answer as (
--   SELECT * EXCEPT(only_vw_answers)
--   /*
--   Takes into account whether the surveys have the
--   question in ascending or descending order
--   */
--   , ARRAY(SELECT x FROM UNNEST(only_vw_answers) x ORDER BY x) only_vw_answers
--   FROM filter_isolated_answers
-- )

, enrich_data_from_helper as (
  SELECT parsed.* except(only_vw_answers)
  /*
  we also extract each VW responses by position as
  this is how survey are designed
  */
  , only_vw_answers[OFFSET(0)] as too_inexpensive
  , only_vw_answers[OFFSET(1)] as inexpensive
  , only_vw_answers[OFFSET(2)] as expensive
  , only_vw_answers[OFFSET(3)] as too_expensive
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

, final_table as (

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
  , other_answers
from enrich_data_from_orders
)

SELECT *
from final_table;