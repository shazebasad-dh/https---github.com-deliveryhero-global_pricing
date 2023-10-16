CREATE OR REPLACE TABLE `logistics-data-storage-staging.long_term_pricing.compscore_evaluation_per_country_v2` AS

################################# LOAD SOURCES

      with markets AS (
            SELECT
            entity_id
            FROM UNNEST([
                  "DJ_CZ"
                  ,"FO_NO"
                  ,"FP_SK"
                  ,"HN_DK"
                  ,"MJM_AT"
                  ,"NP_HU"
                  ,"OP_SE"
                  ,"PO_FI"
                  ,"YS_TR"
                  ,"EF_GR"
                  ,"FY_CY"
                  ,"FP_BD"
                  ,"FP_HK"
                  ,"FP_KH"
                  ,"FP_LA"
                  ,"FP_MM"
                  ,"FP_MY"
                  ,"FP_PH"
                  ,"FP_PK"
                  ,"FP_SG"
                  ,"FP_TH"
                  ,"FP_TW"
                  ,"HS_SA"
                  ,"AP_PA"
                  ,"PY_AR"
                  ,"PY_BO"
                  ,"PY_CL"
                  ,"PY_CR"
                  ,"PY_DO"
                  ,"PY_EC"
                  ,"PY_GT"
                  ,"PY_HN"
                  ,"PY_NI"
                  ,"PY_PE"
                  ,"PY_PY"
                  ,"PY_SV"
                  ,"PY_UY"
                  ,"PY_VE"
                  ,"HF_EG"
                  ,"TB_AE"
                  ,"TB_BH"
                  ,"TB_IQ"
                  ,"TB_JO"
                  ,"TB_KW"
                  ,"TB_OM"
                  ,"TB_QA"
            ]) as entity_id
      )
      
      , quarters AS (
            SELECT
            quarter
            FROM UNNEST(
            GENERATE_DATE_ARRAY("2023-07-01", "2025-01-01",INTERVAL 1 QUARTER)
            ) as quarter
            WHERE TRUE
            AND quarter <= CURRENT_DATE()
      )

      , cross_join_markets_quarters AS (
            SELECT quarter
            , entity_id
            FROM markets 
            CROSS JOIN quarters
      )

      , subs_markets AS (
            SELECT *
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_subs_markets`
      )

      , comp_inputs AS (
            SELECT *
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_competitive_inputs`
      )

      , fleet_constraints AS (
            SELECT *
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_fleet_constraints_inputs`
      )

      , market_leadership AS (
            SELECT *
            , LOWER(market_archetype) LIKE "%leadership%"  as has_leadership
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_market_archetype`
      )

      , legality AS (
            SELECT *
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_legal_status`
      )

      , performance_inputs AS (
            SELECT *
            FROM `logistics-data-storage-staging.long_term_pricing.compscore_performance_inputs`
      )


#################################

################################# DECISION TREES

      , dbdf_tree AS (
            SELECT 
            market.*
            , "dbdf" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market
            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter  
                  AND legality.mechanism_code = "dbdf"   
      )

      , soft_mov_tree AS (
            SELECT 
            market.*
            , "soft_mov" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market
            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "soft_mov"   
      
      )

      , fdnc_tree AS (
            SELECT 
            market.*
            , "fdnc" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market
            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "fdnc"   
      
      )

      , time_conditions_tree AS (
            SELECT 
            market.*
            , "tod_dow" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market
            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "tod_dow"   
      )

      , sbf_tree AS (
            SELECT 
            market.*
            , "sbf" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market
            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "sbf"   
      )

      , service_fee_tree AS (
            SELECT 
            market.*
            , "service_fee" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  AND (
                        IFNULL(sm.has_subscription,FALSE)
                        OR  
                        IFNULL(ml.has_leadership,FALSE)
                        OR 
                        IFNULL(cp.competitor_has_sf,FALSE)
                  )
                  THEN TRUE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "service_fee"  

            LEFT JOIN subs_markets sm
                  ON market.entity_id = sm.entity_id
                  AND market.quarter = sm.quarter

            LEFT JOIN market_leadership ml
                  ON market.entity_id = ml.entity_id
                  AND market.quarter = ml.quarter
            
            LEFT JOIN comp_inputs cp
                  ON market.entity_id = cp.entity_id
                  AND market.quarter = cp.quarter
      )

      , bvdf_tree AS (
            SELECT 
            market.*
            , "bvdf" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  THEN 
                        CASE 
                              WHEN IFNULL(ml.has_leadership, FALSE) THEN FALSE
                              WHEN pi.has_low_cf_over_afv AND pi.has_high_comm_rate THEN TRUE
                              ELSE FALSE
                        END
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "bvdf"  

            LEFT JOIN market_leadership ml
                  ON market.entity_id = ml.entity_id
                  AND market.quarter = ml.quarter

            LEFT JOIN performance_inputs pi
                  ON market.entity_id = pi.entity_id
                  AND market.quarter = pi.quarter
      )

      , dbmov_tree AS (
            SELECT 
            market.*
            , "dbmov" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  THEN 
                        CASE 
                              WHEN IFNULL(ml.has_leadership, FALSE) THEN TRUE
                              ELSE FALSE
                        END
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "dbmov"  

            LEFT JOIN market_leadership ml
                  ON market.entity_id = ml.entity_id
                  AND market.quarter = ml.quarter
      )

      , surge_tree AS (
            SELECT 
            market.*
            , "surge" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  THEN 
                        CASE 
                              WHEN IFNULL(fc.has_fleet_constraints, FALSE) THEN TRUE
                              ELSE FALSE
                        END
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "surge"  

            LEFT JOIN fleet_constraints fc
                  ON market.entity_id = fc.entity_id
                  AND market.quarter = fc.quarter
      )

      , customer_location_tree AS (
            SELECT 
            market.*
            , "customer_location" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  THEN FALSE
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "customer_location"  
            

      )

      , priority_tree AS (
            SELECT 
            market.*
            , "priority_fee" as mechanism_code
            , CASE 
                  WHEN IFNULL(is_legal, TRUE) 
                  THEN CASE
                        WHEN IFNULL(ml.has_leadership, FALSE) 
                              AND NOT IFNULL(fc.has_extreme_fleet_constraints, FALSE) 
                              THEN TRUE
                        ELSE FALSE
                        END
                  ELSE FALSE
            END AS should_have
            FROM cross_join_markets_quarters market

            LEFT JOIN legality
                  ON market.entity_id = legality.entity_id
                  AND market.quarter = legality.quarter    
                  AND legality.mechanism_code = "priority_fee" 

            LEFT JOIN market_leadership ml
                  ON market.entity_id = ml.entity_id
                  AND market.quarter = ml.quarter

            LEFT JOIN fleet_constraints fc
                  ON market.entity_id = fc.entity_id
                  AND market.quarter = fc.quarter
      )

#################################

################################# UNION RESULTS
      , combine_trees AS (
            SELECT *
            FROM dbdf_tree

            UNION ALL

            SELECT *
            FROM soft_mov_tree

            UNION ALL 

            SELECT *
            FROM fdnc_tree

            UNION ALL

            SELECT *
            FROM time_conditions_tree

            UNION ALL

            SELECT *
            FROM sbf_tree

            UNION ALL

            SELECT *
            FROM service_fee_tree

            UNION ALL

            SELECT *
            FROM bvdf_tree

            UNION ALL

            SELECT *
            FROM dbmov_tree

            UNION ALL

            SELECT *
            FROM surge_tree

            UNION ALL

            SELECT *
            FROM customer_location_tree

            UNION ALL

            SELECT *
            FROM priority_tree
      )


#################################
      
SELECT *
FROM combine_trees;