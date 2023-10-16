################################### SUBS MARKTES
    CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_subs_markets`
        (
        quarter DATE,
        entity_id STRING,
        has_subscription BOOL
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
        sheet_range="subs_markets!A:C",
        skip_leading_rows=1
    );
###################################


################################### COMPETITIVE INPUTS
    CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_competitive_inputs`
        (
        quarter DATE,
        entity_id STRING,
        competitor_has_sf BOOL
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
        sheet_range="comp_inputs!A:C",
        skip_leading_rows=1
    );
###################################

################################### FLEET CONSTRAINTS
    CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_fleet_constraints_inputs`
        (
        quarter DATE,
        entity_id STRING,
        has_fleet_constraints BOOL,
        has_extreme_fleet_constraints BOOL
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
        sheet_range="fleet_constraints!A:D",
        skip_leading_rows=1
    );
###################################

################################### MARKET ARCHETYPE STATUS
    CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_market_archetype`
        (
        quarter DATE,
        entity_id STRING,
        market_archetype STRING,
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
        sheet_range="market_leadership!A:C",
        skip_leading_rows=1
    );
###################################

################################### LEGAL STATUS
    CREATE OR REPLACE EXTERNAL TABLE `logistics-data-storage-staging.long_term_pricing.compscore_legal_status`
        (
        quarter DATE,
        entity_id STRING,
        mechanism_code STRING,
        is_legal BOOL
        )
        OPTIONS (
        format="GOOGLE_SHEETS",
        uris=["https://docs.google.com/spreadsheets/d/1ulLRME_ktq81A1HdWWPozxAtJxKAsl5r8Xh3an8aAnQ/edit#gid=0"],
        sheet_range="pm_legality!A:D",
        skip_leading_rows=1
    );
###################################
