# ------------------------------------------------------------------------------
# Data Queries
# ------------------------------------------------------------------------------

def get_marketing_data(entities: Union[list, tuple], week: str, restaurant_flag: str) -> str:

    """
    SQL query to extract holdout and non holdout data from marketing (BIMA table) for given entities and week.

    Args:
        entities (tuple or list): List or tuple of entity IDs to include.
        week (str): ISO week date string (format 'YYYY-MM-DD') for query cutoff.
        restaurant_flag (str): String for SQL operator, e.g., 'IN' or 'NOT IN',
                               to filter vendor_vertical_parent values.

    Returns:
        str: SQL query string.
    """
    
    mkt_data = f"""

    WITH holdout_entities AS (
    SELECT 
        entity_id,
        MIN(CASE WHEN is_customer_holdout THEN created_date END) AS release_date
    FROM `fulfillment-dwh-production.curated_data_shared.dps_holdout_users` AS d
    WHERE 
        is_customer_holdout = TRUE 
        AND created_date < '{week}'
        AND entity_id in {entities}
    GROUP BY entity_id
    HAVING COUNT(CASE WHEN is_customer_holdout THEN customer_id END) > 100
    ),
    orders as (
    SELECT
        dps.entity_id entity_id
        ,dps.dps_customer_id customer_id
        ,COUNT(DISTINCT case when mkt.order_date <= e.release_date then mkt.order_id end) AS orders_pre
        ,COUNT(DISTINCT case when mkt.order_date > e.release_date then mkt.order_id end) AS orders_post
        ,SUM(case when mkt.order_date <= e.release_date then mkt.analytical_profit end) AS analytical_profit_pre
        ,SUM(case when mkt.order_date > e.release_date then mkt.analytical_profit end) AS analytical_profit_post
    FROM `fulfillment-dwh-production.cl.dps_sessions_mapped_to_orders` dps
    JOIN holdout_entities AS e
        ON dps.entity_id = e.entity_id
    left join `fulfillment-dwh-production.curated_data_shared_mkt.bima_order_profitability` mkt
        ON mkt.global_entity_id = dps.entity_id
        AND mkt.order_id = dps.platform_order_code
        AND order_date >= DATE_SUB(release_date, INTERVAL 8 WEEK)
        AND order_date < '{week}'
        AND global_entity_id in {entities}
    WHERE dps.created_date >= DATE_SUB(e.release_date, INTERVAL 8 WEEK) 
        AND dps.created_date < '{week}'
        AND dps.platform_order_code IS NOT NULL
        AND dps.is_own_delivery
        AND dps.is_sent
        AND vendor_vertical_parent {restaurant_flag} ('Restaurant','restaurant','restaurants')
        AND dps.entity_id in {entities}
    GROUP BY 1, 2
    ), 
    customer_information AS (
    SELECT
            e.entity_id,
            e.release_date,
            CASE WHEN d.created_date <= e.release_date THEN FALSE ELSE COALESCE(is_customer_holdout, FALSE) END AS is_customer_holdout,
            d.customer_id
    FROM `fulfillment-dwh-production.cl.dps_holdout_users` AS d
    JOIN holdout_entities AS e
            ON d.entity_id = e.entity_id
    LEFT JOIN `fulfillment-dwh-production.cl._bad_dps_logs_ids` bad_ids
        ON d.customer_id = bad_ids.id
    WHERE d.created_date >= '2025-01-01' 
        AND d.created_date < '{week}'
        AND d.customer_id IS NOT NULL
        AND bad_ids.id IS NULL
        AND d.entity_id in {entities}
    GROUP BY 1, 2, 3, 4
    )
    SELECT  p.brand_name,
            e.entity_id,
            e.customer_id,
            e.is_customer_holdout,
            o.orders_pre,
            o.orders_post,
            o.analytical_profit_pre,
            o.analytical_profit_post
    FROM customer_information e
    LEFT JOIN orders o
        ON o.customer_id = e.customer_id
        AND o.entity_id = e.entity_id
    LEFT JOIN (
        SELECT DISTINCT
            p.entity_id,
            c.country_name,
            c.region,
            CASE 
                WHEN p.brand_name IN ("Foodora", "Foodpanda", "Yemeksepeti") THEN "Pandora"
                WHEN p.brand_name IN ("eFood", "Foody") THEN "GR/CY"
                WHEN p.brand_name = "PedidosYa" THEN "PEYA"
                WHEN p.brand_name = "Baemin" THEN "Woowa"
                WHEN p.brand_name = "FoodPanda" THEN "FP APAC"
                ELSE p.brand_name
            END AS brand_name
        FROM `fulfillment-dwh-production.curated_data_shared.countries` c
        LEFT JOIN UNNEST(c.platforms) p
    ) p
    ON e.entity_id = p.entity_id
    WHERE (analytical_profit_post IS NOT NULL AND orders_post IS NOT NULL)

    """

    return mkt_data


def get_dps_data(entities: Union[list, tuple], week: str, restaurant_flag: str) -> str:

    """
    Generate SQL query to extract non holdout and holdout data from DPS tables for entities not in input list (not in marketing tables).

    Args:
        entities (tuple or list): List or tuple of entity IDs to exclude.
        week (str): ISO week date string (format 'YYYY-MM-DD') for query cutoff.
        restaurant_flag (str): String for SQL operator, e.g., 'IN' or 'NOT IN',
                               to filter vendor_vertical_parent values.

    Returns:
        str: SQL query string.
    """

    dps_data = f"""

    WITH holdout_entities AS (
    SELECT 
        entity_id,
        MIN(CASE WHEN is_customer_holdout THEN created_date END) AS release_date
    FROM `fulfillment-dwh-production.curated_data_shared.dps_holdout_users` AS d
    WHERE 
        is_customer_holdout = TRUE 
        AND created_date < '{week}'
        AND entity_id not in {entities}
    GROUP BY entity_id
    HAVING COUNT(CASE WHEN is_customer_holdout THEN customer_id END) > 100
    ),
    orders as (
    SELECT
         dps.entity_id entity_id
        ,dps.dps_customer_id customer_id
        ,COUNT(DISTINCT case when dps.created_date <= e.release_date then dps.platform_order_code end) AS orders_pre
        ,COUNT(DISTINCT case when dps.created_date > e.release_date then dps.platform_order_code end) AS orders_post
        ,SUM(case when dps.created_date <= e.release_date then dps.fully_loaded_gross_profit_eur end) AS analytical_profit_pre
        ,SUM(case when dps.created_date > e.release_date then dps.fully_loaded_gross_profit_eur end) AS analytical_profit_post
    FROM `fulfillment-dwh-production.curated_data_shared.dps_sessions_mapped_to_orders` dps
    JOIN holdout_entities AS e
        ON dps.entity_id = e.entity_id
    WHERE dps.created_date >= DATE_SUB(e.release_date, INTERVAL 8 WEEK) 
        AND dps.created_date < '{week}'
        AND dps.platform_order_code IS NOT NULL
        AND dps.is_own_delivery
        AND dps.is_sent
        AND vendor_vertical_parent {restaurant_flag} ('Restaurant','restaurant','restaurants')
        AND dps.entity_id not in {entities}
    GROUP BY 1, 2
    ), 
    customer_information AS (
    SELECT
            e.entity_id,
            e.release_date,
            CASE WHEN d.created_date <= e.release_date THEN FALSE ELSE COALESCE(is_customer_holdout, FALSE) END AS is_customer_holdout,
            d.customer_id
    FROM `fulfillment-dwh-production.curated_data_shared.dps_holdout_users` AS d
    JOIN holdout_entities AS e
            ON d.entity_id = e.entity_id
    LEFT JOIN `fulfillment-dwh-production.cl._bad_dps_logs_ids` bad_ids
        ON d.customer_id = bad_ids.id
    WHERE d.created_date BETWEEN '2025-01-01' AND '{week}'
        AND d.customer_id IS NOT NULL
        AND bad_ids.id IS NULL
    GROUP BY 1, 2, 3, 4
    )
    SELECT 
        p.brand_name,
        e.entity_id,
        e.customer_id,
        e.is_customer_holdout,
        o.orders_pre,
        o.orders_post,
        o.analytical_profit_pre,
        o.analytical_profit_post
    FROM customer_information e
    LEFT JOIN orders o
        ON o.customer_id = e.customer_id
        AND o.entity_id = e.entity_id
    LEFT JOIN (
        SELECT DISTINCT
            p.entity_id,
            c.country_name,
            c.region,
            CASE 
                WHEN p.brand_name IN ("Foodora", "Foodpanda", "Yemeksepeti") THEN "Pandora"
                WHEN p.brand_name IN ("eFood", "Foody") THEN "GR/CY"
                WHEN p.brand_name = "PedidosYa" THEN "PEYA"
                WHEN p.brand_name = "Baemin" THEN "Woowa"
                WHEN p.brand_name = "FoodPanda" THEN "FP APAC"
                ELSE p.brand_name
            END AS brand_name
        FROM `fulfillment-dwh-production.curated_data_shared.countries` c
        LEFT JOIN UNNEST(c.platforms) p
    ) p
    ON e.entity_id = p.entity_id
    WHERE (analytical_profit_post IS NOT NULL AND orders_post IS NOT NULL)

    """

    return dps_data