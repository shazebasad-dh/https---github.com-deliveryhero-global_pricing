    -- DECLARE entity_filter STRING DEFAULT "YS_TR";
    -- DECLARE city_id_filter INT64 DEFAULT 1;

    DECLARE entity_id_filter STRING DEFAULT "{entity_id}";
    DECLARE city_id_filter INT64 DEFAULT {city_id};

    SELECT
        p.entity_id
        , ci.id as city_id
        , ci.name as city_name
        , ci.updated_at
        , ST_UNION_AGG(z.shape) as city_shape
    FROM fulfillment-dwh-production.cl.countries co
    LEFT JOIN UNNEST(co.platforms) p
    LEFT JOIN UNNEST(co.cities) ci
    LEFT JOIN UNNEST(zones) z
    WHERE ci.is_active
    AND z.is_active
    AND p.entity_id IS NOT NULL
    AND p.entity_id = entity_id_filter
    AND ci.id = city_id_filter
    AND ci.shape IS NOT NULL
    GROUP BY 1,2,3,4
    QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id, city_id ORDER BY ci.updated_at DESC) = 1

    
    -- with load_latest_row_per_city_id AS (
    --     SELECT
    --     p.entity_id
    --     , ci.id as city_id
    --     , ci.name as city_name
    --     , ci.zones
    --     , ci.updated_at
    --     FROM fulfillment-dwh-production.cl.countries co
    --     LEFT JOIN UNNEST(co.platforms) p
    --     LEFT JOIN UNNEST(co.cities) ci
    --     WHERE ci.is_active
    --     AND p.entity_id IS NOT NULL
    --     AND p.entity_id = entity_filter
    --     AND ci.id = city_id_filter
    --     QUALIFY ROW_NUMBER() OVER(PARTITION BY p.entity_id, ci.id ORDER BY ci.updated_at DESC) = 1
    -- )

    -- , get_latest_row_per_city_name AS (
    --     SELECT *
    --     FROM load_latest_row_per_city_id
    --     QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id, city_name ORDER BY updated_at DESC) = 1

    -- )

    -- , load_latest_zone AS (

    -- SELECT ld.* EXCEPT(zones, updated_at)
    --     , z.id as zone_id
    --     , z.name as zone_name
    --     , z.shape as zone_shape
    --     , z.default_delivery_area_settings
    --     , z.delivery_types
    --     , z.fleet_id
    -- FROM get_latest_row_per_city_name ld
    -- LEFT JOIN UNNEST(zones) z
    -- WHERE TRUE
    -- AND z.shape IS NOT NULL 
    -- AND z.is_active
    -- QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id, city_id, z.id ORDER BY z.updated_at DESC) = 1
    -- )

    -- , aggregate_zone_shapes AS (
    --     SELECT 
    --     entity_id
    --     , city_id 
    --     , city_name 
    --     , ST_UNION_AGG(zone_shape) as city_shape
    --     FROM load_latest_zone
    --     GROUP BY 1,2,3
    -- )

    -- SELECT *
    -- FROM aggregate_zone_shapes