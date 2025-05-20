create or replace table `dh-logistics-product-ops.pricing.vendor_clusters_ar_food_afv_distance` as
select
  * except(nearest_centroids_distance)
from ml.predict(
    model `dh-logistics-product-ops.pricing.vendor_clustering_model_ar_food_afv_distance_custom_c5`,
    (select *
    from `dh-logistics-product-ops.pricing.vendor_clustering_model_ar_food_afv_distance_data`
    where not init_col)
)
order by centroid_id
