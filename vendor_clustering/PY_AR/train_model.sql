create or replace model
  `dh-logistics-product-ops.pricing.vendor_clustering_model_ar_food_afv_distance_custom_c5` 
  options (
    model_type = 'kmeans',
    num_clusters = 5,
    standardize_features = true,
    distance_type = 'COSINE',
    kmeans_init_method = 'CUSTOM',
    kmeans_init_col = 'init_col',
    max_iterations = 50) as
select
  *
from `dh-logistics-product-ops.pricing.vendor_clustering_model_ar_food_afv_distance_data`
