--Open it at https://github.com/deliveryhero/dwh-sql/blob/master/sqls/dwh_bl/tableau_monthly_report_city.sql

/*DROP TABLE dwh_bl.tableau_monthly_report_city;
CREATE TABLE IF NOT EXISTS dwh_bl.tableau_monthly_report_city
(
    source_id                               SMALLINT
    ,management_entity_group                VARCHAR(20)
    ,display_name                           VARCHAR(30)
    ,region                                 VARCHAR(10)
    ,country                                VARCHAR(30)
    ,report_month                           DATE
    ,city                                   VARCHAR(80)
    ,is_competitive                         BOOLEAN
    ,city_tier                              VARCHAR(5)
    ,rank_tier                              VARCHAR(5)
    ,city_rank                              SMALLINT
    ,acquisitions                           INT4
    ,acquisitions_yoy                       INT4
    ,acquisitions_mom                       INT4
    ,acquisitions_mom_yoy                   INT4
    ,customer_base                          INT4
    ,customer_base_yoy                      INT4
    ,customer_base_mom                      INT4
    ,orders                                 INT4
    ,orders_failed                          INT4
    ,top_10_brand_orders                    INT4
    ,own_delivery_orders                    INT4
    ,amt_gmv_eur_dh                         INT8
    ,amt_commission_eur_dh                  INT8
    ,amt_delivery_fee_eur                   INT4
    ,amt_cv_eur                             INT4
    ,amt_delivery_fee_eur_dh                INT4
    ,amt_cv_eur_dh                          INT4
    ,orders_yoy                             INT4
    ,orders_mom                             INT4
    ,orders_mom_yoy                         INT4
    ,own_delivery_orders_yoy                INT4
    ,own_delivery_orders_mom                INT4
    ,orders_failed_mom                      INT4
    ,orders_failed_yoy                      INT4
    ,gmv_eur                                INT4
    ,gmv_source                             INT4
    ,gmv_eur_yoy                            INT4
    ,gmv_eur_mom                            INT4
    ,commission_eur                         INT4
    ,commission_eur_yoy                     INT4
    ,commission_eur_mom                     INT4
    ,commission_eur_dh_yoy                  INT4
    ,commission_eur_dh_mom                  INT8
    ,gmv_eur_dh_yoy                         INT4
    ,gmv_eur_dh_mom                         INT8
    ,active_customers                       INT4
    ,active_customers_source                INT4
    ,active_customers_yoy                   INT4
    ,active_customers_mom                   INT4
    ,restaurants_new                        INT4
    ,restaurants_churned                    INT4
    ,restaurants_online                     INT4
    ,acquisitions_rr                        INT4
    ,returning_customers                    INT4
    ,restaurants_d_rating                   INT4
    ,pp_revenue                             INT4
    ,restaurants_online_zero_orders         INT4
    ,darkstore_online_od                    INT4
    ,darkstore_new_od                       INT4
    ,darkstore_churned_od                   INT4
    ,darkstore_online_zero_orders_od        INT4
    ,vertical_online_od                     INT4
    ,vertical_new_od                        INT4
    ,vertical_churned_od                    INT4
    ,vertical_online_zero_orders_od         INT4
    ,restaurant_online_od                   INT4
    ,restaurant_new_od                      INT4
    ,restaurant_churned_od                  INT4
    ,restaurant_online_zero_orders_od       INT4
    ,online_mp                              INT4
    ,new_mp                                 INT4
    ,churned_mp                             INT4
    ,online_zero_orders_mp                  INT4
    ,concept_online                         INT4
    ,concept_new                            INT4
    ,concept_churned                        INT4
    ,concept_online_zero_orders             INT4
    ,kitchen_restaurant_online              INT4
    ,kitchen_restaurant_new                 INT4
    ,kitchen_restaurant_churned             INT4
    ,kitchen_restaurant_online_zero_orders  INT4
    ,kitchen_online                         INT4
    ,kitchen_new                            INT4
    ,kitchen_churned                        INT4
    ,kitchen_online_zero_orders             INT4
    ,restaurants_recovered_churn            INT4
    ,restaurants_new_dh                     INT4
    ,restaurants_online_dh                  INT4
    ,restaurants_churned_dh                 INT4
    ,restaurants_online_zero_orders_dh      INT4
    ,fp_amt_premium_fee_eur                 INT4
    ,ban_amt_premium_fee_eur                INT4
    ,gmv_eur_kitchen                        INT4
    ,gmv_eur_concept                        INT4
    ,amt_commission_eur_kitchen             INT4
    ,amt_commission_eur_concept             INT4
    ,total_order_kitchen                    INT4
    ,total_order_concept                    INT4
    ,converted_rating_concept               INT4
    ,rated_order_count_concept              INT4
    ,converted_rating_kitchen               INT4
    ,rated_order_count_kitchen              INT4
    ,gmv_eur_od_darkstore                   INT4
    ,amt_commission_eur_od_darkstore        INT4
    ,successful_order_od_darkstore          INT4
    ,order_value_od_darkstore               INT4
    ,delivery_fee_od_darkstore              INT4
    ,converted_rating_od_darkstore          INT4
    ,rated_order_count_od_darkstore         INT4
    ,nps_ao_promoters_od_darkstore          INT4
    ,nps_ao_detractors_od_darkstore         INT4
    ,nps_ao_responses_od_darkstore          INT4
    ,failed_orders_od_darkstore             INT4
    ,acquisitions_od_darkstore              INT4
    ,voucher_order_od_darkstore             INT4
    ,customer_base_od_darkstore             INT4
    ,successful_orders_od_vertical          INT4
    ,gmv_od_vertical                        INT4
    ,order_value_od_vertical                INT4
    ,delivery_fee_od_vertical               INT4
    ,commission_od_vertical                 INT4
    ,converted_rating_od_vertical           INT4
    ,rated_order_count_od_vertical          INT4
    ,nps_ao_promoters_od_vertical           INT4
    ,nps_ao_detractors_od_vertical          INT4
    ,nps_ao_responses_od_vertical           INT4
    ,failed_orders_od_vertical              INT4
    ,acquisitions_od_vertical               INT4
    ,voucher_orders_od_vertical             INT4
    ,customer_base_od_vertical              INT4
    ,successful_orders_mp                   INT4
    ,gmv_mp                                 INT4
    ,order_value_mp                         INT4
    ,delivery_fee_mp                        INT4
    ,commission_mp                          INT4
    ,converted_rating_mp                    INT4
    ,rated_order_count_mp                   INT4
    ,nps_ao_promoters_mp                    INT4
    ,nps_ao_detractors_mp                   INT4
    ,nps_ao_responses_mp                    INT4
    ,failed_orders_mp                       INT4
    ,acquisitions_mp                        INT4
    ,voucher_orders_mp                      INT4
    ,customer_base_mp                       INT4
    ,successful_orders_od_restaurants       INT4
    ,gmv_od_restaurants                     INT8
    ,order_value_od_restaurants             INT8
    ,delivery_fee_od_restaurants            INT4
    ,commission_od_restaurants              INT8
    ,converted_rating_od_restaurants        INT4
    ,rated_order_count_od_restaurants       INT4
    ,nps_ao_promoters_od_restaurants        INT4
    ,nps_ao_detractors_od_restaurants       INT4
    ,nps_ao_responses_od_restaurants        INT4
    ,failed_orders_od_restaurants           INT4
    ,acquisitions_od_restaurants            INT4
    ,voucher_orders_od_restaurants          INT4
    ,customer_base_od_restaurants           INT4
    ,restaurants_online_yoy                 INT4
    ,restaurants_online_mom                 INT4
    ,acquisitions_rr_yoy                    INT4
    ,returning_customers_yoy                INT4
    ,pp_revenue_yoy                         INT4
    ,fp_amt_premium_fee_eur_yoy             INT4
    ,ban_amt_premium_fee_eur_yoy            INT4
    ,pp_revenue_mom                         INT4
    ,fp_amt_premium_fee_eur_mom             INT4
    ,ban_amt_premium_fee_eur_mom            INT4
    ,amt_delivery_fee_eur_yoy               INT4
    ,amt_delivery_fee_eur_mom               INT4
    ,amt_cv_eur_yoy                         INT4
    ,amt_cv_eur_mom                         INT4
    ,amt_delivery_fee_eur_dh_yoy            INT4
    ,amt_delivery_fee_eur_dh_mom            INT4
    ,amt_cv_eur_dh_yoy                      INT4
    ,amt_cv_eur_dh_mom                      INT4
    ,gmv_eur_kitchen_yoy                    INT4
    ,gmv_eur_kitchen_mom                    INT4
    ,gmv_eur_concept_yoy                    INT4
    ,gmv_eur_concept_mom                    INT4
    ,amt_commission_eur_kitchen_yoy         INT4
    ,amt_commission_eur_kitchen_mom         INT4
    ,amt_commission_eur_concept_yoy         INT4
    ,amt_commission_eur_concept_mom         INT4
    ,total_order_kitchen_yoy                INT4
    ,total_order_kitchen_mom                INT4
    ,total_order_concept_yoy                INT4
    ,total_order_concept_mom                INT4
    ,restaurants_new_dh_yoy                 INT4
    ,restaurants_new_dh_mom                 INT4
    ,restaurants_online_dh_yoy              INT4
    ,restaurants_online_dh_mom              INT4
    ,restaurants_churned_dh_yoy             INT4
    ,restaurants_churned_dh_mom             INT4
    ,restaurants_online_zero_orders_dh_yoy  INT4
    ,restaurants_online_zero_orders_dh_mom  INT4
    ,darkstore_online_od_yoy                INT4
    ,darkstore_online_od_mom                INT4
    ,darkstore_new_od_yoy                   INT4
    ,darkstore_new_od_mom                   INT4
    ,darkstore_churned_od_yoy               INT4
    ,darkstore_churned_od_mom               INT4
    ,darkstore_online_zero_orders_od_yoy    INT4
    ,darkstore_online_zero_orders_od_mom    INT4
    ,vertical_online_od_yoy                 INT4
    ,vertical_online_od_mom                 INT4
    ,vertical_new_od_yoy                    INT4
    ,vertical_new_od_mom                    INT4
    ,vertical_churned_od_yoy                INT4
    ,vertical_churned_od_mom                INT4
    ,vertical_online_zero_orders_od_yoy     INT4
    ,vertical_online_zero_orders_od_mom     INT4
    ,restaurant_online_od_yoy               INT4
    ,restaurant_online_od_mom               INT4
    ,restaurant_new_od_yoy                  INT4
    ,restaurant_new_od_mom                  INT4
    ,restaurant_churned_od_yoy              INT4
    ,restaurant_churned_od_mom              INT4
    ,restaurant_online_zero_orders_od_yoy   INT4
    ,restaurant_online_zero_orders_od_mom   INT4
    ,online_mp_yoy                          INT4
    ,online_mp_mom                          INT4
    ,new_mp_yoy                             INT4
    ,new_mp_mom                             INT4
    ,churned_mp_yoy                         INT4
    ,churned_mp_mom                         INT4
    ,online_zero_orders_mp_yoy              INT4
    ,online_zero_orders_mp_mom              INT4
    ,concept_online_yoy                     INT4
    ,concept_online_mom                     INT4
    ,concept_new_yoy                        INT4
    ,concept_new_mom                        INT4
    ,concept_churned_yoy                    INT4
    ,concept_churned_mom                    INT4
    ,concept_online_zero_orders_yoy         INT4
    ,concept_online_zero_orders_mom         INT4
    ,kitchen_restaurant_online_yoy          INT4
    ,kitchen_restaurant_online_mom          INT4
    ,kitchen_restaurant_new_yoy             INT4
    ,kitchen_restaurant_new_mom             INT4
    ,kitchen_restaurant_churned_yoy         INT4
    ,kitchen_restaurant_churned_mom         INT4
    ,kitchen_restaurant_online_zero_orders_yoy INT4
    ,kitchen_restaurant_online_zero_orders_mom INT4
    ,kitchen_online_yoy                     INT4
    ,kitchen_online_mom                     INT4
    ,kitchen_new_yoy                        INT4
    ,kitchen_new_mom                        INT4
    ,kitchen_churned_yoy                    INT4
    ,kitchen_churned_mom                    INT4
    ,kitchen_online_zero_orders_yoy         INT4
    ,kitchen_online_zero_orders_mom         INT4
    ,converted_rating_concept_yoy           INT4
    ,converted_rating_concept_mom           INT4
    ,rated_order_count_concept_yoy          INT4
    ,rated_order_count_concept_mom          INT4
    ,converted_rating_kitchen_yoy           INT4
    ,converted_rating_kitchen_mom           INT4
    ,rated_order_count_kitchen_yoy          INT4
    ,rated_order_count_kitchen_mom          INT4
    ,converted_rating_od_darkstore_yoy      INT4
    ,converted_rating_od_darkstore_mom      INT4
    ,rated_order_count_od_darkstore_yoy     INT4
    ,rated_order_count_od_darkstore_mom     INT4
    ,nps_ao_promoters_od_darkstore_yoy      INT4
    ,nps_ao_promoters_od_darkstore_mom      INT4
    ,nps_ao_detractors_od_darkstore_yoy     INT4
    ,nps_ao_detractors_od_darkstore_mom     INT4
    ,nps_ao_responses_od_darkstore_yoy      INT4
    ,nps_ao_responses_od_darkstore_mom      INT4
    ,converted_rating_od_vertical_yoy       INT4
    ,converted_rating_od_vertical_mom       INT4
    ,rated_order_count_od_vertical_yoy      INT4
    ,rated_order_count_od_vertical_mom      INT4
    ,nps_ao_promoters_od_vertical_yoy       INT4
    ,nps_ao_promoters_od_vertical_mom       INT4
    ,nps_ao_detractors_od_vertical_yoy      INT4
    ,nps_ao_detractors_od_vertical_mom      INT4
    ,nps_ao_responses_od_vertical_yoy       INT4
    ,nps_ao_responses_od_vertical_mom       INT4
    ,converted_rating_mp_yoy                INT4
    ,converted_rating_mp_mom                INT4
    ,rated_order_count_mp_yoy               INT4
    ,rated_order_count_mp_mom               INT4
    ,nps_ao_promoters_mp_yoy                INT4
    ,nps_ao_promoters_mp_mom                INT4
    ,nps_ao_detractors_mp_yoy               INT4
    ,nps_ao_detractors_mp_mom               INT4
    ,nps_ao_responses_mp_yoy                INT4
    ,nps_ao_responses_mp_mom                INT4
    ,converted_rating_od_restaurants_yoy    INT4
    ,converted_rating_od_restaurants_mom    INT4
    ,rated_order_count_od_restaurants_yoy   INT4
    ,rated_order_count_od_restaurants_mom   INT4
    ,nps_ao_promoters_od_restaurants_yoy    INT4
    ,nps_ao_promoters_od_restaurants_mom    INT4
    ,nps_ao_detractors_od_restaurants_yoy   INT4
    ,nps_ao_detractors_od_restaurants_mom   INT4
    ,nps_ao_responses_od_restaurants_yoy    INT4
    ,nps_ao_responses_od_restaurants_mom    INT4
    ,gmv_eur_od_darkstore_yoy               INT4
    ,gmv_eur_od_darkstore_mom               INT4
    ,amt_commission_eur_od_darkstore_yoy    INT4
    ,amt_commission_eur_od_darkstore_mom    INT4
    ,successful_order_od_darkstore_yoy      INT4
    ,successful_order_od_darkstore_mom      INT4
    ,order_value_od_darkstore_yoy           INT4
    ,order_value_od_darkstore_mom           INT4
    ,delivery_fee_od_darkstore_yoy          INT4
    ,delivery_fee_od_darkstore_mom          INT4
    ,failed_orders_od_darkstore_yoy         INT4
    ,failed_orders_od_darkstore_mom         INT4
    ,acquisitions_od_darkstore_yoy          INT4
    ,acquisitions_od_darkstore_mom          INT4
    ,voucher_order_od_darkstore_yoy         INT4
    ,voucher_order_od_darkstore_mom         INT4
    ,customer_base_od_darkstore_yoy         INT4
    ,customer_base_od_darkstore_mom         INT4
    ,successful_orders_od_vertical_yoy      INT4
    ,successful_orders_od_vertical_mom      INT4
    ,gmv_od_vertical_yoy                    INT4
    ,gmv_od_vertical_mom                    INT4
    ,amt_commission_eur_od_vertical_yoy     INT4
    ,amt_commission_eur_od_vertical_mom     INT4
    ,order_value_od_vertical_yoy            INT4
    ,order_value_od_vertical_mom            INT4
    ,delivery_fee_od_vertical_yoy           INT4
    ,delivery_fee_od_vertical_mom           INT4
    ,failed_orders_od_vertical_yoy          INT4
    ,failed_orders_od_vertical_mom          INT4
    ,acquisitions_od_vertical_yoy           INT4
    ,acquisitions_od_vertical_mom           INT4
    ,voucher_orders_od_vertical_yoy         INT4
    ,voucher_orders_od_vertical_mom         INT4
    ,customer_base_od_vertical_yoy          INT4
    ,customer_base_od_vertical_mom          INT4
    ,successful_orders_mp_yoy               INT4
    ,successful_orders_mp_mom               INT4
    ,gmv_mp_yoy                             INT4
    ,gmv_mp_mom                             INT4
    ,amt_commission_eur_mp_yoy              INT4
    ,amt_commission_eur_mp_mom              INT4
    ,order_value_mp_yoy                     INT4
    ,order_value_mp_mom                     INT4
    ,delivery_fee_mp_yoy                    INT4
    ,delivery_fee_mp_mom                    INT4
    ,failed_orders_mp_yoy                   INT4
    ,failed_orders_mp_mom                   INT4
    ,acquisitions_mp_yoy                    INT4
    ,acquisitions_mp_mom                    INT4
    ,voucher_orders_mp_yoy                  INT4
    ,voucher_orders_mp_mom                  INT4
    ,customer_base_mp_yoy                   INT4
    ,customer_base_mp_mom                   INT4
    ,successful_orders_od_restaurants_yoy   INT4
    ,successful_orders_od_restaurants_mom   INT4
    ,gmv_od_restaurants_yoy                 INT8
    ,gmv_od_restaurants_mom                 INT8
    ,amt_commission_eur_od_restaurants_yoy  INT4
    ,amt_commission_eur_od_restaurants_mom  INT4
    ,order_value_od_restaurants_yoy         INT8
    ,order_value_od_restaurants_mom         INT8
    ,delivery_fee_od_restaurants_yoy        INT4
    ,delivery_fee_od_restaurants_mom        INT4
    ,failed_orders_od_restaurants_yoy       INT4
    ,failed_orders_od_restaurants_mom       INT4
    ,acquisitions_od_restaurants_yoy        INT4
    ,acquisitions_od_restaurants_mom        INT4
    ,voucher_orders_od_restaurants_yoy      INT4
    ,voucher_orders_od_restaurants_mom      INT4
    ,customer_base_od_restaurants_yoy       INT4
    ,customer_base_od_restaurants_mom       INT4
    ,joker_revenue                          INT4
    ,joker_revenue_yoy                      INT4
    ,joker_revenue_mom                      INT4
    ,orders_failed_cancellation_yoy         INT4
    ,orders_failed_cancellation_mom         INT4
    ,orders_failed_response_yoy             INT4
    ,orders_failed_response_mom             INT4
    ,orders_failed_rejection_yoy            INT4
    ,orders_failed_rejection_mom            INT4
    ,orders_failed_transmission_yoy         INT4
    ,orders_failed_transmission_mom         INT4
    ,orders_failed_delivery_yoy             INT4
    ,orders_failed_delivery_mom             INT4
    ,orders_failed_payment_yoy              INT4
    ,orders_failed_payment_mom              INT4
    ,orders_failed_verification_yoy         INT4
    ,orders_failed_verification_mom         INT4
    ,orders_failed_other_yoy                INT4
    ,orders_failed_other_mom                INT4
    ,orders_delivery_20m_yoy                INT4
    ,orders_delivery_20m_mom                INT4
    ,orders_promised_20_yoy                 INT4
    ,orders_promised_20_mom                 INT4
    ,orders_promised_20_delivered_20_yoy    INT4
    ,orders_promised_20_delivered_20_mom    INT4
    ,groceries_gmv_yoy                      INT4
    ,groceries_gmv_mom                      INT4
    ,groceries_orders_yoy                   INT4
    ,groceries_orders_mom                   INT4
    ,discount_darkstore_yoy                 INT4
    ,discount_darkstore_mom                 INT4
    ,discount_od_vertical_yoy               INT4
    ,discount_od_vertical_mom               INT4
    ,discount_od_restaurant_yoy             INT4
    ,discount_od_restaurant_mom             INT4
    ,discount_mp_yoy                        INT4
    ,discount_mp_mom                        INT4
    ,delivery_distance_darkstore_yoy        INT4
    ,delivery_distance_darkstore_mom        INT4
    ,delivery_distance_od_vertical_yoy      INT4
    ,delivery_distance_od_vertical_mom      INT4
    ,delivery_distance_od_restaurant_yoy    INT4
    ,delivery_distance_od_restaurant_mom    INT4
    ,delivery_distance_mp_yoy               INT4
    ,delivery_distance_mp_mom               INT4
    ,delivery_time_darkstore_yoy            INT4
    ,delivery_time_darkstore_mom            INT4
    ,delivery_time_od_vertical_yoy          INT4
    ,delivery_time_od_vertical_mom          INT4
    ,delivery_time_od_restaurant_yoy        INT4
    ,delivery_time_od_restaurant_mom        INT4
    ,delivery_time_mp_yoy                   INT4
    ,delivery_time_mp_mom                   INT4
    ,orders_courier_late_10m_darkstore_yoy  INT4
    ,orders_courier_late_10m_darkstore_mom  INT4
    ,orders_courier_late_10m_vertical_yoy   INT4
    ,orders_courier_late_10m_vertical_mom    INT4
    ,orders_courier_late_10m_restaurant_yoy INT4
    ,orders_courier_late_10m_restaurant_mom INT4
    ,orders_courier_late_10m_mp_yoy         INT4
    ,orders_courier_late_10m_mp_mom         INT4
    ,orders_failed_cancellation             INT4
    ,orders_failed_response                 INT4
    ,orders_failed_rejection                INT4
    ,orders_failed_transmission             INT4
    ,orders_failed_delivery                 INT4
    ,orders_failed_payment                  INT4
    ,orders_failed_verification             INT4
    ,orders_failed_other                    INT4
    ,orders_failed_cancellation_dh          INT4
    ,orders_failed_response_dh              INT4
    ,orders_failed_rejection_dh             INT4
    ,orders_failed_transmission_dh          INT4
    ,orders_failed_delivery_dh              INT4
    ,orders_failed_payment_dh               INT4
    ,orders_failed_verification_dh          INT4
    ,orders_failed_other_dh                 INT4
    ,orders_delivery_20m                    INT4
    ,orders_promised_20                     INT4
    ,orders_promised_20_delivered_20        INT4
    ,groceries_orders                       INT4
    ,groceries_gmv                          INT4
    ,discount_darkstore                     INT4
    ,discount_od_vertical                   INT4
    ,discount_od_restaurant                 INT4
    ,discount_mp                            INT4
    ,delivery_distance_darkstore            INT4
    ,delivery_distance_od_vertical          INT4
    ,delivery_distance_od_restaurant        INT4
    ,delivery_distance_mp                   INT4
    ,delivery_time_darkstore                INT4
    ,delivery_time_od_vertical              INT4
    ,delivery_time_od_restaurant            INT4
    ,delivery_time_mp                       INT4
    ,orders_courier_late_10m_darkstore      INT4
    ,orders_courier_late_10m_vertical       INT4
    ,orders_courier_late_10m_restaurant     INT4
    ,orders_courier_late_10m_mp             INT4
    ,all_transactions                       INT4
    ,all_visits                             INT4
    ,all_transactions_yoy                   INT4
    ,all_visits_yoy                         INT4
    ,all_transactions_mom                   INT4
    ,all_visits_mom                         INT4
    ,all_visits_mp                          INT4
    ,all_transactions_mp                    INT4
    ,all_visits_darkstore                   INT4
    ,all_transactions_darkstore             INT4
    ,all_visits_od_vertical                 INT4
    ,all_transactions_od_vertical           INT4
    ,all_visits_od_restaurant               INT4
    ,all_transactions_od_restaurant         INT4
    ,all_visits_mp_yoy                      INT4
    ,all_visits_mp_mom                      INT4
    ,all_transactions_mp_yoy                INT4
    ,all_transactions_mp_mom                INT4
    ,all_visits_darkstore_yoy               INT4
    ,all_visits_darkstore_mom               INT4
    ,all_transactions_darkstore_yoy         INT4
    ,all_transactions_darkstore_mom         INT4
    ,all_visits_od_vertical_yoy             INT4
    ,all_visits_od_vertical_mom             INT4
    ,all_transactions_od_vertical_yoy       INT4
    ,all_transactions_od_vertical_mom       INT4
    ,all_visits_od_restaurant_yoy           INT4
    ,all_visits_od_restaurant_mom           INT4
    ,all_transactions_od_restaurant_yoy     INT4
    ,all_transactions_od_restaurant_mom     INT4
    ,customers_returned_od_darkstore        INT4
    ,customers_total_od_darkstore           INT4
    ,customers_returned_od_vertical         INT4
    ,customers_total_od_vertical            INT4
    ,customers_returned_mp                  INT4
    ,customers_total_mp                     INT4
    ,customers_returned_od_restaurant       INT4
    ,customers_total_od_restaurant          INT4
    ,customers_returned_od_darkstore_yoy    INT4
    ,customers_returned_od_darkstore_mom    INT4
    ,customers_total_od_darkstore_yoy       INT4
    ,customers_total_od_darkstore_mom       INT4
    ,customers_returned_od_vertical_yoy     INT4
    ,customers_returned_od_vertical_mom     INT4
    ,customers_total_od_vertical_yoy        INT4
    ,customers_total_od_vertical_mom        INT4
    ,customers_returned_mp_yoy              INT4
    ,customers_returned_mp_mom              INT4
    ,customers_total_mp_yoy                 INT4
    ,customers_total_mp_mom                 INT4
    ,customers_returned_od_restaurant_yoy   INT4
    ,customers_returned_od_restaurant_mom   INT4
    ,customers_total_od_restaurant_yoy      INT4
    ,customers_total_od_restaurant_mom      INT4
    ,population                             INT4
)
DISTSTYLE ALL
;
*/


--DROP TABLE IF EXISTS construct;
CREATE TEMPORARY TABLE construct
DISTKEY(report_month)
AS
WITH cities AS(
SELECT DISTINCT
    r.source_id,
    c.management_entity_group,
    c.display_name,
    c.region,
    c.common_name AS country,
    country_iso,
    is_active,
    LOWER(TRIM(city)) AS city
FROM dwh_bl.dim_restaurant as r
JOIN dwh_il.dim_countries as c
    ON c.source_id= r.source_id
    AND c.is_active
),

dates AS(
SELECT DISTINCT
    first_day_of_month AS report_month
FROM dwh_il.dim_date
    WHERE first_day_of_month < DATE_TRUNC('MONTH', CURRENT_DATE)
    AND first_day_of_month > DATEADD('MONTH',-25, DATE_TRUNC('MONTH', CURRENT_DATE))
)
SELECT
    *
FROM cities AS c
CROSS JOIN dates AS d;


--DROP TABLE IF EXISTS monthly_online_restaurants_agg;
CREATE TEMPORARY TABLE monthly_online_restaurants_agg
DISTKEY(report_month)
AS
SELECT
    m.source_id,
    m.report_month,
    LOWER(TRIM(r.city)) AS city,
    SUM( m.is_contracted_online::INT ) AS restaurants_online,
    SUM( m.new_online::INT * m.is_contracted::INT ) AS restaurants_new,
    SUM( m.churned::INT * m.is_contracted::INT ) AS restaurants_churned,
    SUM(CASE WHEN m.successful_orders = 0 OR m.successful_orders IS NULL THEN m.is_contracted_online::INT END) AS restaurants_online_zero_orders,

    ---darkstore
    SUM(CASE WHEN r.shop_type = 'darkstores' THEN m.is_contracted_online::INT * r.is_dh_delivery::INT END) AS darkstore_online_od,
    SUM(CASE WHEN r.shop_type = 'darkstores' THEN m.new_online::INT * m.is_contracted::INT * r.is_dh_delivery::INT END) AS darkstore_new_od,
    SUM(CASE WHEN r.shop_type = 'darkstores' THEN m.churned::INT * m.is_contracted::INT * r.is_dh_delivery::INT END) AS darkstore_churned_od,
    SUM(CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_dh_delivery AND r.shop_type = 'darkstores' THEN m.is_contracted_online::INT END) AS darkstore_online_zero_orders_od,

    --- vertical
    SUM( CASE WHEN m.is_contracted_online AND r.is_dh_delivery AND r.is_shop AND r.shop_type <> 'darkstores' THEN 1 END) AS vertical_online_od,
    SUM( CASE WHEN m.new_online AND m.is_contracted AND r.is_dh_delivery AND r.is_shop AND r.shop_type <> 'darkstores' THEN 1 END) AS vertical_new_od,
    SUM( CASE WHEN m.churned AND m.is_contracted AND r.is_dh_delivery AND r.is_shop AND r.shop_type <> 'darkstores' THEN 1 END) AS vertical_churned_od,
    SUM( CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_dh_delivery AND r.is_shop AND r.shop_type <> 'darkstores' THEN m.is_contracted_online::INT END) AS vertical_online_zero_orders_od,

    --- OD restaurants
    SUM( CASE WHEN m.is_contracted_online AND r.is_dh_delivery AND r.is_shop IS FALSE THEN 1 END) AS restaurant_online_od,
    SUM( CASE WHEN m.new_online AND m.is_contracted AND r.is_dh_delivery AND r.is_shop IS FALSE THEN 1 END) AS restaurant_new_od,
    SUM( CASE WHEN m.churned AND m.is_contracted AND r.is_dh_delivery AND r.is_shop IS FALSE THEN 1 END) AS restaurant_churned_od,
    SUM( CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_dh_delivery AND r.is_shop IS FALSE THEN m.is_contracted_online::INT END) AS restaurant_online_zero_orders_od,

    --- MP
    SUM( CASE WHEN m.is_contracted_online AND r.is_dh_delivery IS FALSE THEN 1 END) AS online_mp,
    SUM( CASE WHEN m.new_online AND m.is_contracted AND r.is_dh_delivery IS FALSE THEN 1 END) AS new_mp,
    SUM( CASE WHEN m.churned AND m.is_contracted AND r.is_dh_delivery IS FALSE THEN 1 END) AS churned_mp,
    SUM( CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_dh_delivery IS FALSE THEN m.is_contracted_online::INT END) AS online_zero_orders_mp,

    ---concept
    SUM(CASE WHEN r.is_concept AND r.is_kitchen IS FALSE THEN m.is_contracted_online::INT END) AS concept_online,
    SUM(CASE WHEN r.is_concept AND r.is_kitchen IS FALSE THEN m.new_online::INT * m.is_contracted::INT END) AS concept_new,
    SUM(CASE WHEN r.is_concept AND r.is_kitchen IS FALSE THEN m.churned::INT * m.is_contracted::INT END) AS concept_churned,
    SUM(CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_concept AND r.is_kitchen IS FALSE THEN m.is_contracted_online::INT END) AS concept_online_zero_orders,

    ---kitchen_restaurant
    SUM(CASE WHEN r.is_kitchen THEN m.is_contracted_online::INT END) AS kitchen_restaurant_online,
    SUM(CASE WHEN r.is_kitchen THEN m.new_online::INT * m.is_contracted::INT END) AS kitchen_restaurant_new,
    SUM(CASE WHEN r.is_kitchen THEN m.churned::INT * m.is_contracted::INT END) AS kitchen_restaurant_churned,
    SUM(CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_kitchen THEN m.is_contracted_online::INT END) AS kitchen_restaurant_online_zero_orders,

    ---kitchen_location
    COUNT(DISTINCT(CASE WHEN r.is_kitchen AND m.is_contracted_online THEN r.kitchen_id END)) AS kitchen_online,
    COUNT(DISTINCT(CASE WHEN r.is_kitchen AND m.new_online AND m.is_contracted THEN r.kitchen_id END)) AS kitchen_new,
    COUNT(DISTINCT(CASE WHEN r.is_kitchen AND m.churned AND m.is_contracted THEN r.kitchen_id END)) AS kitchen_churned,
    COUNT(DISTINCT(CASE WHEN (m.successful_orders = 0 OR m.successful_orders IS NULL) AND r.is_kitchen AND m.is_contracted_online THEN r.kitchen_id END)) AS kitchen_online_zero_orders,

    SUM(CASE WHEN m.kill_rate_category = 'D' THEN m.is_contracted_online::INT END) AS restaurants_d_rating,

    SUM( m.acquisition_customers_for_rr ) AS acquisitions_rr,
    SUM( m.returning_customers_0_30d ) AS returning_customers,
    SUM( m.recovered_churn::INT * m.is_contracted::INT ) AS restaurants_recovered_churn,
    SUM(CASE WHEN m.is_dh_delivery IS TRUE THEN m.new_online::INT * m.is_contracted::INT END) AS restaurants_new_dh,
    SUM(CASE WHEN m.is_dh_delivery IS TRUE THEN m.is_online::INT * m.is_contracted::INT END) AS restaurants_online_dh,
    SUM(CASE WHEN m.is_dh_delivery THEN m.churned::INT * m.is_contracted::INT END) AS restaurants_churned_dh,
    SUM(CASE WHEN m.is_dh_delivery AND (m.successful_orders = 0 OR m.successful_orders IS NULL) THEN m.is_contracted_online::INT END) AS restaurants_online_zero_orders_dh,

    SUM( m.amt_premium_fee_eur ) AS pp_revenue,
    SUM( m.amt_fp_fee_eur ) AS fp_amt_premium_fee_eur,
    SUM( m.amt_banner_fee_eur ) AS ban_amt_premium_fee_eur,

    SUM(m.amt_delivery_fee_eur) AS amt_delivery_fee_eur,
    SUM(m.amt_cv_eur) AS amt_cv_eur,

    SUM(CASE WHEN m.is_dh_delivery IS TRUE THEN m.amt_delivery_fee_eur END) AS amt_delivery_fee_eur_dh,
    SUM(CASE WHEN m.is_dh_delivery IS TRUE THEN m.amt_cv_eur END) AS amt_cv_eur_dh,

    ---concept
    SUM(CASE WHEN r.is_concept THEN m.converted_rating_total end) AS converted_rating_concept,
    SUM(CASE WHEN r.is_concept THEN m.rated_order_count end) AS rated_order_count_concept,

    ---kitchen
    SUM(CASE WHEN r.is_kitchen THEN m.converted_rating_total end) AS converted_rating_kitchen,
    SUM(CASE WHEN r.is_kitchen THEN m.rated_order_count end) AS rated_order_count_kitchen,

    ---darkstore
    SUM(CASE WHEN r.shop_type = 'darkstores' AND r.is_dh_delivery THEN m.converted_rating_total end) AS converted_rating_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND r.is_dh_delivery THEN m.rated_order_count end) AS rated_order_count_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_promoters end) AS nps_ao_promoters_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_detractors end) AS nps_ao_detractors_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_responses end) AS nps_ao_responses_od_darkstore,

    --verticals
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND r.is_dh_delivery THEN m.converted_rating_total END) AS converted_rating_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND r.is_dh_delivery THEN m.rated_order_count END) AS rated_order_count_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_promoters END) AS nps_ao_promoters_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_detractors END) AS nps_ao_detractors_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND r.is_dh_delivery THEN m.nps_ao_responses END) AS nps_ao_responses_od_vertical,

    ---MP
    SUM(CASE WHEN r.is_dh_delivery IS FALSE THEN m.converted_rating_total END) AS converted_rating_mp,
    SUM(CASE WHEN r.is_dh_delivery IS FALSE THEN m.rated_order_count END) AS rated_order_count_mp,
    SUM(CASE WHEN r.is_dh_delivery IS FALSE THEN m.nps_ao_promoters END) AS nps_ao_promoters_mp,
    SUM(CASE WHEN r.is_dh_delivery IS FALSE THEN m.nps_ao_detractors END) AS nps_ao_detractors_mp,
    SUM(CASE WHEN r.is_dh_delivery IS FALSE THEN m.nps_ao_responses END) AS nps_ao_responses_mp,

    --- od restaurants
    SUM(CASE WHEN is_shop IS FALSE AND r.is_dh_delivery THEN m.converted_rating_total END) AS converted_rating_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND r.is_dh_delivery THEN m.rated_order_count END) AS rated_order_count_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND r.is_dh_delivery THEN m.nps_ao_promoters END) AS nps_ao_promoters_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND r.is_dh_delivery THEN m.nps_ao_detractors END) AS nps_ao_detractors_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND r.is_dh_delivery THEN m.nps_ao_responses END) AS nps_ao_responses_od_restaurants

FROM dwh_bl.restaurant_monthly_kpis m
LEFT JOIN dwh_bl.dim_restaurant r USING (source_id, restaurant_id)
JOIN dwh_bl.dim_countries_main_source s on m.source_id = s.source_id  --Drops secondary sources, e.g. Pedidosya
WHERE m.source_id NOT IN (5, 18, 86, 123)  --Subway WL, Panama PY, Burger King WL, NPOT
AND (r.is_test_restaurant IS FALSE OR r.is_test_restaurant IS NULL)
AND r.is_crosslisting IS FALSE
GROUP BY 1, 2, 3;


--DROP TABLE IF EXISTS online_restaurants_mom_yoy;
CREATE TEMPORARY TABLE online_restaurants_mom_yoy
DISTKEY(report_month)
AS
SELECT
    source_id,
    city,
    report_month,
    lag(restaurants_online,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS restaurants_online_yoy,
    lag(restaurants_online,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS restaurants_online_mom,
    lag(acquisitions_rr,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_rr_yoy,
    lag(returning_customers,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS returning_customers_yoy,
    lag(pp_revenue,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS pp_revenue_yoy,
    lag(pp_revenue,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS pp_revenue_mom,
    lag(fp_amt_premium_fee_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS fp_amt_premium_fee_eur_yoy,
    lag(fp_amt_premium_fee_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS fp_amt_premium_fee_eur_mom,
    lag(ban_amt_premium_fee_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS ban_amt_premium_fee_eur_yoy,
    lag(ban_amt_premium_fee_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS ban_amt_premium_fee_eur_mom,
    lag(amt_delivery_fee_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_delivery_fee_eur_yoy,
    lag(amt_delivery_fee_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_delivery_fee_eur_mom,
    lag(amt_cv_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_cv_eur_yoy,
    lag(amt_cv_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_cv_eur_mom,
    lag(amt_delivery_fee_eur_dh,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_delivery_fee_eur_dh_yoy,
    lag(amt_delivery_fee_eur_dh,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_delivery_fee_eur_dh_mom,
    lag(amt_cv_eur_dh,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_cv_eur_dh_yoy,
    lag(amt_cv_eur_dh,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_cv_eur_dh_mom,

    lag(restaurants_new_dh,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_new_dh_yoy,
    lag(restaurants_new_dh,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_new_dh_mom,
    lag(restaurants_online_dh,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_online_dh_yoy,
    lag(restaurants_online_dh,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_online_dh_mom,
    lag(restaurants_churned_dh,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_churned_dh_yoy,
    lag(restaurants_churned_dh,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_churned_dh_mom,
    lag(restaurants_online_zero_orders_dh,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_online_zero_orders_dh_yoy,
    lag(restaurants_online_zero_orders_dh,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurants_online_zero_orders_dh_mom,
    lag(darkstore_online_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_online_od_yoy,
    lag(darkstore_online_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_online_od_mom,
    lag(darkstore_new_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_new_od_yoy,
    lag(darkstore_new_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_new_od_mom,
    lag(darkstore_churned_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_churned_od_yoy,
    lag(darkstore_churned_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_churned_od_mom,
    lag(darkstore_online_zero_orders_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_online_zero_orders_od_yoy,
    lag(darkstore_online_zero_orders_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS darkstore_online_zero_orders_od_mom,
    lag(vertical_online_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_online_od_yoy,
    lag(vertical_online_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_online_od_mom,
    lag(vertical_new_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_new_od_yoy,
    lag(vertical_new_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_new_od_mom,
    lag(vertical_churned_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_churned_od_yoy,
    lag(vertical_churned_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_churned_od_mom,
    lag(vertical_online_zero_orders_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_online_zero_orders_od_yoy,
    lag(vertical_online_zero_orders_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS vertical_online_zero_orders_od_mom,
    lag(restaurant_online_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_online_od_yoy,
    lag(restaurant_online_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_online_od_mom,
    lag(restaurant_new_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_new_od_yoy,
    lag(restaurant_new_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_new_od_mom,
    lag(restaurant_churned_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_churned_od_yoy,
    lag(restaurant_churned_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_churned_od_mom,
    lag(restaurant_online_zero_orders_od,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_online_zero_orders_od_yoy,
    lag(restaurant_online_zero_orders_od,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS restaurant_online_zero_orders_od_mom,
    lag(online_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS online_mp_yoy,
    lag(online_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS online_mp_mom,
    lag(new_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS new_mp_yoy,
    lag(new_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS new_mp_mom,
    lag(churned_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS churned_mp_yoy,
    lag(churned_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS churned_mp_mom,
    lag(online_zero_orders_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS online_zero_orders_mp_yoy,
    lag(online_zero_orders_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS online_zero_orders_mp_mom,

    lag(concept_online,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_online_yoy,
    lag(concept_online,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_online_mom,
    lag(concept_new,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_new_yoy,
    lag(concept_new,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_new_mom,
    lag(concept_churned,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_churned_yoy,
    lag(concept_churned,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_churned_mom,
    lag(concept_online_zero_orders,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_online_zero_orders_yoy,
    lag(concept_online_zero_orders,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS concept_online_zero_orders_mom,

    lag(kitchen_restaurant_online,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_online_yoy,
    lag(kitchen_restaurant_online,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_online_mom,
    lag(kitchen_restaurant_new,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_new_yoy,
    lag(kitchen_restaurant_new,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_new_mom,
    lag(kitchen_restaurant_churned,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_churned_yoy,
    lag(kitchen_restaurant_churned,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_churned_mom,
    lag(kitchen_restaurant_online_zero_orders,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_online_zero_orders_yoy,
    lag(kitchen_restaurant_online_zero_orders,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_restaurant_online_zero_orders_mom,

    lag(kitchen_online,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_online_yoy,
    lag(kitchen_online,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_online_mom,
    lag(kitchen_new,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_new_yoy,
    lag(kitchen_new,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_new_mom,
    lag(kitchen_churned,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_churned_yoy,
    lag(kitchen_churned,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_churned_mom,
    lag(kitchen_online_zero_orders,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_online_zero_orders_yoy,
    lag(kitchen_online_zero_orders,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS kitchen_online_zero_orders_mom,

    lag(converted_rating_concept,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_concept_yoy,
    lag(converted_rating_concept,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_concept_mom,
    lag(rated_order_count_concept,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_concept_yoy,
    lag(rated_order_count_concept,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_concept_mom,

    lag(converted_rating_kitchen,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_kitchen_yoy,
    lag(converted_rating_kitchen,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_kitchen_mom,
    lag(rated_order_count_kitchen,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_kitchen_yoy,
    lag(rated_order_count_kitchen,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_kitchen_mom,

    lag(converted_rating_od_darkstore,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_darkstore_yoy,
    lag(converted_rating_od_darkstore,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_darkstore_mom,
    lag(rated_order_count_od_darkstore,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_darkstore_yoy,
    lag(rated_order_count_od_darkstore,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_darkstore_mom,
    lag(nps_ao_promoters_od_darkstore,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_darkstore_yoy,
    lag(nps_ao_promoters_od_darkstore,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_darkstore_mom,
    lag(nps_ao_detractors_od_darkstore,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_darkstore_yoy,
    lag(nps_ao_detractors_od_darkstore,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_darkstore_mom,
    lag(nps_ao_responses_od_darkstore,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_darkstore_yoy,
    lag(nps_ao_responses_od_darkstore,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_darkstore_mom,
    lag(converted_rating_od_vertical,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_vertical_yoy,
    lag(converted_rating_od_vertical,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_vertical_mom,
    lag(rated_order_count_od_vertical,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_vertical_yoy,
    lag(rated_order_count_od_vertical,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_vertical_mom,
    lag(nps_ao_promoters_od_vertical,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_vertical_yoy,
    lag(nps_ao_promoters_od_vertical,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_vertical_mom,
    lag(nps_ao_detractors_od_vertical,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_vertical_yoy,
    lag(nps_ao_detractors_od_vertical,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_vertical_mom,
    lag(nps_ao_responses_od_vertical,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_vertical_yoy,
    lag(nps_ao_responses_od_vertical,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_vertical_mom,
    lag(converted_rating_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_mp_yoy,
    lag(converted_rating_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_mp_mom,
    lag(rated_order_count_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_mp_yoy,
    lag(rated_order_count_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_mp_mom,
    lag(nps_ao_promoters_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_mp_yoy,
    lag(nps_ao_promoters_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_mp_mom,
    lag(nps_ao_detractors_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_mp_yoy,
    lag(nps_ao_detractors_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_mp_mom,
    lag(nps_ao_responses_mp,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_mp_yoy,
    lag(nps_ao_responses_mp,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_mp_mom,
    lag(converted_rating_od_restaurants,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_restaurants_yoy,
    lag(converted_rating_od_restaurants,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS converted_rating_od_restaurants_mom,
    lag(rated_order_count_od_restaurants,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_restaurants_yoy,
    lag(rated_order_count_od_restaurants,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS rated_order_count_od_restaurants_mom,
    lag(nps_ao_promoters_od_restaurants,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_restaurants_yoy,
    lag(nps_ao_promoters_od_restaurants,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_promoters_od_restaurants_mom,
    lag(nps_ao_detractors_od_restaurants,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_restaurants_yoy,
    lag(nps_ao_detractors_od_restaurants,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_detractors_od_restaurants_mom,
    lag(nps_ao_responses_od_restaurants,12) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_restaurants_yoy,
    lag(nps_ao_responses_od_restaurants,1) OVER (PARTITION BY source_id,city ORDER BY report_month) AS nps_ao_responses_od_restaurants_mom

FROM monthly_online_restaurants_agg
;

--DROP TABLE IF EXISTS orders_cumulative;
CREATE TEMPORARY TABLE orders_cumulative
DISTKEY(report_month)
AS
WITH orders AS (
   SELECT
        o.source_id,
        LOWER(TRIM(o.city)) AS city,
        DATE_TRUNC('MONTH', o.date) AS report_month,
        SUM(o.acquisitions) AS acquisitions,
        SUM(o.orders) AS orders,
        SUM(o.gmv_eur) AS gmv_eur,
        SUM(o.orders_failed) AS orders_failed,
        SUM(o.commission_eur) AS commission_eur,
        SUM(o.active_customers) AS active_customers
    FROM dwh_bl.city_orders o
    --LEFT JOIN dwh_il.dim_city_mapping cm ON LOWER(TRIM(o.city)) = LOWER(TRIM(cm.city)) AND o.source_id = cm.source_id
    WHERE o.date < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1, 2, 3
),
city_start_month AS
(
    SELECT
        source_id,
        city,
        MIN(report_month) AS start_month
    FROM orders
    GROUP BY 1, 2
),
city_month_construct AS
( --This contruct is needed so to not drop cumulative acquisitions from cities that no-longer get orders, e.g. "Unknown" in CD GR
    SELECT DISTINCT
        c.source_id,
        c.city,
        d.first_day_of_month AS report_month
    FROM dwh_il.dim_date d
    CROSS JOIN city_start_month c
    WHERE d.first_day_of_month BETWEEN c.start_month AND DATE_TRUNC('month', CURRENT_DATE)-1
),
cumulative AS
(
    SELECT
        c.source_id,
        c.city,
        c.report_month,
        o.acquisitions,
        SUM(o.acquisitions) OVER (PARTITION BY c.source_id, c.city ORDER BY c.report_month ROWS UNBOUNDED PRECEDING) AS acquisitions_cumulative,
        SUM(o.active_customers) OVER (PARTITION BY c.source_id, c.report_month) AS active_customers_source,
        o.orders,
        o.orders_failed,
        o.gmv_eur,
        SUM(o.gmv_eur) OVER (PARTITION BY c.source_id, c.report_month) AS gmv_source,
        o.commission_eur,
        o.active_customers
    FROM city_month_construct c
    LEFT JOIN orders o USING (source_id, city, report_month)
)
SELECT
    source_id,
    city,
    report_month,
    acquisitions,
    acquisitions_cumulative,
    active_customers_source,
    orders,
    orders_failed,
    gmv_eur,
    gmv_source,
    commission_eur,
    active_customers
FROM cumulative
WHERE report_month >= ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE), -25)
;


--DROP TABLE IF EXISTS orders_cumulative_mom_yoy;
CREATE TEMPORARY TABLE orders_cumulative_mom_yoy
DISTKEY(report_month)
AS
SELECT
    source_id,
    city,
    report_month,
    lag(acquisitions_cumulative,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customer_base_yoy,
    lag(acquisitions_cumulative,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customer_base_mom,
    lag(acquisitions,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mom,
    lag(acquisitions,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_yoy,
    lag(acquisitions,13) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mom_yoy,
    lag(orders,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_mom,
    lag(orders,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_yoy,
    lag(orders,13) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_mom_yoy,
    lag(active_customers,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS active_customers_yoy,
    lag(active_customers,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS active_customers_mom,
    lag(orders_failed,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_yoy,
    lag(orders_failed,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_mom,
    lag(gmv_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_yoy,
    lag(gmv_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_mom,
    lag(commission_eur,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS commission_eur_yoy,
    lag(commission_eur,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS commission_eur_mom

FROM orders_cumulative
;


--DROP TABLE IF EXISTS top_10_brands;
CREATE TEMP TABLE top_10_brands
DISTKEY(report_month)
AS
WITH brand_orders AS (
    SELECT
        k.source_id,
        k.report_month,
        r.brand_name,
        LOWER(TRIM(r.city)) AS city,
        SUM(k.successful_orders) AS successful_orders
    FROM dwh_bl.dim_restaurant r
    JOIN dwh_bl.restaurant_monthly_kpis k USING (source_id, restaurant_id)
    --LEFT JOIN dwh_il.dim_city_mapping cm ON LOWER(TRIM(r.city)) = LOWER(TRIM(cm.city)) AND r.source_id = cm.source_id
    WHERE (r.is_test_restaurant IS FALSE OR r.is_test_restaurant IS NULL)
    AND r.source_id NOT IN (5, 18, 86, 123)
    GROUP BY 1 ,2, 3, 4
),
ranked_brands AS (
    SELECT
        b.*,
        RANK() OVER (PARTITION BY source_id, report_month, city ORDER BY successful_orders DESC NULLS LAST) AS order_rank
    FROM brand_orders b
    WHERE successful_orders > 0
)
SELECT
    source_id,
    report_month,
    city,
    SUM(successful_orders) AS successful_orders
FROM ranked_brands
WHERE order_rank <= 10
GROUP BY 1, 2, 3
;


--DROP TABLE IF EXISTS joker;
CREATE TEMPORARY TABLE joker
DISTSTYLE ALL
AS
SELECT
    a.source_id,
    DATE_TRUNC('month', a.order_date)::DATE AS report_month,
    COALESCE(LOWER(TRIM(r.city)),'unknown') AS city,
    SUM(CASE WHEN is_joker IS TRUE AND a.is_sent IS TRUE THEN amt_joker_eur ELSE 0 END) AS amt_joker_eur_total,

    SUM(CASE WHEN os.order_status = 'failed_cancellation' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_cancellation,
    SUM(CASE WHEN os.order_status = 'failed_response' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_response, -- failed_verification and failed_delivery are falling into orders_failed_other
    SUM(CASE WHEN os.order_status = 'failed_rejection' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_rejection,
    SUM(CASE WHEN os.order_status = 'failed_transmission' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_transmission,
    SUM(CASE WHEN os.order_status = 'failed_delivery' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_delivery,
    SUM(CASE WHEN os.order_status = 'failed_overall' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_overall,
    SUM(CASE WHEN os.order_status = 'failed_payment' AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_payment,
    SUM(CASE WHEN os.order_status = 'failed_verification'  AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_verification,
    SUM(CASE WHEN os.order_status NOT IN ('sent', 'failed_cancellation', 'failed_response', 'failed_rejection', 'failed_transmission', 'failed_delivery', 'failed_overall', 'failed_payment', 'failed_verification')
        AND a.is_click_to_call IS FALSE THEN a.order_qty ELSE 0 END) AS orders_failed_other,

    SUM(CASE WHEN os.order_status = 'failed_cancellation' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_cancellation_dh,
    SUM(CASE WHEN os.order_status = 'failed_response' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_response_dh, -- failed_verification and failed_delivery are falling into orders_failed_other
    SUM(CASE WHEN os.order_status = 'failed_rejection' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_rejection_dh,
    SUM(CASE WHEN os.order_status = 'failed_transmission' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_transmission_dh,
    SUM(CASE WHEN os.order_status = 'failed_delivery' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_delivery_dh,
    SUM(CASE WHEN os.order_status = 'failed_overall' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_overall_dh,
    SUM(CASE WHEN os.order_status = 'failed_payment' AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_payment_dh,
    SUM(CASE WHEN os.order_status = 'failed_verification'  AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_verification_dh,
    SUM(CASE WHEN os.order_status NOT IN ('sent', 'failed_cancellation', 'failed_response', 'failed_rejection', 'failed_transmission', 'failed_delivery', 'failed_overall', 'failed_payment', 'failed_verification')
        AND a.is_click_to_call IS FALSE AND a.is_dh_delivery IS TRUE THEN a.order_qty ELSE 0 END) AS orders_failed_other_dh,

    SUM(CASE WHEN a.is_shopping AND a.is_sent THEN a.order_qty END) AS groceries_orders,
    SUM(CASE WHEN a.is_shopping AND a.is_sent THEN a.amt_gmv_eur END) AS groceries_gmv,

    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery THEN a.amt_discount_dh_eur + a.amt_voucher_dh_eur END) AS discount_darkstore, --coalesce
    SUM(CASE WHEN r.is_shop AND a.is_dh_delivery AND r.shop_type <> 'darkstores' THEN a.amt_discount_dh_eur + a.amt_voucher_dh_eur END) AS discount_od_vertical,
    SUM(CASE WHEN r.is_shop IS FALSE AND a.is_dh_delivery THEN a.amt_discount_dh_eur + a.amt_voucher_dh_eur END) AS discount_od_restaurant,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE THEN a.amt_discount_dh_eur + a.amt_voucher_dh_eur END) AS discount_mp,

    ---kitchen & concept
    SUM(CASE WHEN r.is_kitchen THEN a.amt_gmv_eur end) AS gmv_eur_kitchen,
    SUM(CASE WHEN r.is_concept AND is_kitchen IS FALSE THEN a.amt_gmv_eur end) AS gmv_eur_concept,
    SUM(CASE WHEN r.is_kitchen THEN a.amt_commission_eur end) AS amt_commission_eur_kitchen,
    SUM(CASE WHEN r.is_concept AND is_kitchen IS FALSE THEN a.amt_commission_eur end) AS amt_commission_eur_concept,
    SUM(CASE WHEN r.is_kitchen THEN a.order_qty end) AS total_order_kitchen,
    SUM(CASE WHEN r.is_concept AND is_kitchen IS FALSE THEN a.order_qty end) AS total_order_concept,

    ---darkstore
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_gmv_eur end) AS gmv_eur_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_commission_eur end) AS amt_commission_eur_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.order_qty end) AS successful_order_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_cv_eur end) AS order_value_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_delivery_fee_eur end) AS delivery_fee_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent IS FALSE THEN a.order_qty end) AS failed_orders_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_acquisition THEN 1 ELSE 0 END) AS acquisitions_od_darkstore,
    SUM(CASE WHEN r.shop_type = 'darkstores' AND a.is_dh_delivery AND a.is_sent AND a.is_voucher THEN a.order_qty end) AS voucher_order_od_darkstore,

    --od verticals
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.order_qty END) AS successful_orders_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_gmv_eur END) AS gmv_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_cv_eur END) AS order_value_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_delivery_fee_eur END) AS delivery_fee_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent THEN a.amt_commission_eur END) AS commission_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent IS FALSE THEN a.order_qty END) AS failed_orders_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_acquisition THEN 1 ELSE 0 END) AS acquisitions_od_vertical,
    SUM(CASE WHEN r.is_shop AND r.shop_type <> 'darkstores' AND a.is_dh_delivery AND a.is_sent AND a.is_voucher THEN a.order_qty END) AS voucher_orders_od_vertical,

    ---MP
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent THEN a.order_qty END) AS successful_orders_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent THEN a.amt_gmv_eur END) AS gmv_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent THEN a.amt_cv_eur END) AS order_value_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent THEN a.amt_delivery_fee_eur END) AS delivery_fee_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent THEN a.amt_commission_eur END) AS commission_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent IS FALSE THEN a.order_qty END) AS failed_orders_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_acquisition THEN 1 ELSE 0 END) AS acquisitions_mp,
    SUM(CASE WHEN a.is_dh_delivery IS FALSE AND a.is_sent AND a.is_voucher THEN a.order_qty END) AS voucher_orders_mp,

    --- od restaurants
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent THEN a.order_qty END) AS successful_orders_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent THEN a.amt_gmv_eur END) AS gmv_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent THEN a.amt_cv_eur END) AS order_value_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent THEN a.amt_delivery_fee_eur END) AS delivery_fee_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent THEN a.amt_commission_eur END) AS commission_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent IS FALSE THEN a.order_qty END) AS failed_orders_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_acquisition THEN 1 ELSE 0 END) AS acquisitions_od_restaurants,
    SUM(CASE WHEN is_shop IS FALSE AND a.is_dh_delivery AND a.is_sent AND a.is_voucher THEN a.order_qty END) AS voucher_orders_od_restaurants,

    --- NEW FIELD
    SUM(CASE WHEN a.is_dh_delivery AND a.is_sent THEN a.order_qty END) AS own_delivery_orders,
    SUM(CASE WHEN a.is_dh_delivery AND a.is_sent THEN a.amt_gmv_eur END) AS amt_gmv_eur_dh,
    SUM(CASE WHEN a.is_dh_delivery AND a.is_sent THEN a.amt_commission_eur END) AS amt_commission_eur_dh

--FROM (SELECT * FROM dwh_il.ranked_fct_order WHERE order_date > '2019-01-01') AS a
FROM dwh_il.ranked_fct_order AS a
LEFT JOIN dwh_bl.dim_restaurant r USING (source_id, restaurant_id)
LEFT JOIN dwh_il.dim_order_status AS os
    ON os.source_id = a.source_id
    AND os.order_status_id = a.order_status_id
--    AND is_click_to_call IS FALSE
JOIN dwh_il.dim_countries AS co on co.source_id = a.source_id
WHERE os.net_order IS TRUE
AND co.is_active IS TRUE
AND a.order_date > '2018-01-01'
AND (a.amt_paid_eur < 2000 OR a.amt_paid_eur IS NULL)  --No order should be thousands of EUR
GROUP BY 1,2,3
;


CREATE TEMPORARY TABLE joker_cumulative
DISTSTYLE ALL
AS
SELECT
    source_id,
    city,
    report_month,
    SUM(acquisitions_od_darkstore) OVER (PARTITION BY source_id, city ORDER BY report_month ROWS UNBOUNDED PRECEDING) AS acquisitions_od_darkstore_cumulative,
    SUM(acquisitions_od_vertical) OVER (PARTITION BY source_id, city ORDER BY report_month ROWS UNBOUNDED PRECEDING) AS acquisitions_od_vertical_cumulative,
    SUM(acquisitions_mp) OVER (PARTITION BY source_id, city ORDER BY report_month ROWS UNBOUNDED PRECEDING) AS acquisitions_mp_cumulative,
    SUM(acquisitions_od_restaurants) OVER (PARTITION BY source_id, city ORDER BY report_month ROWS UNBOUNDED PRECEDING) AS acquisitions_od_restaurants_cumulative
FROM joker;

CREATE TEMPORARY TABLE joker_cumulative_yoy
DISTSTYLE ALL
AS
SELECT
    source_id,
    city,
    report_month,
    lag(acquisitions_od_darkstore_cumulative,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_darkstore_cumulative_yoy,
    lag(acquisitions_od_darkstore_cumulative,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_darkstore_cumulative_mom,
    lag(acquisitions_od_vertical_cumulative,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_vertical_cumulative_yoy,
    lag(acquisitions_od_vertical_cumulative,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_vertical_cumulative_mom,
    lag(acquisitions_mp_cumulative,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mp_cumulative_yoy,
    lag(acquisitions_mp_cumulative,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mp_cumulative_mom,
    lag(acquisitions_od_restaurants_cumulative,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_restaurants_cumulative_yoy,
    lag(acquisitions_od_restaurants_cumulative,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_restaurants_cumulative_mom
FROM joker_cumulative
;


--DROP TABLE IF EXISTS joker_yoy;
CREATE TEMPORARY TABLE joker_yoy
DISTSTYLE ALL
AS
SELECT
    source_id,
    city,
    report_month,
    lag(amt_joker_eur_total,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_joker_eur_total_yoy,
    lag(amt_joker_eur_total,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_joker_eur_total_mom,
    lag(orders_failed_cancellation,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_cancellation_yoy,
    lag(orders_failed_cancellation,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_cancellation_mom,
    lag(orders_failed_response,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_response_yoy,
    lag(orders_failed_response,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_response_mom,
    lag(orders_failed_rejection,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_rejection_yoy,
    lag(orders_failed_rejection,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_rejection_mom,
    lag(orders_failed_transmission,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_transmission_yoy,
    lag(orders_failed_transmission,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_transmission_mom,
    lag(orders_failed_delivery,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_delivery_yoy,
    lag(orders_failed_delivery,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_delivery_mom,
    lag(orders_failed_overall,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_overall_yoy,
    lag(orders_failed_overall,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_overall_mom,
    lag(orders_failed_payment,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_payment_yoy,
    lag(orders_failed_payment,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_payment_mom,
    lag(orders_failed_verification,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_verification_yoy,
    lag(orders_failed_verification,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_verification_mom,
    lag(orders_failed_other,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_other_yoy,
    lag(orders_failed_other,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_failed_other_mom,

    lag(groceries_gmv,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS groceries_gmv_yoy,
    lag(groceries_gmv,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS groceries_gmv_mom,
    lag(groceries_orders,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS groceries_orders_yoy,
    lag(groceries_orders,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS groceries_orders_mom,

    lag(discount_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_darkstore_yoy,
    lag(discount_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_darkstore_mom,
    lag(discount_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_od_vertical_yoy,
    lag(discount_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_od_vertical_mom,
    lag(discount_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_od_restaurant_yoy,
    lag(discount_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_od_restaurant_mom,
    lag(discount_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_mp_yoy,
    lag(discount_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS discount_mp_mom,

    lag(gmv_eur_kitchen,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_kitchen_yoy,
    lag(gmv_eur_kitchen,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_kitchen_mom,
    lag(gmv_eur_concept,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_concept_yoy,
    lag(gmv_eur_concept,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_concept_mom,
    lag(amt_commission_eur_kitchen,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_kitchen_yoy,
    lag(amt_commission_eur_kitchen,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_kitchen_mom,
    lag(amt_commission_eur_concept,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_concept_yoy,
    lag(amt_commission_eur_concept,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_concept_mom,
    lag(total_order_kitchen,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS total_order_kitchen_yoy,
    lag(total_order_kitchen,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS total_order_kitchen_mom,
    lag(total_order_concept,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS total_order_concept_yoy,
    lag(total_order_concept,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS total_order_concept_mom,

    lag(successful_order_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_order_od_darkstore_yoy,
    lag(successful_order_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_order_od_darkstore_mom,
    lag(gmv_eur_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_od_darkstore_yoy,
    lag(gmv_eur_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_od_darkstore_mom,
    lag(amt_commission_eur_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_darkstore_yoy,
    lag(amt_commission_eur_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_darkstore_mom,
    lag(order_value_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_darkstore_yoy,
    lag(order_value_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_darkstore_mom,
    lag(delivery_fee_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_darkstore_yoy,
    lag(delivery_fee_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_darkstore_mom,
    lag(failed_orders_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_darkstore_yoy,
    lag(failed_orders_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_darkstore_mom,
    lag(acquisitions_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_darkstore_yoy,
    lag(acquisitions_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_darkstore_mom,
    lag(voucher_order_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_order_od_darkstore_yoy,
    lag(voucher_order_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_order_od_darkstore_mom,

    lag(successful_orders_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_od_vertical_yoy,
    lag(successful_orders_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_od_vertical_mom,
    lag(gmv_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_od_vertical_yoy,
    lag(gmv_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_od_vertical_mom,
    lag(commission_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_vertical_yoy,
    lag(commission_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_vertical_mom,
    lag(order_value_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_vertical_yoy,
    lag(order_value_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_vertical_mom,
    lag(delivery_fee_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_vertical_yoy,
    lag(delivery_fee_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_vertical_mom,
    lag(failed_orders_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_vertical_yoy,
    lag(failed_orders_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_vertical_mom,
    lag(acquisitions_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_vertical_yoy,
    lag(acquisitions_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_vertical_mom,
    lag(voucher_orders_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_od_vertical_yoy,
    lag(voucher_orders_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_od_vertical_mom,

    lag(successful_orders_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_mp_yoy,
    lag(successful_orders_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_mp_mom,
    lag(gmv_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_mp_yoy,
    lag(gmv_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_mp_mom,
    lag(commission_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_mp_yoy,
    lag(commission_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_mp_mom,
    lag(order_value_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_mp_yoy,
    lag(order_value_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_mp_mom,
    lag(delivery_fee_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_mp_yoy,
    lag(delivery_fee_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_mp_mom,
    lag(failed_orders_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_mp_yoy,
    lag(failed_orders_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_mp_mom,
    lag(acquisitions_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mp_yoy,
    lag(acquisitions_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_mp_mom,
    lag(voucher_orders_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_mp_yoy,
    lag(voucher_orders_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_mp_mom,

    lag(successful_orders_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_od_restaurants_yoy,
    lag(successful_orders_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS successful_orders_od_restaurants_mom,
    lag(gmv_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_od_restaurants_yoy,
    lag(gmv_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_od_restaurants_mom,
    lag(commission_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_restaurants_yoy,
    lag(commission_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS amt_commission_eur_od_restaurants_mom,
    lag(order_value_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_restaurants_yoy,
    lag(order_value_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS order_value_od_restaurants_mom,
    lag(delivery_fee_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_restaurants_yoy,
    lag(delivery_fee_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_fee_od_restaurants_mom,
    lag(failed_orders_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_restaurants_yoy,
    lag(failed_orders_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS failed_orders_od_restaurants_mom,
    lag(acquisitions_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_restaurants_yoy,
    lag(acquisitions_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS acquisitions_od_restaurants_mom,
    lag(voucher_orders_od_restaurants,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_od_restaurants_yoy,
    lag(voucher_orders_od_restaurants,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS voucher_orders_od_restaurants_mom,

    lag(amt_commission_eur_dh,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS commission_eur_dh_yoy,
    lag(amt_commission_eur_dh,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS commission_eur_dh_mom,
    lag(amt_gmv_eur_dh,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_dh_yoy,
    lag(amt_gmv_eur_dh,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS gmv_eur_dh_mom,
    lag(own_delivery_orders,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS own_delivery_orders_yoy,
    lag(own_delivery_orders,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS own_delivery_orders_mom

FROM joker
;



--DROP TABLE IF EXISTS ops_data;
CREATE TEMPORARY TABLE ops_data
DISTSTYLE ALL
AS
SELECT
    a.source_id,
    DATE_TRUNC('month', a.order_date)::DATE AS report_month,
    COALESCE(LOWER(TRIM(r.city)),'unknown') AS city,

    SUM(CASE WHEN a.is_dh_delivery IS TRUE AND a.is_sent IS TRUE AND op.hu_eff_delivery_time_m <= 20 THEN 1 END) AS orders_delivery_20m, -- Orders that took longer than 45m to deliver -- in minutes
    SUM(CASE WHEN a.is_dh_delivery IS TRUE AND a.is_sent IS TRUE AND a.is_preorder IS FALSE AND op.promised_eta_m <= 20 THEN 1 END) AS orders_promised_20,
    SUM(CASE WHEN a.is_dh_delivery IS TRUE AND a.is_sent IS TRUE AND a.is_preorder IS FALSE AND op.promised_eta_m <= 20 AND op.hu_eff_delivery_time_m <= 20 THEN 1 END) AS orders_promised_20_delivered_20,

    SUM(CASE WHEN a.is_sent AND r.shop_type = 'darkstores' AND a.is_dh_delivery THEN op.hu_delivery_distance_km END) AS delivery_distance_darkstore,
    SUM(CASE WHEN a.is_sent AND r.is_shop AND a.is_dh_delivery AND r.shop_type <> 'darkstores' THEN op.hu_delivery_distance_km END) AS delivery_distance_od_vertical,
    SUM(CASE WHEN a.is_sent AND r.is_shop IS FALSE AND a.is_dh_delivery THEN op.hu_delivery_distance_km END) AS delivery_distance_od_restaurant,
    SUM(CASE WHEN a.is_sent AND a.is_dh_delivery IS FALSE THEN op.hu_delivery_distance_km END) AS delivery_distance_mp,
    SUM(CASE WHEN a.is_sent AND r.shop_type = 'darkstores' AND a.is_dh_delivery THEN op.hu_eff_delivery_time_m END) AS delivery_time_darkstore,
    SUM(CASE WHEN a.is_sent AND r.is_shop AND a.is_dh_delivery AND r.shop_type <> 'darkstores' THEN op.hu_eff_delivery_time_m END) AS delivery_time_od_vertical,
    SUM(CASE WHEN a.is_sent AND r.is_shop IS FALSE AND a.is_dh_delivery THEN op.hu_eff_delivery_time_m END) AS delivery_time_od_restaurant,
    SUM(CASE WHEN a.is_sent AND a.is_dh_delivery IS FALSE THEN op.hu_eff_delivery_time_m END) AS delivery_time_mp,
    SUM(CASE WHEN a.is_sent AND op.hu_rider_delay_m >= 10 AND op.hu_is_rider_pickup_ontime IS FALSE AND r.shop_type = 'darkstores' AND a.is_dh_delivery THEN 1 END) AS orders_courier_late_10m_darkstore,
    SUM(CASE WHEN a.is_sent AND op.hu_rider_delay_m >= 10 AND op.hu_is_rider_pickup_ontime IS FALSE AND r.is_shop AND a.is_dh_delivery AND r.shop_type <> 'darkstores' THEN 1 END) AS orders_courier_late_10m_vertical,
    SUM(CASE WHEN a.is_sent AND op.hu_rider_delay_m >= 10 AND op.hu_is_rider_pickup_ontime IS FALSE AND r.is_shop IS FALSE AND a.is_dh_delivery THEN 1 END) AS orders_courier_late_10m_restaurant,
    SUM(CASE WHEN a.is_sent AND op.hu_rider_delay_m >= 10 AND op.hu_is_rider_pickup_ontime IS FALSE AND a.is_dh_delivery IS FALSE THEN 1 END) AS orders_courier_late_10m_mp
FROM dwh_il.ranked_fct_order AS a
LEFT JOIN dwh_bl.dim_restaurant AS r
    ON a.source_id = r.source_id
    AND a.restaurant_id = r.restaurant_id
LEFT JOIN dwh_il.dim_order_status AS os
    ON os.source_id = a.source_id
    AND os.order_status_id = a.order_status_id
JOIN dwh_il.fct_ops AS op
    ON a.source_id = op.source_id
    AND a.order_id = op.order_id
    AND op.order_date >= '2018-01-01'
    AND op.order_date < DATE_TRUNC('month', CURRENT_DATE)
    AND op.is_sent
JOIN dwh_il.dim_countries AS co
    ON co.source_id = a.source_id
WHERE os.net_order IS TRUE
AND co.is_active IS TRUE
AND a.is_sent
AND a.order_date > '2018-01-01'
AND (a.amt_paid_eur < 2000 OR a.amt_paid_eur IS NULL)  --No order should be thousands of EUR
GROUP BY 1,2,3
;


--DROP TABLE IF EXISTS ops_data_yoy;
CREATE TEMPORARY TABLE ops_data_yoy
DISTSTYLE ALL
AS
SELECT
    source_id,
    city,
    report_month,
    lag(orders_delivery_20m,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_delivery_20m_yoy,
    lag(orders_delivery_20m,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_delivery_20m_mom,
    lag(orders_promised_20,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_promised_20_yoy,
    lag(orders_promised_20,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_promised_20_mom,
    lag(orders_promised_20_delivered_20,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_promised_20_delivered_20_yoy,
    lag(orders_promised_20_delivered_20,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_promised_20_delivered_20_mom,

    lag(delivery_distance_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_darkstore_yoy,
    lag(delivery_distance_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_darkstore_mom,
    lag(delivery_distance_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_od_vertical_yoy,
    lag(delivery_distance_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_od_vertical_mom,
    lag(delivery_distance_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_od_restaurant_yoy,
    lag(delivery_distance_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_od_restaurant_mom,
    lag(delivery_distance_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_mp_yoy,
    lag(delivery_distance_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_distance_mp_mom,

    lag(delivery_time_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_darkstore_yoy,
    lag(delivery_time_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_darkstore_mom,
    lag(delivery_time_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_od_vertical_yoy,
    lag(delivery_time_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_od_vertical_mom,
    lag(delivery_time_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_od_restaurant_yoy,
    lag(delivery_time_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_od_restaurant_mom,
    lag(delivery_time_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_mp_yoy,
    lag(delivery_time_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS delivery_time_mp_mom,
    lag(orders_courier_late_10m_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_darkstore_yoy,
    lag(orders_courier_late_10m_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_darkstore_mom,
    lag(orders_courier_late_10m_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_vertical_yoy,
    lag(orders_courier_late_10m_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_vertical_mom,
    lag(orders_courier_late_10m_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_restaurant_yoy,
    lag(orders_courier_late_10m_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_restaurant_mom,
    lag(orders_courier_late_10m_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_mp_yoy,
    lag(orders_courier_late_10m_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS orders_courier_late_10m_mp_mom

FROM ops_data
;


CREATE TEMPORARY TABLE cvr_bad_city_hack
DISTSTYLE ALL
AS
SELECT
    cvr.source_id,
    cvr.city,
    cm.city_fixed,
    ROW_NUMBER() OVER (PARTITION BY cvr.source_id, cvr.city) AS row_number
FROM ds_global_dev.bq_city_cvr cvr
JOIN dwh_il.dim_city_mapping cm ON cvr.source_id = cm.source_id and LOWER(cvr.city) ILIKE cm.city || '%'
WHERE cvr.source_id IN (1, 2, 6, 33)
;


--DROP TABLE IF EXISTS cvr;
CREATE TEMPORARY TABLE cvr
DISTSTYLE ALL
AS
SELECT
    cvr.source_id,
    DATE_TRUNC('month', cvr.report_date::date)::date AS report_month,
    LOWER(TRIM(COALESCE(h.city_fixed, cm.city_fixed, cvr.city))) AS city,
    SUM(cvr.all_visits) AS all_visits,
    SUM(cvr.all_transactions) AS all_transactions
FROM ds_global_dev.bq_city_cvr AS cvr
LEFT JOIN dwh_il.dim_city_mapping AS cm ON cm.source_id = cvr.source_id AND LOWER(TRIM(cm.city)) = LOWER(TRIM(cvr.city))
LEFT JOIN cvr_bad_city_hack h ON h.source_id = cvr.source_id AND h.city = cvr.city AND h.row_number = 1
WHERE cvr.report_date::date < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY 1, 2, 3
;

--DROP TABLE IF EXISTS rca;
CREATE TEMPORARY TABLE rca
DISTKEY(report_month)
AS
SELECT
    rca.source_id,
    LOWER(TRIM(dr.city)) AS city,
    DATE_TRUNC('month', rca.report_date::DATE)::DATE AS report_month,
    SUM(CASE WHEN dr.is_dh_delivery THEN rca.all_visits END) AS all_visits_mp,
    SUM(CASE WHEN dr.is_dh_delivery THEN rca.all_transactions END) AS all_transactions_mp,
    SUM(CASE WHEN dr.shop_type = 'darkstores' THEN rca.all_visits END) AS all_visits_darkstore,
    SUM(CASE WHEN dr.shop_type = 'darkstores' THEN rca.all_transactions END) AS all_transactions_darkstore,
    SUM(CASE WHEN dr.shop_type <> 'darkstores' AND dr.is_shop AND is_dh_delivery THEN rca.all_visits END) AS all_visits_od_vertical,
    SUM(CASE WHEN dr.shop_type <> 'darkstores' AND dr.is_shop AND is_dh_delivery THEN rca.all_transactions  END) AS all_transactions_od_vertical,
    SUM(CASE WHEN dr.is_shop IS FALSE AND is_dh_delivery THEN rca.all_visits END) AS all_visits_od_restaurant,
    SUM(CASE WHEN dr.is_shop IS FALSE AND is_dh_delivery THEN rca.all_transactions END) AS all_transactions_od_restaurant
FROM dwh_ds.bq_restaurant_cvr_area AS rca
LEFT JOIN dwh_bl.dim_restaurant AS dr ON rca.restaurant_id = dr.restaurant_id AND rca.source_id = dr.source_id
GROUP BY 1,2,3;


--DROP TABLE IF EXISTS rca;
CREATE TEMPORARY TABLE rca_mom_yoy
DISTKEY(report_month)
AS
SELECT
    source_id,
    city,
    report_month,
    lag(all_visits_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_mp_yoy,
    lag(all_visits_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_mp_mom,
    lag(all_transactions_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_mp_yoy,
    lag(all_transactions_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_mp_mom,
    lag(all_visits_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_darkstore_yoy,
    lag(all_visits_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_darkstore_mom,
    lag(all_transactions_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_darkstore_yoy,
    lag(all_transactions_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_darkstore_mom,
    lag(all_visits_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_od_vertical_yoy,
    lag(all_visits_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_od_vertical_mom,
    lag(all_transactions_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_od_vertical_yoy,
    lag(all_transactions_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_od_vertical_mom,
    lag(all_visits_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_od_restaurant_yoy,
    lag(all_visits_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_visits_od_restaurant_mom,
    lag(all_transactions_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_od_restaurant_yoy,
    lag(all_transactions_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS all_transactions_od_restaurant_mom

FROM rca
;


CREATE TEMPORARY TABLE rfo_reorders
DISTKEY(city)
AS
SELECT
    rfo.source_id,
    LOWER(TRIM(dim_r.city)) AS city,
    DATE_TRUNC('month',rfo.order_date)::date AS report_month,
    rfo.analytical_customer_id,
    CASE WHEN dim_r.shop_type = 'darkstores' THEN TRUE END AS customer_darkstore,
    CASE WHEN dim_r.shop_type = 'darkstores' AND order_j.source_id IS NOT NULL THEN TRUE END AS did_return_darkstore,
    CASE WHEN dim_r.shop_type <> 'darkstores' AND dim_r.is_dh_delivery AND dim_r.is_shop THEN TRUE END AS customer_od_vertical,
    CASE WHEN dim_r.shop_type <> 'darkstores' AND dim_r.is_dh_delivery AND dim_r.is_shop AND order_j.source_id IS NOT NULL THEN TRUE END AS did_return_od_vertical,
    CASE WHEN dim_r.is_dh_delivery IS FALSE THEN TRUE END AS customer_mp,
    CASE WHEN dim_r.is_dh_delivery IS FALSE AND order_j.source_id IS NOT NULL THEN TRUE END AS did_return_mp,
    CASE WHEN dim_r.is_shop IS FALSE AND dim_r.is_dh_delivery THEN TRUE END AS customer_od_restaurant,
    CASE WHEN dim_r.is_shop IS FALSE AND dim_r.is_dh_delivery AND order_j.source_id IS NOT NULL THEN TRUE END AS did_return_od_restaurant
FROM dwh_il.ranked_fct_order AS rfo
LEFT JOIN dwh_il.dim_countries AS dim_c USING(source_id)
LEFT JOIN dwh_bl.dim_restaurant AS dim_r USING(source_id, restaurant_id)
LEFT JOIN dwh_il.ranked_fct_order order_j
                 ON rfo.source_id = order_j.source_id
                AND rfo.analytical_customer_id = order_j.analytical_customer_id
                AND rfo.customer_order_rank+1 = order_j.customer_order_rank
                AND DATEADD('month',1, rfo.order_date) >  DATE_TRUNC('month',order_j.order_date)
                AND order_j.order_date >= '2018-01-01'
                AND order_j.is_sent
                AND order_j.analytical_customer_id is not null
WHERE   dim_c.is_active IS TRUE
AND (dim_r.is_test_restaurant IS FALSE OR dim_r.is_test_restaurant IS NULL)
AND rfo.is_sent IS TRUE
AND rfo.order_date  BETWEEN '2018-01-01' AND DATE_TRUNC('day', CURRENT_DATE)::DATE-'1 second'::INTERVAL
AND rfo.analytical_customer_id IS NOT NULL;


CREATE TEMPORARY TABLE reorders
DISTKEY(report_month)
AS
SELECT
    source_id,
    city,
    report_month::date,
    COUNT(DISTINCT CASE WHEN did_return_darkstore       THEN analytical_customer_id END) AS customers_returned_od_darkstore,
    COUNT(DISTINCT CASE WHEN customer_darkstore         THEN analytical_customer_id END) AS customers_total_od_darkstore,
    COUNT(DISTINCT CASE WHEN did_return_od_vertical     THEN analytical_customer_id END) AS customers_returned_od_vertical,
    COUNT(DISTINCT CASE WHEN customer_od_vertical       THEN analytical_customer_id END) AS customers_total_od_vertical,
    COUNT(DISTINCT CASE WHEN did_return_mp              THEN analytical_customer_id END) AS customers_returned_mp,
    COUNT(DISTINCT CASE WHEN customer_mp                THEN analytical_customer_id END) AS customers_total_mp,
    COUNT(DISTINCT CASE WHEN did_return_od_restaurant   THEN analytical_customer_id END) AS customers_returned_od_restaurant,
    COUNT(DISTINCT CASE WHEN customer_od_restaurant     THEN analytical_customer_id END) AS customers_total_od_restaurant
FROM rfo_reorders
GROUP BY 1,2,3;



CREATE TEMPORARY TABLE reorders_mom_yoy AS
SELECT
    source_id,
    city,
    report_month,
    lag(customers_returned_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_darkstore_yoy,
    lag(customers_returned_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_darkstore_mom,
    lag(customers_total_od_darkstore,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_darkstore_yoy,
    lag(customers_total_od_darkstore,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_darkstore_mom,
    lag(customers_returned_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_vertical_yoy,
    lag(customers_returned_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_vertical_mom,
    lag(customers_total_od_vertical,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_vertical_yoy,
    lag(customers_total_od_vertical,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_vertical_mom,
    lag(customers_returned_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_mp_yoy,
    lag(customers_returned_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_mp_mom,
    lag(customers_total_mp,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_mp_yoy,
    lag(customers_total_mp,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_mp_mom,
    lag(customers_returned_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_restaurant_yoy,
    lag(customers_returned_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_returned_od_restaurant_mom,
    lag(customers_total_od_restaurant,12) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_restaurant_yoy,
    lag(customers_total_od_restaurant,1) OVER (PARTITION BY source_id, city ORDER BY report_month) AS customers_total_od_restaurant_mom
FROM reorders
;



--DROP TABLE IF EXISTS competitive_cities;
CREATE TEMPORARY TABLE competitive_cities
DISTSTYLE ALL
AS
SELECT
    dc.source_id,
    city_new,
    MAX(is_competitive) AS is_competitive
FROM dwh_gs_marketing."cities$cities_dh" AS cc
JOIN dwh_il.dim_countries AS dc
     ON dc.common_name = cc.country
    AND dc.is_active
GROUP BY 1, 2
;

-- KPIs are aggregated again below to account for cases where we have two city names in our data which map to only one city name in our population data
-- Example: city = 'taipei' (stopped reporting orders but still exists due to old acquisition data) and city = 'taipei city' (contains current data), both of these map to Taipei in the city_coords table
-- If not aggregated THEN we would have two entries for the same city every month
-- The aggregation MIN() used on city_tier and city_rank is to make sure we take the highest rank for the city where available
-- For the columns population/gmv_source/active_customers_source, these columns will contain duplicated data so they are aggregated using MAX()

--DROP TABLE IF EXISTS dwh_bl.tableau_monthly_report_city;
--CREATE TABLE dwh_bl.tableau_monthly_report_city AS
TRUNCATE TABLE  dwh_bl.tableau_monthly_report_city;
INSERT INTO     dwh_bl.tableau_monthly_report_city
SELECT
    c.source_id,
    c.management_entity_group,
    c.display_name,
    c.region,
    c.country,
    c.report_month,
    LOWER(TRIM(c.city)) AS city,
    CASE WHEN comp.is_competitive = 'yes' THEN TRUE ELSE FALSE END AS is_competitive,
    MIN(ct.city_tier) AS city_tier,
    MIN(CASE WHEN ct.city_rank > 10 THEN ct.city_tier ELSE ct.city_rank::VARCHAR(5) END)  AS rank_tier,
    MIN(ct.city_rank) AS city_rank,
    SUM(o.acquisitions) AS acquisitions,
    SUM(omy.acquisitions_yoy) AS acquisitions_yoy,
    SUM(omy.acquisitions_mom) AS acquisitions_mom,
    SUM(omy.acquisitions_mom_yoy) AS acquisitions_mom_yoy,
    SUM(o.acquisitions_cumulative) AS customer_base,
    SUM(omy.customer_base_yoy) AS customer_base_yoy,
    SUM(omy.customer_base_mom) AS customer_base_mom,
    SUM(o.orders) AS orders,
    SUM(o.orders_failed) AS orders_failed,
    SUM(tb.successful_orders) AS top_10_brand_orders,
    SUM(j.own_delivery_orders) AS own_delivery_orders,
    SUM(j.amt_gmv_eur_dh) AS amt_gmv_eur_dh,
    SUM(j.amt_commission_eur_dh) AS amt_commission_eur_dh,
    SUM(mor.amt_delivery_fee_eur) AS amt_delivery_fee_eur,
    SUM(mor.amt_cv_eur) AS amt_cv_eur,
    SUM(mor.amt_delivery_fee_eur_dh) AS amt_delivery_fee_eur_dh,
    SUM(mor.amt_cv_eur_dh) AS amt_cv_eur_dh,
    SUM(omy.orders_yoy) AS orders_yoy,
    SUM(omy.orders_mom) AS orders_mom,
    SUM(omy.orders_mom_yoy) AS orders_mom_yoy,
    SUM(jy.own_delivery_orders_yoy) AS own_delivery_orders_yoy,
    SUM(jy.own_delivery_orders_mom) AS own_delivery_orders_mom,
    SUM(omy.orders_failed_mom) AS orders_failed_mom,
    SUM(omy.orders_failed_yoy) AS orders_failed_yoy,
    SUM(o.gmv_eur) AS gmv_eur,
    MAX(o.gmv_source) AS gmv_source,
    SUM(omy.gmv_eur_yoy) AS gmv_eur_yoy,
    SUM(omy.gmv_eur_mom) AS gmv_eur_mom,
    SUM(o.commission_eur) AS commission_eur,
    SUM(omy.commission_eur_yoy) AS commission_eur_yoy,
    SUM(omy.commission_eur_mom) AS commission_eur_mom,
    SUM(jy.commission_eur_dh_yoy) AS commission_eur_dh_yoy,
    SUM(jy.commission_eur_dh_mom) AS commission_eur_dh_mom,
    SUM(jy.gmv_eur_dh_yoy) AS gmv_eur_dh_yoy,
    SUM(jy.gmv_eur_dh_mom) AS gmv_eur_dh_mom,
    SUM(o.active_customers) AS active_customers,
    MAX(o.active_customers_source) AS active_customers_source,
    SUM(omy.active_customers_yoy) AS active_customers_yoy,
    SUM(omy.active_customers_mom) AS active_customers_mom,
    SUM(mor.restaurants_new) AS restaurants_new,
    SUM(mor.restaurants_churned) AS restaurants_churned,
    SUM(mor.restaurants_online) AS restaurants_online,
    SUM(mor.acquisitions_rr) AS acquisitions_rr,
    SUM(mor.returning_customers) AS returning_customers,
    SUM(mor.restaurants_d_rating) AS restaurants_d_rating,
    SUM(mor.pp_revenue) AS pp_revenue,
    SUM(mor.restaurants_online_zero_orders) AS restaurants_online_zero_orders,
    SUM(mor.darkstore_online_od) AS darkstore_online_od,
    SUM(mor.darkstore_new_od) AS darkstore_new_od,
    SUM(mor.darkstore_churned_od) AS darkstore_churned_od,
    SUM(mor.darkstore_online_zero_orders_od) AS darkstore_online_zero_orders_od,
    SUM(mor.vertical_online_od) AS vertical_online_od,
    SUM(mor.vertical_new_od) AS vertical_new_od,
    SUM(mor.vertical_churned_od) AS vertical_churned_od,
    SUM(mor.vertical_online_zero_orders_od) AS vertical_online_zero_orders_od,

    SUM(mor.restaurant_online_od) AS restaurant_online_od,
    SUM(mor.restaurant_new_od) AS restaurant_new_od,
    SUM(mor.restaurant_churned_od) AS restaurant_churned_od,
    SUM(mor.restaurant_online_zero_orders_od) AS restaurant_online_zero_orders_od,

    SUM(mor.online_mp) AS online_mp,
    SUM(mor.new_mp) AS new_mp,
    SUM(mor.churned_mp) AS churned_mp,
    SUM(mor.online_zero_orders_mp) AS online_zero_orders_mp,

    SUM(mor.concept_online) AS concept_online,
    SUM(mor.concept_new) AS concept_new,
    SUM(mor.concept_churned) AS concept_churned,
    SUM(mor.concept_online_zero_orders) AS concept_online_zero_orders,

    SUM(mor.kitchen_restaurant_online) AS kitchen_restaurant_online,
    SUM(mor.kitchen_restaurant_new) AS kitchen_restaurant_new,
    SUM(mor.kitchen_restaurant_churned) AS kitchen_restaurant_churned,
    SUM(mor.kitchen_restaurant_online_zero_orders) AS kitchen_restaurant_online_zero_orders,

    SUM(mor.kitchen_online) AS kitchen_online,
    SUM(mor.kitchen_new) AS kitchen_new,
    SUM(mor.kitchen_churned) AS kitchen_churned,
    SUM(mor.kitchen_online_zero_orders) AS kitchen_online_zero_orders,

    SUM(mor.restaurants_recovered_churn) AS restaurants_recovered_churn,
    SUM(mor.restaurants_new_dh) AS restaurants_new_dh,
    SUM(mor.restaurants_online_dh) AS restaurants_online_dh,
    SUM(mor.restaurants_churned_dh) AS restaurants_churned_dh,
    SUM(mor.restaurants_online_zero_orders_dh) AS restaurants_online_zero_orders_dh,
    SUM(mor.fp_amt_premium_fee_eur) AS fp_amt_premium_fee_eur,
    SUM(mor.ban_amt_premium_fee_eur) AS ban_amt_premium_fee_eur,
    SUM(j.gmv_eur_kitchen) AS gmv_eur_kitchen,
    SUM(j.gmv_eur_concept) AS gmv_eur_concept,
    SUM(j.amt_commission_eur_kitchen) AS amt_commission_eur_kitchen,
    SUM(j.amt_commission_eur_concept) AS amt_commission_eur_concept,
    SUM(j.total_order_kitchen) AS total_order_kitchen,
    SUM(j.total_order_concept) AS total_order_concept,

    SUM(mor.converted_rating_concept) AS converted_rating_concept,
    SUM(mor.rated_order_count_concept) AS rated_order_count_concept,

    SUM(mor.converted_rating_kitchen) AS converted_rating_kitchen,
    SUM(mor.rated_order_count_kitchen) AS rated_order_count_kitchen,

    SUM(j.gmv_eur_od_darkstore) AS gmv_eur_od_darkstore,
    SUM(j.amt_commission_eur_od_darkstore) AS amt_commission_eur_od_darkstore,
    SUM(j.successful_order_od_darkstore) AS successful_order_od_darkstore,
    SUM(j.order_value_od_darkstore) AS order_value_od_darkstore,
    SUM(j.delivery_fee_od_darkstore) AS delivery_fee_od_darkstore,
    SUM(mor.converted_rating_od_darkstore) AS converted_rating_od_darkstore,
    SUM(mor.rated_order_count_od_darkstore) AS rated_order_count_od_darkstore,
    SUM(mor.nps_ao_promoters_od_darkstore) AS nps_ao_promoters_od_darkstore,
    SUM(mor.nps_ao_detractors_od_darkstore) AS nps_ao_detractors_od_darkstore,
    SUM(mor.nps_ao_responses_od_darkstore) AS nps_ao_responses_od_darkstore,
    SUM(j.failed_orders_od_darkstore) AS failed_orders_od_darkstore,
    SUM(j.acquisitions_od_darkstore) AS acquisitions_od_darkstore,
    SUM(j.voucher_order_od_darkstore) AS voucher_order_od_darkstore,
    SUM(jc.acquisitions_od_darkstore_cumulative) AS customer_base_od_darkstore,

    SUM(j.successful_orders_od_vertical) AS successful_orders_od_vertical,
    SUM(j.gmv_od_vertical) AS gmv_od_vertical,
    SUM(j.order_value_od_vertical) AS order_value_od_vertical,
    SUM(j.delivery_fee_od_vertical) AS delivery_fee_od_vertical,
    SUM(j.commission_od_vertical) AS commission_od_vertical,
    SUM(mor.converted_rating_od_vertical) AS converted_rating_od_vertical,
    SUM(mor.rated_order_count_od_vertical) AS rated_order_count_od_vertical,
    SUM(mor.nps_ao_promoters_od_vertical) AS nps_ao_promoters_od_vertical,
    SUM(mor.nps_ao_detractors_od_vertical) AS nps_ao_detractors_od_vertical,
    SUM(mor.nps_ao_responses_od_vertical) AS nps_ao_responses_od_vertical,
    SUM(j.failed_orders_od_vertical) AS failed_orders_od_vertical,
    SUM(j.acquisitions_od_vertical) AS acquisitions_od_vertical,
    SUM(j.voucher_orders_od_vertical) AS voucher_orders_od_vertical,
    SUM(jc.acquisitions_od_vertical_cumulative) AS customer_base_od_vertical,

    SUM(j.successful_orders_mp) AS successful_orders_mp,
    SUM(j.gmv_mp) AS gmv_mp,
    SUM(j.order_value_mp) AS order_value_mp,
    SUM(j.delivery_fee_mp) AS delivery_fee_mp,
    SUM(j.commission_mp) AS commission_mp,
    SUM(mor.converted_rating_mp) AS converted_rating_mp,
    SUM(mor.rated_order_count_mp) AS rated_order_count_mp,
    SUM(mor.nps_ao_promoters_mp) AS nps_ao_promoters_mp,
    SUM(mor.nps_ao_detractors_mp) AS nps_ao_detractors_mp,
    SUM(mor.nps_ao_responses_mp) AS nps_ao_responses_mp,
    SUM(j.failed_orders_mp) AS failed_orders_mp,
    SUM(j.acquisitions_mp) AS acquisitions_mp,
    SUM(j.voucher_orders_mp) AS voucher_orders_mp,
    SUM(jc.acquisitions_mp_cumulative) AS customer_base_mp,

    SUM(j.successful_orders_od_restaurants) AS successful_orders_od_restaurants,
    SUM(j.gmv_od_restaurants) AS gmv_od_restaurants,
    SUM(j.order_value_od_restaurants) AS order_value_od_restaurants,
    SUM(j.delivery_fee_od_restaurants) AS delivery_fee_od_restaurants,
    SUM(j.commission_od_restaurants) AS commission_od_restaurants,
    SUM(mor.converted_rating_od_restaurants) AS converted_rating_od_restaurants,
    SUM(mor.rated_order_count_od_restaurants) AS rated_order_count_od_restaurants,
    SUM(mor.nps_ao_promoters_od_restaurants) AS nps_ao_promoters_od_restaurants,
    SUM(mor.nps_ao_detractors_od_restaurants) AS nps_ao_detractors_od_restaurants,
    SUM(mor.nps_ao_responses_od_restaurants) AS nps_ao_responses_od_restaurants,
    SUM(j.failed_orders_od_restaurants) AS failed_orders_od_restaurants,
    SUM(j.acquisitions_od_restaurants) AS acquisitions_od_restaurants,
    SUM(j.voucher_orders_od_restaurants) AS voucher_orders_od_restaurants,
    SUM(jc.acquisitions_od_restaurants_cumulative) AS customer_base_od_restaurants,

    SUM(my.restaurants_online_yoy) AS restaurants_online_yoy,
    SUM(my.restaurants_online_mom) AS restaurants_online_mom,
    SUM(my.acquisitions_rr_yoy) AS acquisitions_rr_yoy,
    SUM(my.returning_customers_yoy) AS returning_customers_yoy,
    SUM(my.pp_revenue_yoy) AS pp_revenue_yoy,
    SUM(my.fp_amt_premium_fee_eur_yoy) AS fp_amt_premium_fee_eur_yoy,
    SUM(my.ban_amt_premium_fee_eur_yoy) AS ban_amt_premium_fee_eur_yoy,
    SUM(my.pp_revenue_mom) AS pp_revenue_mom,
    SUM(my.fp_amt_premium_fee_eur_mom) AS fp_amt_premium_fee_eur_mom,
    SUM(my.ban_amt_premium_fee_eur_mom) AS ban_amt_premium_fee_eur_mom,
    SUM(my.amt_delivery_fee_eur_yoy) AS amt_delivery_fee_eur_yoy,
    SUM(my.amt_delivery_fee_eur_mom) AS amt_delivery_fee_eur_mom,
    SUM(my.amt_cv_eur_yoy) AS amt_cv_eur_yoy,
    SUM(my.amt_cv_eur_mom) AS amt_cv_eur_mom,
    SUM(my.amt_delivery_fee_eur_dh_yoy) AS amt_delivery_fee_eur_dh_yoy,
    SUM(my.amt_delivery_fee_eur_dh_mom) AS amt_delivery_fee_eur_dh_mom,
    SUM(my.amt_cv_eur_dh_yoy) AS amt_cv_eur_dh_yoy,
    SUM(my.amt_cv_eur_dh_mom) AS amt_cv_eur_dh_mom,
    SUM(jy.gmv_eur_kitchen_yoy) AS gmv_eur_kitchen_yoy,
    SUM(jy.gmv_eur_kitchen_mom) AS gmv_eur_kitchen_mom,
    SUM(jy.gmv_eur_concept_yoy) AS gmv_eur_concept_yoy,
    SUM(jy.gmv_eur_concept_mom) AS gmv_eur_concept_mom,
    SUM(jy.amt_commission_eur_kitchen_yoy) AS amt_commission_eur_kitchen_yoy,
    SUM(jy.amt_commission_eur_kitchen_mom) AS amt_commission_eur_kitchen_mom,
    SUM(jy.amt_commission_eur_concept_yoy) AS amt_commission_eur_concept_yoy,
    SUM(jy.amt_commission_eur_concept_mom) AS amt_commission_eur_concept_mom,
    SUM(jy.total_order_kitchen_yoy) AS total_order_kitchen_yoy,
    SUM(jy.total_order_kitchen_mom) AS total_order_kitchen_mom,
    SUM(jy.total_order_concept_yoy) AS total_order_concept_yoy,
    SUM(jy.total_order_concept_mom) AS total_order_concept_mom,

    SUM(my.restaurants_new_dh_yoy) AS restaurants_new_dh_yoy,
    SUM(my.restaurants_new_dh_mom) AS restaurants_new_dh_mom,
    SUM(my.restaurants_online_dh_yoy) AS restaurants_online_dh_yoy,
    SUM(my.restaurants_online_dh_mom) AS restaurants_online_dh_mom,
    SUM(my.restaurants_churned_dh_yoy) AS restaurants_churned_dh_yoy,
    SUM(my.restaurants_churned_dh_mom) AS restaurants_churned_dh_mom,
    SUM(my.restaurants_online_zero_orders_dh_yoy) AS restaurants_online_zero_orders_dh_yoy,
    SUM(my.restaurants_online_zero_orders_dh_mom) AS restaurants_online_zero_orders_dh_mom,
    SUM(my.darkstore_online_od_yoy) AS darkstore_online_od_yoy,
    SUM(my.darkstore_online_od_mom) AS darkstore_online_od_mom,
    SUM(my.darkstore_new_od_yoy) AS darkstore_new_od_yoy,
    SUM(my.darkstore_new_od_mom) AS darkstore_new_od_mom,
    SUM(my.darkstore_churned_od_yoy) AS darkstore_churned_od_yoy,
    SUM(my.darkstore_churned_od_mom) AS darkstore_churned_od_mom,
    SUM(my.darkstore_online_zero_orders_od_yoy) AS darkstore_online_zero_orders_od_yoy,
    SUM(my.darkstore_online_zero_orders_od_mom) AS darkstore_online_zero_orders_od_mom,
    SUM(my.vertical_online_od_yoy) AS vertical_online_od_yoy,
    SUM(my.vertical_online_od_mom) AS vertical_online_od_mom,
    SUM(my.vertical_new_od_yoy) AS vertical_new_od_yoy,
    SUM(my.vertical_new_od_mom) AS vertical_new_od_mom,
    SUM(my.vertical_churned_od_yoy) AS vertical_churned_od_yoy,
    SUM(my.vertical_churned_od_mom) AS vertical_churned_od_mom,
    SUM(my.vertical_online_zero_orders_od_yoy) AS vertical_online_zero_orders_od_yoy,
    SUM(my.vertical_online_zero_orders_od_mom) AS vertical_online_zero_orders_od_mom,

    SUM(my.restaurant_online_od_yoy) AS restaurant_online_od_yoy,
    SUM(my.restaurant_online_od_mom) AS restaurant_online_od_mom,
    SUM(my.restaurant_new_od_yoy) AS restaurant_new_od_yoy,
    SUM(my.restaurant_new_od_mom) AS restaurant_new_od_mom,
    SUM(my.restaurant_churned_od_yoy) AS restaurant_churned_od_yoy,
    SUM(my.restaurant_churned_od_mom) AS restaurant_churned_od_mom,
    SUM(my.restaurant_online_zero_orders_od_yoy) AS restaurant_online_zero_orders_od_yoy,
    SUM(my.restaurant_online_zero_orders_od_mom) AS restaurant_online_zero_orders_od_mom,

    SUM(my.online_mp_yoy) AS online_mp_yoy,
    SUM(my.online_mp_mom) AS online_mp_mom,
    SUM(my.new_mp_yoy) AS new_mp_yoy,
    SUM(my.new_mp_mom) AS new_mp_mom,
    SUM(my.churned_mp_yoy) AS churned_mp_yoy,
    SUM(my.churned_mp_mom) AS churned_mp_mom,
    SUM(my.online_zero_orders_mp_yoy) AS online_zero_orders_mp_yoy,
    SUM(my.online_zero_orders_mp_mom) AS online_zero_orders_mp_mom,

    SUM(my.concept_online_yoy) AS concept_online_yoy,
    SUM(my.concept_online_mom) AS concept_online_mom,
    SUM(my.concept_new_yoy) AS concept_new_yoy,
    SUM(my.concept_new_mom) AS concept_new_mom,
    SUM(my.concept_churned_yoy) AS concept_churned_yoy,
    SUM(my.concept_churned_mom) AS concept_churned_mom,
    SUM(my.concept_online_zero_orders_yoy) AS concept_online_zero_orders_yoy,
    SUM(my.concept_online_zero_orders_mom) AS concept_online_zero_orders_mom,

    SUM(my.kitchen_restaurant_online_yoy) AS kitchen_restaurant_online_yoy,
    SUM(my.kitchen_restaurant_online_mom) AS kitchen_restaurant_online_mom,
    SUM(my.kitchen_restaurant_new_yoy) AS kitchen_restaurant_new_yoy,
    SUM(my.kitchen_restaurant_new_mom) AS kitchen_restaurant_new_mom,
    SUM(my.kitchen_restaurant_churned_yoy) AS kitchen_restaurant_churned_yoy,
    SUM(my.kitchen_restaurant_churned_mom) AS kitchen_restaurant_churned_mom,
    SUM(my.kitchen_restaurant_online_zero_orders_yoy) AS kitchen_restaurant_online_zero_orders_yoy,
    SUM(my.kitchen_restaurant_online_zero_orders_mom) AS kitchen_restaurant_online_zero_orders_mom,

    SUM(my.kitchen_online_yoy) AS kitchen_online_yoy,
    SUM(my.kitchen_online_mom) AS kitchen_online_mom,
    SUM(my.kitchen_new_yoy) AS kitchen_new_yoy,
    SUM(my.kitchen_new_mom) AS kitchen_new_mom,
    SUM(my.kitchen_churned_yoy) AS kitchen_churned_yoy,
    SUM(my.kitchen_churned_mom) AS kitchen_churned_mom,
    SUM(my.kitchen_online_zero_orders_yoy) AS kitchen_online_zero_orders_yoy,
    SUM(my.kitchen_online_zero_orders_mom) AS kitchen_online_zero_orders_mom,

    SUM(my.converted_rating_concept_yoy) AS converted_rating_concept_yoy,
    SUM(my.converted_rating_concept_mom) AS converted_rating_concept_mom,
    SUM(my.rated_order_count_concept_yoy) AS rated_order_count_concept_yoy,
    SUM(my.rated_order_count_concept_mom) AS rated_order_count_concept_mom,

    SUM(my.converted_rating_kitchen_yoy) AS converted_rating_kitchen_yoy,
    SUM(my.converted_rating_kitchen_mom) AS converted_rating_kitchen_mom,
    SUM(my.rated_order_count_kitchen_yoy) AS rated_order_count_kitchen_yoy,
    SUM(my.rated_order_count_kitchen_mom) AS rated_order_count_kitchen_mom,



    SUM(my.converted_rating_od_darkstore_yoy) AS converted_rating_od_darkstore_yoy,
    SUM(my.converted_rating_od_darkstore_mom) AS converted_rating_od_darkstore_mom,
    SUM(my.rated_order_count_od_darkstore_yoy) AS rated_order_count_od_darkstore_yoy,
    SUM(my.rated_order_count_od_darkstore_mom) AS rated_order_count_od_darkstore_mom,
    SUM(my.nps_ao_promoters_od_darkstore_yoy) AS nps_ao_promoters_od_darkstore_yoy,
    SUM(my.nps_ao_promoters_od_darkstore_mom) AS nps_ao_promoters_od_darkstore_mom,
    SUM(my.nps_ao_detractors_od_darkstore_yoy) AS nps_ao_detractors_od_darkstore_yoy,
    SUM(my.nps_ao_detractors_od_darkstore_mom) AS nps_ao_detractors_od_darkstore_mom,
    SUM(my.nps_ao_responses_od_darkstore_yoy) AS nps_ao_responses_od_darkstore_yoy,
    SUM(my.nps_ao_responses_od_darkstore_mom) AS nps_ao_responses_od_darkstore_mom,
    SUM(my.converted_rating_od_vertical_yoy) AS converted_rating_od_vertical_yoy,
    SUM(my.converted_rating_od_vertical_mom) AS converted_rating_od_vertical_mom,
    SUM(my.rated_order_count_od_vertical_yoy) AS rated_order_count_od_vertical_yoy,
    SUM(my.rated_order_count_od_vertical_mom) AS rated_order_count_od_vertical_mom,
    SUM(my.nps_ao_promoters_od_vertical_yoy) AS nps_ao_promoters_od_vertical_yoy,
    SUM(my.nps_ao_promoters_od_vertical_mom) AS nps_ao_promoters_od_vertical_mom,
    SUM(my.nps_ao_detractors_od_vertical_yoy) AS nps_ao_detractors_od_vertical_yoy,
    SUM(my.nps_ao_detractors_od_vertical_mom) AS nps_ao_detractors_od_vertical_mom,
    SUM(my.nps_ao_responses_od_vertical_yoy) AS nps_ao_responses_od_vertical_yoy,
    SUM(my.nps_ao_responses_od_vertical_mom) AS nps_ao_responses_od_vertical_mom,
    SUM(my.converted_rating_mp_yoy) AS converted_rating_mp_yoy,
    SUM(my.converted_rating_mp_mom) AS converted_rating_mp_mom,
    SUM(my.rated_order_count_mp_yoy) AS rated_order_count_mp_yoy,
    SUM(my.rated_order_count_mp_mom) AS rated_order_count_mp_mom,
    SUM(my.nps_ao_promoters_mp_yoy) AS nps_ao_promoters_mp_yoy,
    SUM(my.nps_ao_promoters_mp_mom) AS nps_ao_promoters_mp_mom,
    SUM(my.nps_ao_detractors_mp_yoy) AS nps_ao_detractors_mp_yoy,
    SUM(my.nps_ao_detractors_mp_mom) AS nps_ao_detractors_mp_mom,
    SUM(my.nps_ao_responses_mp_yoy) AS nps_ao_responses_mp_yoy,
    SUM(my.nps_ao_responses_mp_mom) AS nps_ao_responses_mp_mom,
    SUM(my.converted_rating_od_restaurants_yoy) AS converted_rating_od_restaurants_yoy,
    SUM(my.converted_rating_od_restaurants_mom) AS converted_rating_od_restaurants_mom,
    SUM(my.rated_order_count_od_restaurants_yoy) AS rated_order_count_od_restaurants_yoy,
    SUM(my.rated_order_count_od_restaurants_mom) AS rated_order_count_od_restaurants_mom,
    SUM(my.nps_ao_promoters_od_restaurants_yoy) AS nps_ao_promoters_od_restaurants_yoy,
    SUM(my.nps_ao_promoters_od_restaurants_mom) AS nps_ao_promoters_od_restaurants_mom,
    SUM(my.nps_ao_detractors_od_restaurants_yoy) AS nps_ao_detractors_od_restaurants_yoy,
    SUM(my.nps_ao_detractors_od_restaurants_mom) AS nps_ao_detractors_od_restaurants_mom,
    SUM(my.nps_ao_responses_od_restaurants_yoy) AS nps_ao_responses_od_restaurants_yoy,
    SUM(my.nps_ao_responses_od_restaurants_mom) AS nps_ao_responses_od_restaurants_mom,

    SUM(jy.gmv_eur_od_darkstore_yoy) AS gmv_eur_od_darkstore_yoy,
    SUM(jy.gmv_eur_od_darkstore_mom) AS gmv_eur_od_darkstore_mom,
    SUM(jy.amt_commission_eur_od_darkstore_yoy) AS amt_commission_eur_od_darkstore_yoy,
    SUM(jy.amt_commission_eur_od_darkstore_mom) AS amt_commission_eur_od_darkstore_mom,
    SUM(jy.successful_order_od_darkstore_yoy) AS successful_order_od_darkstore_yoy,
    SUM(jy.successful_order_od_darkstore_mom) AS successful_order_od_darkstore_mom,
    SUM(jy.order_value_od_darkstore_yoy) AS order_value_od_darkstore_yoy,
    SUM(jy.order_value_od_darkstore_mom) AS order_value_od_darkstore_mom,
    SUM(jy.delivery_fee_od_darkstore_yoy) AS delivery_fee_od_darkstore_yoy,
    SUM(jy.delivery_fee_od_darkstore_mom) AS delivery_fee_od_darkstore_mom,
    SUM(jy.failed_orders_od_darkstore_yoy) AS failed_orders_od_darkstore_yoy,
    SUM(jy.failed_orders_od_darkstore_mom) AS failed_orders_od_darkstore_mom,
    SUM(jy.acquisitions_od_darkstore_yoy) AS acquisitions_od_darkstore_yoy,
    SUM(jy.acquisitions_od_darkstore_mom) AS acquisitions_od_darkstore_mom,
    SUM(jy.voucher_order_od_darkstore_yoy) AS voucher_order_od_darkstore_yoy,
    SUM(jy.voucher_order_od_darkstore_mom) AS voucher_order_od_darkstore_mom,
    SUM(jcy.acquisitions_od_darkstore_cumulative_yoy) AS customer_base_od_darkstore_yoy,
    SUM(jcy.acquisitions_od_darkstore_cumulative_mom) AS customer_base_od_darkstore_mom,

    SUM(jy.successful_orders_od_vertical_yoy) AS successful_orders_od_vertical_yoy,
    SUM(jy.successful_orders_od_vertical_mom) AS successful_orders_od_vertical_mom,
    SUM(jy.gmv_od_vertical_yoy) AS gmv_od_vertical_yoy,
    SUM(jy.gmv_od_vertical_mom) AS gmv_od_vertical_mom,
    SUM(jy.amt_commission_eur_od_vertical_yoy) AS amt_commission_eur_od_vertical_yoy,
    SUM(jy.amt_commission_eur_od_vertical_mom) AS amt_commission_eur_od_vertical_mom,
    SUM(jy.order_value_od_vertical_yoy) AS order_value_od_vertical_yoy,
    SUM(jy.order_value_od_vertical_mom) AS order_value_od_vertical_mom,
    SUM(jy.delivery_fee_od_vertical_yoy) AS delivery_fee_od_vertical_yoy,
    SUM(jy.delivery_fee_od_vertical_mom) AS delivery_fee_od_vertical_mom,
    SUM(jy.failed_orders_od_vertical_yoy) AS failed_orders_od_vertical_yoy,
    SUM(jy.failed_orders_od_vertical_mom) AS failed_orders_od_vertical_mom,
    SUM(jy.acquisitions_od_vertical_yoy) AS acquisitions_od_vertical_yoy,
    SUM(jy.acquisitions_od_vertical_mom) AS acquisitions_od_vertical_mom,
    SUM(jy.voucher_orders_od_vertical_yoy) AS voucher_orders_od_vertical_yoy,
    SUM(jy.voucher_orders_od_vertical_mom) AS voucher_orders_od_vertical_mom,
    SUM(jcy.acquisitions_od_vertical_cumulative_yoy) AS customer_base_od_vertical_yoy,
    SUM(jcy.acquisitions_od_vertical_cumulative_mom) AS customer_base_od_vertical_mom,

    SUM(jy.successful_orders_mp_yoy) AS successful_orders_mp_yoy,
    SUM(jy.successful_orders_mp_mom) AS successful_orders_mp_mom,
    SUM(jy.gmv_mp_yoy) AS gmv_mp_yoy,
    SUM(jy.gmv_mp_mom) AS gmv_mp_mom,
    SUM(jy.amt_commission_eur_mp_yoy) AS amt_commission_eur_mp_yoy,
    SUM(jy.amt_commission_eur_mp_mom) AS amt_commission_eur_mp_mom,
    SUM(jy.order_value_mp_yoy) AS order_value_mp_yoy,
    SUM(jy.order_value_mp_mom) AS order_value_mp_mom,
    SUM(jy.delivery_fee_mp_yoy) AS delivery_fee_mp_yoy,
    SUM(jy.delivery_fee_mp_mom) AS delivery_fee_mp_mom,
    SUM(jy.failed_orders_mp_yoy) AS failed_orders_mp_yoy,
    SUM(jy.failed_orders_mp_mom) AS failed_orders_mp_mom,
    SUM(jy.acquisitions_mp_yoy) AS acquisitions_mp_yoy,
    SUM(jy.acquisitions_mp_mom) AS acquisitions_mp_mom,
    SUM(jy.voucher_orders_mp_yoy) AS voucher_orders_mp_yoy,
    SUM(jy.voucher_orders_mp_mom) AS voucher_orders_mp_mom,
    SUM(jcy.acquisitions_mp_cumulative_yoy) AS customer_base_mp_yoy,
    SUM(jcy.acquisitions_mp_cumulative_mom) AS customer_base_mp_mom,

    SUM(jy.successful_orders_od_restaurants_yoy) AS successful_orders_od_restaurants_yoy,
    SUM(jy.successful_orders_od_restaurants_mom) AS successful_orders_od_restaurants_mom,
    SUM(jy.gmv_od_restaurants_yoy) AS gmv_od_restaurants_yoy,
    SUM(jy.gmv_od_restaurants_mom) AS gmv_od_restaurants_mom,
    SUM(jy.amt_commission_eur_od_restaurants_yoy) AS amt_commission_eur_od_restaurants_yoy,
    SUM(jy.amt_commission_eur_od_restaurants_mom) AS amt_commission_eur_od_restaurants_mom,
    SUM(jy.order_value_od_restaurants_yoy) AS order_value_od_restaurants_yoy,
    SUM(jy.order_value_od_restaurants_mom) AS order_value_od_restaurants_mom,
    SUM(jy.delivery_fee_od_restaurants_yoy) AS delivery_fee_od_restaurants_yoy,
    SUM(jy.delivery_fee_od_restaurants_mom) AS delivery_fee_od_restaurants_mom,
    SUM(jy.failed_orders_od_restaurants_yoy) AS failed_orders_od_restaurants_yoy,
    SUM(jy.failed_orders_od_restaurants_mom) AS failed_orders_od_restaurants_mom,
    SUM(jy.acquisitions_od_restaurants_yoy) AS acquisitions_od_restaurants_yoy,
    SUM(jy.acquisitions_od_restaurants_mom) AS acquisitions_od_restaurants_mom,
    SUM(jy.voucher_orders_od_restaurants_yoy) AS voucher_orders_od_restaurants_yoy,
    SUM(jy.voucher_orders_od_restaurants_mom) AS voucher_orders_od_restaurants_mom,
    SUM(jcy.acquisitions_od_restaurants_cumulative_yoy) AS customer_base_od_restaurants_yoy,
    SUM(jcy.acquisitions_od_restaurants_cumulative_mom) AS customer_base_od_restaurants_mom,

    SUM(j.amt_joker_eur_total) AS joker_revenue,
    SUM(jy.amt_joker_eur_total_yoy) AS joker_revenue_yoy,
    SUM(jy.amt_joker_eur_total_mom) AS joker_revenue_mom,
    SUM(jy.orders_failed_cancellation_yoy) AS orders_failed_cancellation_yoy,
    SUM(jy.orders_failed_cancellation_mom) AS orders_failed_cancellation_mom,
    SUM(jy.orders_failed_response_yoy) AS orders_failed_response_yoy,
    SUM(jy.orders_failed_response_mom) AS orders_failed_response_mom,
    SUM(jy.orders_failed_rejection_yoy) AS orders_failed_rejection_yoy,
    SUM(jy.orders_failed_rejection_mom) AS orders_failed_rejection_mom,
    SUM(jy.orders_failed_transmission_yoy) AS orders_failed_transmission_yoy,
    SUM(jy.orders_failed_transmission_mom) AS orders_failed_transmission_mom,
    SUM(jy.orders_failed_delivery_yoy) AS orders_failed_delivery_yoy,
    SUM(jy.orders_failed_delivery_mom) AS orders_failed_delivery_mom,
    SUM(jy.orders_failed_payment_yoy) AS orders_failed_payment_yoy,
    SUM(jy.orders_failed_payment_mom) AS orders_failed_payment_mom,
    SUM(jy.orders_failed_verification_yoy) AS orders_failed_verification_yoy,
    SUM(jy.orders_failed_verification_mom) AS orders_failed_verification_mom,
    SUM(jy.orders_failed_overall_yoy) + SUM(jy.orders_failed_other_yoy) AS orders_failed_other_yoy,
    SUM(jy.orders_failed_overall_mom) + SUM(jy.orders_failed_other_mom) AS orders_failed_other_mom,
    SUM(opsy.orders_delivery_20m_yoy) AS orders_delivery_20m_yoy,
    SUM(opsy.orders_delivery_20m_mom) AS orders_delivery_20m_mom,
    SUM(opsy.orders_promised_20_yoy) AS orders_promised_20_yoy,
    SUM(opsy.orders_promised_20_mom) AS orders_promised_20_mom,
    SUM(opsy.orders_promised_20_delivered_20_yoy) AS orders_promised_20_delivered_20_yoy,
    SUM(opsy.orders_promised_20_delivered_20_mom) AS orders_promised_20_delivered_20_mom,
    SUM(jy.groceries_gmv_yoy) AS groceries_gmv_yoy,
    SUM(jy.groceries_gmv_mom) AS groceries_gmv_mom,
    SUM(jy.groceries_orders_yoy) AS groceries_orders_yoy,
    SUM(jy.groceries_orders_mom) AS groceries_orders_mom,
    SUM(jy.discount_darkstore_yoy) AS discount_darkstore_yoy,
    SUM(jy.discount_darkstore_mom) AS discount_darkstore_mom,
    SUM(jy.discount_od_vertical_yoy) AS discount_od_vertical_yoy,
    SUM(jy.discount_od_vertical_mom) AS discount_od_vertical_mom,
    SUM(jy.discount_od_restaurant_yoy) AS discount_od_restaurant_yoy,
    SUM(jy.discount_od_restaurant_mom) AS discount_od_restaurant_mom,
    SUM(jy.discount_mp_yoy) AS discount_mp_yoy,
    SUM(jy.discount_mp_mom) AS discount_mp_mom,

    SUM(opsy.delivery_distance_darkstore_yoy) AS delivery_distance_darkstore_yoy,
    SUM(opsy.delivery_distance_darkstore_mom) AS delivery_distance_darkstore_mom,
    SUM(opsy.delivery_distance_od_vertical_yoy) AS delivery_distance_od_vertical_yoy,
    SUM(opsy.delivery_distance_od_vertical_mom) AS delivery_distance_od_vertical_mom,
    SUM(opsy.delivery_distance_od_restaurant_yoy) AS delivery_distance_od_restaurant_yoy,
    SUM(opsy.delivery_distance_od_restaurant_mom) AS delivery_distance_od_restaurant_mom,
    SUM(opsy.delivery_distance_mp_yoy) AS delivery_distance_mp_yoy,
    SUM(opsy.delivery_distance_mp_mom) AS delivery_distance_mp_mom,

    SUM(opsy.delivery_time_darkstore_yoy) AS delivery_time_darkstore_yoy,
    SUM(opsy.delivery_time_darkstore_mom) AS delivery_time_darkstore_mom,
    SUM(opsy.delivery_time_od_vertical_yoy) AS delivery_time_od_vertical_yoy,
    SUM(opsy.delivery_time_od_vertical_mom) AS delivery_time_od_vertical_mom,
    SUM(opsy.delivery_time_od_restaurant_yoy) AS delivery_time_od_restaurant_yoy,
    SUM(opsy.delivery_time_od_restaurant_mom) AS delivery_time_od_restaurant_mom,
    SUM(opsy.delivery_time_mp_yoy) AS delivery_time_mp_yoy,
    SUM(opsy.delivery_time_mp_mom) AS delivery_time_mp_mom,
    SUM(opsy.orders_courier_late_10m_darkstore_yoy) AS orders_courier_late_10m_darkstore_yoy,
    SUM(opsy.orders_courier_late_10m_darkstore_mom) AS orders_courier_late_10m_darkstore_mom,
    SUM(opsy.orders_courier_late_10m_vertical_yoy) AS orders_courier_late_10m_vertical_yoy,
    SUM(opsy.orders_courier_late_10m_vertical_mom) AS orders_courier_late_10m_vertical_mom,
    SUM(opsy.orders_courier_late_10m_restaurant_yoy) AS orders_courier_late_10m_restaurant_yoy,
    SUM(opsy.orders_courier_late_10m_restaurant_mom) AS orders_courier_late_10m_restaurant_mom,
    SUM(opsy.orders_courier_late_10m_mp_yoy) AS orders_courier_late_10m_mp_yoy,
    SUM(opsy.orders_courier_late_10m_mp_mom) AS orders_courier_late_10m_mp_mom,

    SUM(j.orders_failed_cancellation) AS orders_failed_cancellation,
    SUM(j.orders_failed_response) AS orders_failed_response, -- failed_verification and failed_delivery are falling into orders_failed_other
    SUM(j.orders_failed_rejection) AS orders_failed_rejection,
    SUM(j.orders_failed_transmission) AS orders_failed_transmission,
    SUM(j.orders_failed_delivery) AS orders_failed_delivery,
    SUM(j.orders_failed_payment) AS orders_failed_payment,
    SUM(j.orders_failed_verification) AS orders_failed_verification,
    SUM(j.orders_failed_overall)+ SUM(j.orders_failed_other) AS orders_failed_other,
    SUM(j.orders_failed_cancellation_dh) AS orders_failed_cancellation_dh,
    SUM(j.orders_failed_response_dh) AS orders_failed_response_dh, -- failed_verification and failed_delivery are falling into orders_failed_other
    SUM(j.orders_failed_rejection_dh) AS orders_failed_rejection_dh,
    SUM(j.orders_failed_transmission_dh) AS orders_failed_transmission_dh,
    SUM(j.orders_failed_delivery_dh) AS orders_failed_delivery_dh,
    SUM(j.orders_failed_payment_dh) AS orders_failed_payment_dh,
    SUM(j.orders_failed_verification_dh) AS orders_failed_verification_dh,
    SUM(j.orders_failed_overall_dh) + SUM(j.orders_failed_other_dh) AS orders_failed_other_dh,
    SUM(ops.orders_delivery_20m) AS orders_delivery_20m,
    SUM(ops.orders_promised_20) AS orders_promised_20,
    SUM(ops.orders_promised_20_delivered_20) AS orders_promised_20_delivered_20,
    SUM(j.groceries_orders) AS groceries_orders,
    SUM(j.groceries_gmv) AS groceries_gmv,
    SUM(j.discount_darkstore) AS discount_darkstore,
    SUM(j.discount_od_vertical) AS discount_od_vertical,
    SUM(j.discount_od_restaurant) AS discount_od_restaurant,
    SUM(j.discount_mp) AS discount_mp,
    SUM(ops.delivery_distance_darkstore) AS delivery_distance_darkstore,
    SUM(ops.delivery_distance_od_vertical) AS delivery_distance_od_vertical,
    SUM(ops.delivery_distance_od_restaurant) AS delivery_distance_od_restaurant,
    SUM(ops.delivery_distance_mp) AS delivery_distance_mp,
    SUM(ops.delivery_time_darkstore) AS delivery_time_darkstore,
    SUM(ops.delivery_time_od_vertical) AS delivery_time_od_vertical,
    SUM(ops.delivery_time_od_restaurant) AS delivery_time_od_restaurant,
    SUM(ops.delivery_time_mp) AS delivery_time_mp,
    SUM(ops.orders_courier_late_10m_darkstore) AS orders_courier_late_10m_darkstore,
    SUM(ops.orders_courier_late_10m_vertical) AS orders_courier_late_10m_vertical,
    SUM(ops.orders_courier_late_10m_restaurant) AS orders_courier_late_10m_restaurant,
    SUM(ops.orders_courier_late_10m_mp) AS orders_courier_late_10m_mp,
    SUM(cv.all_transactions) AS all_transactions,
    SUM(cv.all_visits) AS all_visits,
    SUM(cv_yoy.all_transactions) AS all_transactions_yoy,
    SUM(cv_yoy.all_visits) AS all_visits_yoy,
    SUM(cv_mom.all_transactions) AS all_transactions_mom,
    SUM(cv_mom.all_visits) AS all_visits_mom,
    SUM(rca.all_visits_mp) AS all_visits_mp,
    SUM(rca.all_transactions_mp) AS all_transactions_mp,
    SUM(rca.all_visits_darkstore) AS all_visits_darkstore,
    SUM(rca.all_transactions_darkstore) AS all_transactions_darkstore,
    SUM(rca.all_visits_od_vertical) AS all_visits_od_vertical,
    SUM(rca.all_transactions_od_vertical) AS all_transactions_od_vertical,
    SUM(rca.all_visits_od_restaurant) AS all_visits_od_restaurant,
    SUM(rca.all_transactions_od_restaurant) AS all_transactions_od_restaurant,

    SUM(rcamy.all_visits_mp_yoy) AS all_visits_mp_yoy,
    SUM(rcamy.all_visits_mp_mom) AS all_visits_mp_mom,
    SUM(rcamy.all_transactions_mp_yoy) AS all_transactions_mp_yoy,
    SUM(rcamy.all_transactions_mp_mom) AS all_transactions_mp_mom,
    SUM(rcamy.all_visits_darkstore_yoy) AS all_visits_darkstore_yoy,
    SUM(rcamy.all_visits_darkstore_mom) AS all_visits_darkstore_mom,
    SUM(rcamy.all_transactions_darkstore_yoy) AS all_transactions_darkstore_yoy,
    SUM(rcamy.all_transactions_darkstore_mom) AS all_transactions_darkstore_mom,
    SUM(rcamy.all_visits_od_vertical_yoy) AS all_visits_od_vertical_yoy,
    SUM(rcamy.all_visits_od_vertical_mom) AS all_visits_od_vertical_mom,
    SUM(rcamy.all_transactions_od_vertical_yoy) AS all_transactions_od_vertical_yoy,
    SUM(rcamy.all_transactions_od_vertical_mom) AS all_transactions_od_vertical_mom,
    SUM(rcamy.all_visits_od_restaurant_yoy) AS all_visits_od_restaurant_yoy,
    SUM(rcamy.all_visits_od_restaurant_mom) AS all_visits_od_restaurant_mom,
    SUM(rcamy.all_transactions_od_restaurant_yoy) AS all_transactions_od_restaurant_yoy,
    SUM(rcamy.all_transactions_od_restaurant_mom) AS all_transactions_od_restaurant_mom,

    SUM(re.customers_returned_od_darkstore) AS customers_returned_od_darkstore,
    SUM(re.customers_total_od_darkstore) AS customers_total_od_darkstore,
    SUM(re.customers_returned_od_vertical) AS customers_returned_od_vertical,
    SUM(re.customers_total_od_vertical) AS customers_total_od_vertical,
    SUM(re.customers_returned_mp) AS customers_returned_mp,
    SUM(re.customers_total_mp) AS customers_total_mp,
    SUM(re.customers_returned_od_restaurant) AS customers_returned_od_restaurant,
    SUM(re.customers_total_od_restaurant) AS customers_total_od_restaurant,
    SUM(remy.customers_returned_od_darkstore_yoy) AS customers_returned_od_darkstore_yoy,
    SUM(remy.customers_returned_od_darkstore_mom) AS customers_returned_od_darkstore_mom,
    SUM(remy.customers_total_od_darkstore_yoy) AS customers_total_od_darkstore_yoy,
    SUM(remy.customers_total_od_darkstore_mom) AS customers_total_od_darkstore_mom,
    SUM(remy.customers_returned_od_vertical_yoy) AS customers_returned_od_vertical_yoy,
    SUM(remy.customers_returned_od_vertical_mom) AS customers_returned_od_vertical_mom,
    SUM(remy.customers_total_od_vertical_yoy) AS customers_total_od_vertical_yoy,
    SUM(remy.customers_total_od_vertical_mom) AS customers_total_od_vertical_mom,
    SUM(remy.customers_returned_mp_yoy) AS customers_returned_mp_yoy,
    SUM(remy.customers_returned_mp_mom) AS customers_returned_mp_mom,
    SUM(remy.customers_total_mp_yoy) AS customers_total_mp_yoy,
    SUM(remy.customers_total_mp_mom) AS customers_total_mp_mom,
    SUM(remy.customers_returned_od_restaurant_yoy) AS customers_returned_od_restaurant_yoy,
    SUM(remy.customers_returned_od_restaurant_mom) AS customers_returned_od_restaurant_mom,
    SUM(remy.customers_total_od_restaurant_yoy) AS customers_total_od_restaurant_yoy,
    SUM(remy.customers_total_od_restaurant_mom) AS customers_total_od_restaurant_mom,
    MAX(cc.population) AS population
FROM construct AS c
LEFT JOIN orders_cumulative o
    ON c.source_id = o.source_id
    AND LOWER(TRIM(c.city)) = LOWER(TRIM(o.city))
    AND c.report_month = o.report_month
LEFT JOIN orders_cumulative_mom_yoy omy
    ON omy.source_id = c.source_id
    AND LOWER(TRIM(omy.city)) = LOWER(TRIM(c.city))
    AND omy.report_month = c.report_month
LEFT JOIN joker j
    ON j.source_id = c.source_id
    AND LOWER(TRIM(j.city)) = LOWER(TRIM(c.city))
    AND j.report_month = c.report_month
LEFT JOIN joker_yoy jy
    ON jy.source_id = c.source_id
    AND LOWER(TRIM(jy.city)) = LOWER(TRIM(c.city))
    AND jy.report_month = c.report_month
LEFT JOIN ops_data ops
    ON c.source_id = ops.source_id
    AND LOWER(TRIM(c.city)) = LOWER(TRIM(ops.city))
    AND c.report_month = ops.report_month
LEFT JOIN ops_data_yoy opsy
    ON opsy.source_id = c.source_id
    AND LOWER(TRIM(opsy.city)) = LOWER(TRIM(c.city))
    AND opsy.report_month = c.report_month
LEFT JOIN joker_cumulative jc
    ON jc.source_id = c.source_id
    AND LOWER(TRIM(jc.city)) = LOWER(TRIM(c.city))
    AND jc.report_month = c.report_month
LEFT JOIN joker_cumulative_yoy jcy
    ON jcy.source_id = c.source_id
    AND LOWER(TRIM(jcy.city)) = LOWER(TRIM(c.city))
    AND jcy.report_month = c.report_month
LEFT JOIN monthly_online_restaurants_agg mor
    ON mor.source_id = c.source_id
    AND LOWER(TRIM(mor.city)) = LOWER(TRIM(c.city))
    AND mor.report_month = c.report_month
LEFT JOIN online_restaurants_mom_yoy my
    ON my.source_id = c.source_id
    AND LOWER(TRIM(my.city)) = LOWER(TRIM(c.city))
    AND my.report_month = c.report_month
LEFT JOIN cvr cv
    ON cv.source_id = c.source_id
    AND LOWER(TRIM(cv.city)) = LOWER(TRIM(c.city))
    AND cv.report_month = c.report_month
LEFT JOIN cvr cv_mom
    ON cv_mom.source_id = c.source_id
    AND LOWER(TRIM(cv_mom.city)) = LOWER(TRIM(c.city))
    AND cv_mom.report_month = ADD_MONTHS(c.report_month, -1)
LEFT JOIN cvr cv_yoy
    ON cv_yoy.source_id = c.source_id
    AND LOWER(TRIM(cv_yoy.city)) = LOWER(TRIM(c.city))
    AND cv_yoy.report_month = ADD_MONTHS(c.report_month, -12)
LEFT JOIN top_10_brands tb
    ON tb.source_id = c.source_id
    AND LOWER(TRIM(tb.city)) = LOWER(TRIM(c.city))
    AND tb.report_month = c.report_month
LEFT JOIN rca rca
    ON rca.source_id = c.source_id
    AND LOWER(TRIM(rca.city)) = LOWER(TRIM(c.city))
    AND rca.report_month = c.report_month
LEFT JOIN rca_mom_yoy AS rcamy
    ON rcamy.source_id = c.source_id
    AND LOWER(TRIM(rcamy.city)) = LOWER(TRIM(c.city))
    AND rcamy.report_month = c.report_month
LEFT JOIN reorders re
    ON re.source_id = c.source_id
    AND LOWER(TRIM(re.city)) = LOWER(TRIM(c.city))
    AND re.report_month = c.report_month
LEFT JOIN reorders_mom_yoy remy
    ON remy.source_id = c.source_id
    AND LOWER(TRIM(remy.city)) = LOWER(TRIM(c.city))
    AND remy.report_month = c.report_month
LEFT JOIN dwh_bl.city_ranking ct
    ON ct.source_id = c.source_id
    AND LOWER(TRIM(ct.city)) = LOWER(TRIM(c.city))
LEFT JOIN dwh_bl.city_coords cc
    ON cc.country_iso = c.country_iso
    AND (LOWER(TRIM(c.city)) = LOWER(TRIM(cc.city)) OR LOWER(TRIM(c.city)) = LOWER(TRIM(cc.city_plain)))
LEFT JOIN competitive_cities comp
    ON LOWER(TRIM(comp.city_new)) = LOWER(TRIM(c.city))
    AND comp.source_id = c.source_id
WHERE c.is_active IS TRUE
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;