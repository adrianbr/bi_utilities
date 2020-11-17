create or replace view `get-data-team.tenjin_dv_test.absolute_cohort_metrics` as
with
events_flat_date as
  (select *,
    date(created_at) AS created_date
  FROM `get-data-team.tenjin_dv_test.events` events
  WHERE date(created_at) BETWEEN date('2019-08-01') and date_add(current_date, interval 1 day)
),
 daily_iap_revenue as
    (SELECT
           created_date,
           date(acquired_at) AS install_date,
           date_diff(created_date, date(acquired_at), DAY) AS days_since_install,
           source_campaign_id,
           site_id,
           app_id,
           country,
           COUNT(CASE WHEN event = 'open' THEN 1 ELSE NULL END) AS sessions,
           COUNT(distinct(CASE WHEN events.event = 'open' THEN coalesce(advertising_id, developer_device_id) END)) AS dau,
           SUM(COUNT(CASE WHEN events.event = 'open' THEN 1 END)) over (partition by created_date, events.app_id, coalesce(country, 'un')) AS app_country_sessions,
           SUM(CASE WHEN event_type = 'purchase' AND (purchase_state = 0 OR purchase_state = 3) THEN events.revenue ELSE 0 END) AS iap_revenue
       FROM
       	events_flat_date as events
       GROUP BY 1,2,3,4,5,6,7
       ),
daily_ad_revenue as
  (SELECT
       date,
       app_id,
       country,
       SUM(coalesce(daily_ad_revenue.revenue, 0)) AS revenue
   FROM
       	`get-data-team.tenjin_dv_test.daily_ad_revenue` daily_ad_revenue
   INNER JOIN `get-data-team.tenjin_dv_test.publisher_apps`  publisher_apps
   ON publisher_apps.id = daily_ad_revenue.publisher_app_id
   WHERE date BETWEEN '2019-08-01' AND current_date
   GROUP BY 1,2,3)
SELECT
   install_date,
   days_since_install,
   site_id,
   source_campaign_id,
   daily_iap_revenue.app_id,
   coalesce(daily_ad_revenue.country, daily_iap_revenue.country, 'undefined') AS country,
   SUM(daily_ad_revenue.revenue * daily_iap_revenue.sessions / nullif(daily_iap_revenue.app_country_sessions, 0)) / 100.0 AS publisher_ad_revenue,
   SUM(daily_iap_revenue.iap_revenue) / 100.0 AS iap_revenue,
   SUM(daily_iap_revenue.dau) AS daily_active_users
FROM daily_ad_revenue
INNER JOIN daily_iap_revenue
   ON daily_iap_revenue.install_date = daily_ad_revenue.date
   AND daily_iap_revenue.app_id = daily_ad_revenue.app_id
   AND daily_iap_revenue.country = daily_ad_revenue.country
GROUP BY 1,2,3,4,5,6;


