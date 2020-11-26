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



create or replace view `get-data-team.tenjin_dv_test.view_tenjin_ironsource_metrics`   as
with ironsource_metrics as
  (select
    --dimensions
    country,
    cast(applicationId as string) as site_id,
    campaignName as campaign_name,
    cast(date as date) as date,
    -- in case name changes, take only one of them.
    max(applicationName) as application_name,
    max(os) as os,
    --metrics
    sum(spend) as spend_is,
    sum(completions) as completions_is,
    sum(impressions) as impressions_is,
    sum(clicks) as clicks_is,
    sum(installs) as installs_is
  from `get-data-team.tenjin_dv_test.ironsource_metrics`
  group by 1,2,3,4)
,tenjin_metrics as
(select
  --dims
  country,
  site_id,
  c.name as campaign_name,
  date ,
  max(n.name) as ad_network_name,
  max(a.name) as game_name,
  --metrics:
  sum(tracked_impressions) as tracked_impressions,
  sum(tracked_clicks) as tracked_clicks,
  sum(tracked_installs) as tracked_installs,
  sum(reported_clicks) as reported_clicks,
  sum(reported_spend) as reported_spend,
  sum(iap_revenue) as iap_revenue,
  sum(reported_installs) as reported_installs,
  sum(reported_impressions) as reported_impressions,
  sum(publisher_ad_revenue) as publisher_ad_revenue
  from `get-data-team.tenjin_dv_test.reporting_metrics` r
left join `get-data-team.tenjin_dv_test.campaigns` c
  on c.id = r.campaign_id
left join `get-data-team.tenjin_dv_test.ad_networks` n
  on n.id = r.ad_network_id
left join `get-data-team.tenjin_dv_test.apps` a
  on a.id = r.app_id
group by 1,2,3,4),
tenjin_cohort_metrics as
  (select
  --dims
  country,
  site_id,
  c.name as campaign_name,
  install_date as date ,
  --metrics:
  sum(case when days_since_install <1 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d1,
  sum(case when days_since_install <2 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d2,
  sum(case when days_since_install <3 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d3,
  sum(case when days_since_install <4 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d4,
  sum(case when days_since_install <5 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d5,
  sum(case when days_since_install <6 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d6,
  sum(case when days_since_install <7 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d7,
  sum(case when days_since_install <14 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d14,
  sum(case when days_since_install <30 then coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0) else null end) as cohort_revenue_d30,
  sum(coalesce(iap_revenue, 0) +  coalesce(cast(publisher_ad_revenue as numeric),0)) as cohort_revenue_total
  from `get-data-team.tenjin_dv_test.absolute_cohort_metrics` r
  left join `get-data-team.tenjin_dv_test.campaigns` c
    on c.id = r.source_campaign_id
  group by 1,2,3,4
  )
select
    im.application_name,
    im.os,
    tm.*,
    im.spend_is,
    im.completions_is,
    im.impressions_is,
    im.clicks_is,
    im.installs_is,
    tcm.cohort_revenue_d1,
    tcm.cohort_revenue_d2,
    tcm.cohort_revenue_d3,
    tcm.cohort_revenue_d4,
    tcm.cohort_revenue_d5,
    tcm.cohort_revenue_d6,
    tcm.cohort_revenue_d7,
    tcm.cohort_revenue_d14,
    tcm.cohort_revenue_d30,
     tcm.cohort_revenue_total
from tenjin_metrics as tm
inner join ironsource_metrics as im
on im.country = tm.country
and im.site_id = tm.site_id
and im.campaign_name = tm.campaign_name
and im.date = tm.date
left join tenjin_cohort_metrics as tcm
on tcm.country = tm.country
and tcm.site_id = tm.site_id
and tcm.campaign_name = tm.campaign_name
and tcm.date = tm.date