create view `get-data-team.tenjin_dv_test.view_tenjin_ironsource_metrics` as
with ironsource_metrics as
  (select
    --dimensions
    country,
    cast(applicationId as string) as site_id,
    campaignName as campaign_name,
    cast(date as date) as date,
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
group by 1,2,3,4)
select
    tm.*,
    im.spend_is,
    im.completions_is,
    im.impressions_is,
    im.clicks_is,
    im.installs_is
from tenjin_metrics as tm
left join ironsource_metrics as im
on im.country = tm.country
and im.site_id = tm.site_id
and im.campaign_name = tm.campaign_name
and im.date = tm.date
--limit 1000

