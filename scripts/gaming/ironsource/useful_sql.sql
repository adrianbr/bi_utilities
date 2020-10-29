

/*create or replace view `get-data-team.tenjin_dv_test.view_ironsource_revenue` as
SELECT *, _TABLE_SUFFIX as table_id,
 cast(SPLIT(_TABLE_SUFFIX, '_')[OFFSET(0)] as date) as dataset_date,
 SPLIT(_TABLE_SUFFIX, '_')[OFFSET(1)] as appkey
FROM `get-data-team.tenjin_dv_test.ironsource_revenue_*` ;*/


-- example join of the data sets - could do a cohort analysis with the revenue over time or something like that, by any of the tenjin event dimensions
-- could also leverage the ironsource ab testing (not sure what that is)
with daily_user_ir_revenue as
  (select advertising_id,
      sum( revenue) as revenue
  from `get-data-team.tenjin_dv_test.view_ironsource_revenue` ir),
tenjin_installs as
  (select  distinct advertising_id, acquired_at
  FROM     tenjin.events e
  WHERE e.event = 'open'
  )
select i.acquired_at,
  sum(revenue) as revenue,
  count(distinct i.advertising_id) as installs,
  sum(revenue)/count(distinct i.advertising_id) as revenue_per_install
from tenjin_installs i
left join daily_user_ir_revenue r
  on r.advertising_id = i.advertising_id
