-- each row is a unique purchaser of boost summarizing whether the profile repurchased in same week
-- and hours between repurchase if they repurchased

set start_d = '2022-05-25';
set end_d = '2022-05-31';

create or replace table kelvinc.public.boost_purchaser_20220525_0531 as
(
  with purch_evt as
  (
    select
      to_timestamp(evt.timestamp) as purchase_ts
      ,evt.profile_id
      ,evt.app_category
      ,evt.os_platform
      ,row_number() over (partition by evt.profile_id order by evt.timestamp) as rn
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    where evt.ds between $start_d and $end_d
      and
      (
        lower(evt.event_name) in ('purchase_completed')
        and lower(to_variant(parse_json(params):feature)) = 'boost'
      )
  )
  select
      p1.profile_id
      ,mode(p1.app_category) as app_category
      ,mode(p1.os_platform) as os_platform
      ,count(1) as n_purchases
      ,avg(datediff('min',p1.purchase_ts,p2.purchase_ts)/60.0) as n_hr_btw_purch_avg
  from purch_evt p1
  left join purch_evt p2 on p1.profile_id = p2.profile_id and p1.rn+1 = p2.rn
  group by 1
)
