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
  ,boost_report_summ as
  (
    select
      profile_id
      ,avg(views_count) as views_count
      ,avg(taps_count) as taps_count
      ,avg(chats_count) as chats_count
      ,avg(case when rn_latest > 1 or rn_latest = rn then views_count end) as views_count_pred
      ,avg(case when rn_latest > 1 or rn_latest = rn then taps_count end) as taps_count_pred
      ,avg(case when rn_latest > 1 or rn_latest = rn then chats_count end) as chats_count_pred
    from
    (
      select
        bse.profile_id
        ,bse.views_count
        ,bse.taps_count
        ,bse.chats_count
        ,row_number() over (partition by bse.profile_id order by bse.start_time desc) as rn_latest
        ,row_number() over (partition by bse.profile_id order by bse.start_time) as rn
      from "AURORA_GRINDR"."RAW"."BOOST_SESSION" bse
      where to_date(start_time) >= $start_d and to_date(end_time) <= $end_d
    )
    group by 1
  )
  select
      p1.profile_id
      ,mode(p1.app_category) as app_category
      ,mode(p1.os_platform) as os_platform
      ,count(1) as n_purchases
      ,avg(datediff('min',p1.purchase_ts,p2.purchase_ts)/60.0) as n_hr_btw_purch_avg
      ,bse.views_count
      ,bse.taps_count
      ,bse.chats_count
      ,bse.views_count_pred
      ,bse.taps_count_pred
      ,bse.chats_count_pred
  from purch_evt p1
  left join purch_evt p2 on p1.profile_id = p2.profile_id and p1.rn+1 = p2.rn
  left join boost_report_summ bse on p1.profile_id = bse.profile_id
  group by 1,bse.views_count
      ,bse.taps_count
      ,bse.chats_count
      ,bse.views_count_pred
      ,bse.taps_count_pred
      ,bse.chats_count_pred
)
