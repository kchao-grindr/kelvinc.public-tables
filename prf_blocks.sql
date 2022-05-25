set end_d = '2022-05-23';

create or replace table kelvinc.public.prf_block_boost_20220517_0523 as
(
  with prf as
  (
    --profiles eligible to see boost feature
    select distinct auc.profile_id
    from "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" auc
    where auc.ds between to_date($end_d) - interval '6 day' and $end_d
      and lower(country) in ('aus','nzl')
    group by 1
  )
  --profile blocks
  select
      evt.profile_id as blocker
      ,evt.app_category as blocker_app_category
      ,evt.params:pii_target_profile_id as profile_blocked
      ,to_timestamp(evt.timestamp) as block_ts
  from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
  join prf on evt.profile_id = prf.profile_id
  where ds between to_date($end_d) - interval '6 day' and $end_d
      and evt.event_name in ('profile_blocked')
)
