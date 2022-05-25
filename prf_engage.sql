set end_d = '2022-05-23';

create or replace table kelvinc.public.prf_engage_boost_20220517_0523 as
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
  --profile engagement
  select
      evt.profile_id as actor
      ,evt.app_category as actor_app_category
      ,evt.params:pii_target_profile_id as recipient
      ,evt.event_name
      ,to_timestamp(evt.timestamp) as event_ts
  from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
  join prf on evt.profile_id = prf.profile_id
  where ds between to_date($end_d) - interval '6 day' and $end_d
      and evt.event_name in ('tap_sent'
                             ,'profile_chat_tapped'
                             ,'chat_sent'
                             ,'profile_favorited')
)
