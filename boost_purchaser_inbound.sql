-- table that labels inbound attention for boost purchasers (target) as boost attributed vs not
-- if actor engages with a target while target is boosted, all engagement from actor is boost attributable
-- ie, boost labeling is greedy

set start_d = '2022-05-25';
set end_d = '2022-05-31';

create or replace table kelvinc.public.boost_purchaser_inbound as
(
  with boost_tm as
  (
    select
      evt.profile_id as booster_profile_id
      ,to_timestamp(evt.timestamp) as boost_start_ts
      ,dateadd('min', 60, boost_start_ts) as boost_end_ts
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join kelvinc.public.boost_purchaser pch on evt.profile_id = pch.profile_id
    where to_date(to_timestamp(evt.timestamp)) between $start_d and $end_d
      and
      (
        lower(evt.event_name) in ('purchase_completed')
        and evt.params:feature = 'boost'
      )
  )
  ,view_prof as
  (
    select
      evt.profile_id as actor_profile_id
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
      ,'profile_viewed' as event_name
      ,to_timestamp(evt.timestamp) as event_ts
      ,max(case when btm.booster_profile_id is not null then 1 else 0 end) as is_boost
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    left join boost_tm btm on try_cast(evt.params:pii_target_profile_id::text as bigint) = btm.booster_profile_id and to_timestamp(evt.timestamp) between btm.boost_start_ts and btm.boost_end_ts
    where event_ts between $start_d and $end_d
      and lower(evt.event_name) = 'profile_viewed'
    group by 1,2,3,4
  )
  ,tap_chat as
  (
    select
      try_cast(evt.params:pii_target_profile_id::text as bigint) as actor_profile_id
      ,evt.profile_id as target_profile_id --recipient of the tap or chat
      ,case when evt.event_name = 'tap_received' then 'tap'
            when evt.event_name = 'chat_received' then 'chat' end as event_name
      ,to_timestamp(evt.timestamp) as event_ts
      ,max(iff(evt.params:boost='true',1,0)) as is_boost
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join kelvinc.public.boost_purchaser pch on evt.profile_id = pch.profile_id
    where event_ts between $start_d and $end_d
      and evt.event_name in ('chat_received','tap_received')
    group by 1,2,3,4
  )
  select * from view_prof
  union all
  select * from tap_chat
)
