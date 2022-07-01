-- table that labels all outbound attention from boost purchasers as boost attributed vs not
-- if the response is made to a profile after a boost attributed engagement from that profile, the response is considered boost attributable

set start_d = '2022-05-25';
set end_d = '2022-05-31';

create or replace table kelvinc.public.boost_purchaser_outbound as
(
  with view_prof as
  (
    -- logged when user views another profile from the `Viewed Me` list
    select
      evt.profile_id as actor_profile_id --this is the booster
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
      ,evt.event_name
      ,to_timestamp(evt.timestamp) as event_ts
      ,iff(evt.params:boost='true',1,0) as is_boost
      ,null as is_latest_boost
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join kelvinc.public.boost_purchaser_20220525_0531 pch on evt.profile_id = pch.profile_id
    where event_ts between $start_d and $end_d
      and evt.event_name in ('viewed_me_profile_clicked')
  )
  ,tap_chat_raw as
  (
    select
      evt.profile_id as actor_profile_id
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
      ,lower(event_name) as event_name
      ,to_timestamp(evt.timestamp) as event_ts
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join kelvinc.public.boost_purchaser_20220525_0531 pch on evt.profile_id = pch.profile_id
    where event_ts between $start_d and $end_d
        and evt.event_name in ('tap_sent','chat_sent','profile_blocked')
  )
  ,tap_chat as
  (
    select
        tcr.actor_profile_id
        ,tcr.target_profile_id
        ,tcr.event_name
        ,tcr.event_ts
        ,max(case when bpi.actor_profile_id is not null then 1 else 0 end) as is_boost
        ,max(case when bpi.is_latest_boost = 1 then 1 else 0 end) as is_latest_boost
    from tap_chat_raw tcr
    left join kelvinc.public.boost_purchaser_inbound bpi
    on tcr.actor_profile_id = bpi.target_profile_id
        and tcr.target_profile_id = bpi.actor_profile_id
        and tcr.event_ts >= bpi.event_ts
        and bpi.is_boost = 1
    -- if booster responds to a profile that view/tap/chats the booster during boost,
    -- the response is labeled as boosted if response occurs after boosted view/tap/chat
    group by 1,2,3,4
  )
  select * from view_prof
  union all
  select * from tap_chat
)
