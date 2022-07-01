-- labels inbound attention for boost purchasers (target) as boost attributed vs not
-- engagement is boosted if actor engages with a target while target is boosted

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
      ,row_number() over (partition by evt.profile_id order by evt.timestamp desc) as rn
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join kelvinc.public.boost_purchaser_20220525_0531 pch on evt.profile_id = pch.profile_id
    where to_date(to_timestamp(evt.timestamp)) between $start_d and $end_d
      and
      (
        lower(evt.event_name) in ('purchase_completed')
        and evt.params:feature = 'boost'
      )
  )
  ,repurchasers as
  (
    select booster_profile_id, count(1) as n_purchases
    from boost_tm
    group by 1
  )
  select
    evt.profile_id as actor_profile_id
    ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
    ,lower(event_name) as event_name
    ,to_timestamp(evt.timestamp) as event_ts
    ,max(case when btm.booster_profile_id is not null then 1 else 0 end) as is_boost
    ,max(case when btm.rn = 1 and rpc.n_purchases > 1 then 1 else 0 end) as is_latest_boost
  from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
  join kelvinc.public.boost_purchaser_20220525_0531 pch on try_cast(evt.params:pii_target_profile_id::text as bigint) = pch.profile_id
  left join boost_tm btm on try_cast(evt.params:pii_target_profile_id::text as bigint) = btm.booster_profile_id and to_timestamp(evt.timestamp) between btm.boost_start_ts and btm.boost_end_ts
  left join repurchasers rpc on pch.profile_id = rpc.booster_profile_id
  where event_ts between $start_d and $end_d
    and lower(evt.event_name) in ('profile_viewed','chat_sent','tap_sent')
  group by 1,2,3,4
)
