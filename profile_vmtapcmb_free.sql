set start_d = '2022-06-20';
set end_d = '2022-06-26';

create or replace table kelvinc.public.profile_vmtapcmb_free as
(
  with profiles_viewed as
  (
    select
      evt.profile_id
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as auc on evt.profile_id = auc.profile_id and evt.ds = auc.ds
    left join "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as aucf on target_profile_id = aucf.profile_id and evt.ds = aucf.ds
    where evt.ds between $start_d and $end_d
      and evt.event_name = 'profile_viewed'
      and lower(aucf.app_category) = 'free'
  )
  ,behaviors as
  (
    select
      evt.profile_id
      ,evt.event_name
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as auc on evt.profile_id = auc.profile_id and evt.ds = auc.ds
    where evt.ds between $start_d and $end_d
      and evt.event_name in ('tap_received'
                             ,'chat_received'
                             ,'viewed_me_list_viewed'
                             ,'inbox_taps_viewed'
                             ,'inbox_taps_tab_viewed'
                             ,'inbox_messages_viewed')
      and lower(auc.app_category) = 'free'
  )
  select
    profile_id
    ,count(case when event_name = 'profile_viewed' then 1 end) as n_view
    ,count(case when event_name = 'tap_received' then 1 end) as n_tap
    ,count(case when event_name = 'chat_received' then 1 end) as n_chat
    ,max(iff(event_name = 'viewed_me_list_viewed',1,0)) as viewed_me
    ,max(iff(event_name in ('inbox_taps_viewed','inbox_taps_tab_viewed'),1,0)) as tap_inbox
    ,max(iff(event_name = 'inbox_messages_viewed',1,0)) as chat_inbox
  from
  (
    (select target_profile_id as profile_id, 'profile_viewed' as event_name from profiles_viewed)
     union all
    (select * from behaviors)
  )
  group by 1
)
