set start_d = '2022-06-20';
set end_d = '2022-06-26';

create or replace table kelvinc.public.chat_tap_evts as
(
  with new_chat as
  (
    select
      evt.params:source::text as chat_source
      ,evt.params:type::text as chat_type
      ,evt.profile_id
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
      ,least(evt.profile_id, target_profile_id) || greatest(evt.profile_id, target_profile_id) as connection_id
      ,to_timestamp(evt.timestamp) as event_ts
      ,auc.app_category
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as auc on evt.profile_id = auc.profile_id and evt.ds = auc.ds
    where evt.ds between $start_d and $end_d
      and evt.event_name = 'chat_new_thread'
  )
  ,tap as
  (
    select
      evt.params:source::text as tap_source
      ,evt.params:type::text as tap_type
      ,evt.profile_id
      ,try_cast(evt.params:pii_target_profile_id::text as bigint) as target_profile_id
      ,least(evt.profile_id, target_profile_id) || greatest(evt.profile_id, target_profile_id) as connection_id
      ,to_timestamp(evt.timestamp) as event_ts
      ,auc.app_category
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as auc on evt.profile_id = auc.profile_id and evt.ds = auc.ds
    where evt.ds between to_date($start_d) - interval '6 days' and $end_d
      and evt.event_name = 'tap_sent'
  )
  select
      coalesce(cht.connection_id, tap.connection_id) as connection_id
      ,mode(cht.chat_source) as chat_src
      ,count(distinct tap.profile_id) as n_profiles_tapping
      ,case when n_profiles_tapping = 0 and max(cht.connection_id) is not null then 'no tap, chat started'
            when n_profiles_tapping = 1 and max(cht.connection_id) is not null and max(cht.profile_id) = max(tap.profile_id) then 'single tap, chat started by tapper'
            when n_profiles_tapping = 1 and max(cht.connection_id) is not null and max(cht.profile_id) <> max(tap.profile_id) and max(tap.app_category) = 'Free' then 'single tap, chat started by Free tap receiver'
            when n_profiles_tapping = 1 and max(cht.connection_id) is not null and max(cht.profile_id) <> max(tap.profile_id) then 'single tap, chat started by Xtra/UL tap receiver'
            when n_profiles_tapping = 2 and max(cht.connection_id) is not null then 'mutual tap, chat started'
            when n_profiles_tapping = 1 and max(cht.connection_id) is null then 'single tap, no chat'
            when n_profiles_tapping = 2 and max(cht.connection_id) is null then 'mutual tap, no chat'
            end as connection_type
      ,min(datediff('minute',tap.event_ts,cht.event_ts)) as tap_to_chat_mins
  from new_chat cht
  full outer join tap on cht.connection_id = tap.connection_id and datediff('hours',tap.event_ts,cht.event_ts) between 0 and 24*7
  group by 1
)
