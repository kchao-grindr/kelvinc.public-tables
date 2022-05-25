set end_d = '2022-05-23';

create or replace table kelvinc.public.prf_view_boost_20220517_0523 as
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
  ,imp as
  (
    --profile views
    select
        evt.profile_id as viewer
        ,evt.app_category as viewer_app_category
        ,evt.params:pii_target_profile_id as profile_viewed
        ,to_timestamp(evt.timestamp) as view_ts
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join prf on evt.profile_id = prf.profile_id
    where to_date(view_ts) between to_date($end_d) - interval '6 day' and $end_d
        and evt.event_name in ('profile_viewed')
  )
  ,boost as
  (
    --boosted profiles
    select
      evt.profile_id
      ,evt.event_name
//      ,evt.app_category
      ,to_timestamp(evt.timestamp) as boost_start_ts
      ,dateadd('min', 60, boost_start_ts) as boost_end_ts
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join prf on evt.profile_id = prf.profile_id
      and to_date(to_timestamp(evt.timestamp)) between to_date($end_d) - interval '6 day' and $end_d
      and
      (
          lower(evt.event_name) in ('purchase_completed'
                                   )
          and lower(to_variant(parse_json(params):feature)) = 'boost'
      )
  )
  select
      imp.viewer
      ,imp.viewer_app_category
      ,imp.view_ts
      ,imp.profile_viewed
      ,max(case when bst.profile_id is not null then 1 else 0 end) as profile_viewed_is_boosted
  from imp
  left join boost bst on imp.profile_viewed = bst.profile_id and imp.view_ts between bst.boost_start_ts and bst.boost_end_ts
  group by 1,2,3,4
)
