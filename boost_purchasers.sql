create or replace table kelvinc.public.boost_purchasers as
(
  with geo as
  (
    select auc.profile_id, mode(os_platform) os_platform
    from "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" auc
    where auc.ds between $start_d and $end_d
      and lower(auc.country) in ('aus','nzl')
    group by 1
  )
  ,raw_evt as
  (
    select
      to_timestamp(evt.timestamp) as purchase_ts
      ,evt.profile_id
      ,geo.os_platform
      ,evt.event_name
      ,evt.app_category
      ,lower(to_variant(parse_json(params):source)) as src
    from "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt
    join geo on evt.profile_id = geo.profile_id
    where evt.ds between $start_d and $end_d
      and
      (
        lower(evt.event_name) in ('purchase_completed')
        and lower(to_variant(parse_json(params):feature)) = 'boost'
      )
  )
  ,headline as
  (
    select ret.profile_id, max(iff(dus.displayname is null,0,1)) as has_headline
    from raw_evt ret
    left join bi_user.reporting.dim_user dus on to_date(ret.purchase_ts) = dus.ds and ret.profile_id = dus.profile_id
    group by 1
  )
  ,photo as
  (
    select
        med.profile_id
        ,count(sec.position)+1 as n_pics
    from "AURORA_GRINDR"."RAW"."MEDIA" med
    left join "AURORA_GRINDR"."RAW"."PROFILE_SECONDARY_IMAGE" sec on med.profile_id = sec.profile_id
    where med.isprimary=1
      and med.profile_id in (select distinct profile_id from raw_evt)
    group by 1
  )
  ,n_purch as
  (
    select profile_id,count(1) as n_purchases
    from raw_evt
    group by 1
  )
  ,results_by_boost as
  (
    select
      bse.profile_id
      ,bse.boost_session_id
      ,bse.start_time
      ,bse.end_time
      ,bse.chats_count
      ,bse.taps_count
      ,bse.views_count
      ,rev.os_platform
      ,rev.app_category
      ,row_number() over (partition by bse.profile_id order by bse.start_time desc) as rn
    from "AURORA_GRINDR"."RAW"."BOOST_SESSION" bse
    join raw_evt rev on rev.profile_id = bse.profile_id and abs(datediff('min',to_timestamp(rev.purchase_ts),to_timestamp(bse.start_time))) <= 5
  )
  ,results_by_user as
  (
    --user attributes from all boosts prior to the latest boost
    select
      rbb.profile_id
      ,1 as is_repurchase
      ,max(npu.n_purchases) as n_purchases
      ,mode(os_platform) as os_platform
      ,mode(app_category) as app_category
      ,avg(chats_count) as chats
      ,avg(taps_count) as taps
      ,avg(views_count) as views
    from results_by_boost rbb
    join n_purch npu on rbb.profile_id = npu.profile_id
    where rbb.rn > 1
      and npu.n_purchases > 1
    group by 1,2

    union

    --take attributes from single boost for single purchase users
    select
      rbb.profile_id
      ,0 as is_repurchase
      ,1 as n_purchases
      ,mode(os_platform) as os_platform
      ,mode(app_category) as app_category
      ,avg(chats_count) as chats
      ,avg(taps_count) as taps
      ,avg(views_count) as views
    from results_by_boost rbb
    join n_purch npu on rbb.profile_id = npu.profile_id
    where rbb.rn = 1
      and npu.n_purchases = 1
    group by 1,2,3
  )
  select
      rbu.*
      ,coalesce(hdl.has_headline,0) as has_headline
      ,coalesce(pht.n_pics,0) as n_pics
  from results_by_user rbu
  left join headline hdl on rbu.profile_id = hdl.profile_id
  left join photo pht on rbu.profile_id = pht.profile_id
)
