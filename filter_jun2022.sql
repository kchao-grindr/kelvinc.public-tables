set start_d = '2022-06-01';
set end_d = '2022-06-30';

create or replace table kelvinc.public.filter_jun2022 as
(
  with filters as
  (
    select
      auc.profile_id
      ,auc.device_id
      ,lower(auc.app_category) as app_category
      ,evt.event_name
      ,case when evt.event_name = 'cascade_filtered' then evt.params:filter::text
            when evt.event_name = 'cascade_filtered_albums_only' then 'albums'
            when evt.event_name = 'cascade_filtered_face_only' then 'face'
            when evt.event_name = 'cascade_filtered_my_type' then 'mytype'
            when evt.event_name = 'cascade_filtered_online_only' then 'online'
            when evt.event_name = 'cascade_filtered_photos_only' then 'photos' end as filter
    from "BI_USER"."REPORTING"."ACTIVE_USER_CLEAN" as auc
    left join "FLUENTD_EVENTS"."REPORTING"."CLIENT_EVENT_HOURLY" evt on auc.profile_id = evt.profile_id and auc.ds = evt.ds
    where auc.ds between $start_d and $end_d
      and
      (evt.event_name is null
       or
       evt.event_name in ('cascade_filtered'
                         ,'cascade_filtered_albums_only'
                         ,'cascade_filtered_face_only'
                         ,'cascade_filtered_my_type'
                         ,'cascade_filtered_online_only'
                         ,'cascade_filtered_photos_only')
       )
  )

  select
      profile_id
      ,device_id
      ,app_category
      ,array_intersection
      (
          split(replace(replace(replace(replace(replace(replace(replace(replace(filter,'my_type','mytype'),'_for'),'_now'),'_only'),'haven\'t'),'_type'),'_status'),'_at'),'_')
          ,split('age_looking_tribes_online_photos_face_albums_chatted_weight_height_body_position_relationship_meet_nsfw_mytype','_')
      ) as filter_cleaned
  from filters
)
