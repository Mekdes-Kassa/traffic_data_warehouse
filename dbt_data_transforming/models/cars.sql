{{ config(materialized='table') }}

select
    track_id,
    'Car' as type,
    traveled_distance,
    avg_speed
from {{ ref('df_track') }}
where type = 'Car'