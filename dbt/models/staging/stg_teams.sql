with source as (
    select * from {{ source('cfb_raw', 'teams') }}
)

select
    data:team_id::int            as team_id,
    season,
    data:school::string          as school,
    data:mascot::string          as mascot,
    data:abbreviation::string    as abbreviation,
    data:conference::string      as conference,
    data:color::string           as color,
    data:alternate_color::string as alternate_color,
    data:venue_name::string      as venue_name,
    data:venue_city::string      as venue_city,
    data:venue_state::string     as venue_state,
    data:venue_capacity::int     as venue_capacity,
    data:venue_dome::boolean     as venue_dome,
    data:venue_grass::boolean    as venue_grass
from source
