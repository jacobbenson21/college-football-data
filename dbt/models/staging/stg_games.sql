with source as (
    select * from {{ source('cfb_raw', 'games') }}
)

select
    data:game_id::int                  as game_id,
    season,
    data:week::int                     as week,
    try_to_timestamp_ntz(data:start_date::string) as start_date,
    data:neutral_site::boolean         as neutral_site,
    data:conference_game::boolean      as conference_game,
    data:venue::string                 as venue,
    data:attendance::int               as attendance,
    data:excitement_index::float       as excitement_index,

    -- home side
    data:home_team_id::int             as home_team_id,
    data:home_team::string             as home_team,
    data:home_conference::string       as home_conference,
    data:home_classification::string   as home_classification,
    data:home_points::int              as home_points,
    data:home_q1::int                  as home_q1,
    data:home_q2::int                  as home_q2,
    data:home_q3::int                  as home_q3,
    data:home_q4::int                  as home_q4,
    data:home_ot::int                  as home_ot,
    data:home_pregame_elo::int         as home_pregame_elo,
    data:home_postgame_win_prob::float as home_postgame_win_prob,

    -- away side
    data:away_team_id::int             as away_team_id,
    data:away_team::string             as away_team,
    data:away_conference::string       as away_conference,
    data:away_classification::string   as away_classification,
    data:away_points::int              as away_points,
    data:away_q1::int                  as away_q1,
    data:away_q2::int                  as away_q2,
    data:away_q3::int                  as away_q3,
    data:away_q4::int                  as away_q4,
    data:away_ot::int                  as away_ot,
    data:away_pregame_elo::int         as away_pregame_elo,
    data:away_postgame_win_prob::float as away_postgame_win_prob
from source
