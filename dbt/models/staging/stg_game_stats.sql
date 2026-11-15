with source as (
    select * from {{ source('cfb_raw', 'game_stats') }}
)

select
    data:game_id::int        as game_id,
    data:team_id::int        as team_id,
    season,
    data:week::int           as week,
    data:team::string        as team,
    data:home_away::string   as home_away,
    data:points::int         as points,

    -- rushing / passing / totals
    data:rushing_yards::int           as rushing_yards,
    data:rushing_attempts::int        as rushing_attempts,
    data:rushing_tds::int             as rushing_tds,
    data:yards_per_rush_attempt::float as yards_per_rush_attempt,
    data:net_passing_yards::int       as net_passing_yards,
    data:passing_tds::int             as passing_tds,
    data:yards_per_pass::float        as yards_per_pass,
    data:completions::int             as completions,
    data:pass_attempts::int           as pass_attempts,
    data:total_yards::int             as total_yards,
    data:first_downs::int             as first_downs,

    -- down conversions
    data:third_down_conversions::int  as third_down_conversions,
    data:third_down_attempts::int     as third_down_attempts,
    data:fourth_down_conversions::int as fourth_down_conversions,
    data:fourth_down_attempts::int    as fourth_down_attempts,

    -- turnovers
    data:interceptions::int           as interceptions,
    data:passes_intercepted::int      as passes_intercepted,
    data:fumbles_recovered::int       as fumbles_recovered,
    data:fumbles_lost::int            as fumbles_lost,
    data:total_fumbles::int           as total_fumbles,
    data:turnovers::int               as turnovers,

    -- defense
    data:tackles::int                 as tackles,
    data:tackles_for_loss::float      as tackles_for_loss,
    data:sacks::float                 as sacks,
    data:qb_hurries::int              as qb_hurries,
    data:passes_deflected::int        as passes_deflected,
    data:interception_yards::int      as interception_yards,
    data:interception_tds::int        as interception_tds,
    data:defensive_tds::int           as defensive_tds,

    -- special teams
    data:kick_returns::int            as kick_returns,
    data:kick_return_yards::int       as kick_return_yards,
    data:kick_return_tds::int         as kick_return_tds,
    data:punt_returns::int            as punt_returns,
    data:punt_return_yards::int       as punt_return_yards,
    data:punt_return_tds::int         as punt_return_tds,
    data:kicking_points::int          as kicking_points,

    -- penalties / possession
    data:penalties::int               as penalties,
    data:penalty_yards::int           as penalty_yards,
    data:possession_seconds::int      as possession_seconds
from source
