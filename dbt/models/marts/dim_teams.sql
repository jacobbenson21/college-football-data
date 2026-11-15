-- Team dimension at team-season grain (one row per team per season), so
-- conference realignment and FBS membership changes are preserved over time.

with teams as (
    select * from {{ ref('stg_teams') }}
)

select
    team_id || '-' || season as team_season_key,  -- surrogate key for the grain
    team_id,
    season,
    school,
    mascot,
    abbreviation,
    conference,
    color,
    alternate_color,
    venue_name,
    venue_city,
    venue_state,
    venue_capacity,
    venue_dome,
    venue_grass
from teams
