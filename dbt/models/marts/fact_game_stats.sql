-- Fact table at game-team grain (one row per team per game).
-- Scoped to FBS-vs-FBS games only: we keep stats for games where BOTH teams are
-- FBS, so every team_id resolves to dim_teams and the relationships tests hold.
-- (Loosen the fbs_games filter later to include FBS-vs-FCS games.)

with stats as (
    select * from {{ ref('stg_game_stats') }}
),

fbs_games as (
    select game_id
    from {{ ref('stg_games') }}
    where home_classification = 'fbs'
      and away_classification = 'fbs'
)

select
    s.game_id || '-' || s.team_id as game_team_key,  -- surrogate key for the grain
    s.game_id,
    s.team_id,
    s.season,
    s.week,
    s.team,
    s.home_away,
    s.points,

    -- rushing / passing / totals
    s.rushing_yards,
    s.rushing_attempts,
    s.rushing_tds,
    s.yards_per_rush_attempt,
    s.net_passing_yards,
    s.passing_tds,
    s.yards_per_pass,
    s.completions,
    s.pass_attempts,
    s.total_yards,
    s.first_downs,

    -- down conversions
    s.third_down_conversions,
    s.third_down_attempts,
    s.fourth_down_conversions,
    s.fourth_down_attempts,

    -- turnovers
    s.interceptions,
    s.passes_intercepted,
    s.fumbles_recovered,
    s.fumbles_lost,
    s.total_fumbles,
    s.turnovers,

    -- defense
    s.tackles,
    s.tackles_for_loss,
    s.sacks,
    s.qb_hurries,
    s.passes_deflected,
    s.interception_yards,
    s.interception_tds,
    s.defensive_tds,

    -- special teams
    s.kick_returns,
    s.kick_return_yards,
    s.kick_return_tds,
    s.punt_returns,
    s.punt_return_yards,
    s.punt_return_tds,
    s.kicking_points,

    -- penalties / possession
    s.penalties,
    s.penalty_yards,
    s.possession_seconds
from stats s
inner join fbs_games g on s.game_id = g.game_id
