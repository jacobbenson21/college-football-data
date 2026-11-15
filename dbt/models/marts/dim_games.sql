-- Game dimension: one row per game, with home/away attributes side by side.

with games as (
    select * from {{ ref('stg_games') }}
)

select
    game_id,
    season,
    week,
    start_date,
    neutral_site,
    conference_game,
    venue,
    attendance,
    excitement_index,

    home_team_id,
    home_team,
    home_conference,
    home_classification,
    home_points,
    home_q1,
    home_q2,
    home_q3,
    home_q4,
    home_ot,
    home_pregame_elo,
    home_postgame_win_prob,

    away_team_id,
    away_team,
    away_conference,
    away_classification,
    away_points,
    away_q1,
    away_q2,
    away_q3,
    away_q4,
    away_ot,
    away_pregame_elo,
    away_postgame_win_prob
from games
