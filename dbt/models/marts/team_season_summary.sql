-- Team-season performance summary built from fact_game_stats.
-- Technique: self-join the fact on game_id to pair each team's row with its
-- opponent's row in the same game, which gives us "for vs. against" metrics
-- (points/yards allowed, takeaways) that a single row can't express.

with stats as (
    select * from {{ ref('fact_game_stats') }}
),

-- each team's game row + the opponent's row in that game
team_games as (
    select
        s.season,
        s.team_id,
        s.team,
        s.game_id,
        s.points                                   as points_for,
        o.points                                   as points_against,
        s.total_yards                              as yards_for,
        o.total_yards                              as yards_against,
        (s.rushing_attempts + s.pass_attempts)     as plays,          -- offensive plays (approx)
        s.third_down_conversions,
        s.third_down_attempts,
        s.turnovers                                as giveaways,      -- this team's turnovers
        o.turnovers                                as takeaways,      -- opponent's turnovers
        case when s.points > o.points then 1 else 0 end as win
    from stats s
    join stats o
      on s.game_id = o.game_id
     and s.team_id <> o.team_id          -- the other team in the same game
)

select
    team_id || '-' || season                                           as team_season_key,
    season,
    team_id,
    team,
    count(*)                                                            as games_played,
    sum(win)                                                           as wins,
    count(*) - sum(win)                                                as losses,
    round(avg(points_for), 1)                                         as points_per_game,
    round(avg(points_against), 1)                                     as points_allowed_per_game,
    round(avg(yards_for), 1)                                          as yards_per_game,
    round(sum(yards_for) / nullif(sum(plays), 0), 2)                  as yards_per_play,
    round(100.0 * sum(third_down_conversions)
          / nullif(sum(third_down_attempts), 0), 1)                   as third_down_pct,
    sum(takeaways) - sum(giveaways)                                   as turnover_margin
from team_games
group by season, team_id, team
