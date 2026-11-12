# Data notes & modeling decisions

Running notes on things learned from inspecting the raw CFBD data. These feed
the README's "Modeling decisions & data notes" section (Phase 7).

## Teams, games, stats — the three sources

| Endpoint | Grain | Target table |
|---|---|---|
| `/teams/fbs?year=<year>` | one row per FBS team (~134) | `dim_teams` |
| `/games?year=<year>&seasonType=regular&classification=fbs` | one row per game (~874/season) | `dim_games` |
| `/games/teams?year=<year>&week=<week>&classification=fbs` | one row per game × team (2/game) | `fact_game_stats` |

Use the `classification=fbs` filter server-side. Without it, `/teams` returns
~1,933 teams (all divisions) and `/games` returns ~3,747 games (all divisions).

## Neutral-site games: venue ≠ home team's stadium

"Home team" is a scheduling/box-score label, **not** a statement of where the
game was played. Example: 2024 week 1, Georgia Tech (home) vs Florida State was
played at **Aviva Stadium, Dublin, Ireland** with `neutralSite: true`.

- Team's usual stadium lives in `dim_teams.location`.
- The specific game's venue lives in `dim_games.venue` / `venueId` / `neutralSite`.
- Keep them separate. Deriving venue from the home team is wrong for every
  neutral-site game (season openers abroad, bowls, conference championships, CFP).
- Analytics impact: any home-field-advantage metric must exclude
  `neutralSite = true` games.

## `/games` returns the schedule, not just results

The games endpoint includes **future, unplayed games**. They have
`completed: false` and `null` scores (`homePoints`/`awayPoints`).

- Example: as of 2026-11-12 (week 11), the 2026 season has 888 games — 691
  completed, 197 not yet played.
- `fact_game_stats` only exists for completed games (the stats endpoint returns
  nothing for an unplayed game).
- Rule: filter `WHERE completed = true` before building the fact table, or nulls
  corrupt aggregations and break `not_null` tests.

## Stats are "long" (EAV) and string-typed

In `/games/teams`, each team's `stats` is a list of `{category, stat}` pairs, and
every value is a **string** — several are compound and need parsing:

| Category | Raw | Meaning |
|---|---|---|
| `rushingYards` | `"190"` | int |
| `thirdDownEff` | `"5-9"` | made / attempts → 2 columns |
| `completionAttempts` | `"11-16"` | completions / attempts |
| `totalPenaltiesYards` | `"3-35"` | penalties / yards |
| `possessionTime` | `"29:21"` | mm:ss → seconds |

Transformation job: pivot the long list to wide columns, then split/parse the
compound strings into typed values (raw → staging → analytics).

## Scope decision: backfill history, then go live

- **Phase 1–6:** backfill completed seasons **2022, 2023, 2024** — immutable,
  every game completed, every game has stats. No edge cases while learning the stack.
- **Then:** add the current (2026) season as a **live** source. This gives the
  Airflow schedule a real purpose — as games complete each week, new rows flow in.
  - Caching becomes season-aware: past seasons cached permanently; current season
    re-fetched each run (because `completed` flips and scores fill in).
  - Loads reload the current season **by partition** (drop/replace just that
    season's rows) to stay idempotent without row-level MERGE.
