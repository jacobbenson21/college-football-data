"""Streamlit dashboard on the CFB analytics warehouse (Snowflake).

Run: streamlit run dashboard/app.py   (with the repo .env sourced)
"""

import os

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="College Football Analytics", page_icon="🏈", layout="wide")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


@st.cache_data(ttl=600)
def query(sql: str) -> pd.DataFrame:
    cur = get_connection().cursor()
    cur.execute(sql)
    return cur.fetch_pandas_all()


# ---------------------------------------------------------------- header / sidebar
st.title("🏈 College Football Analytics")
st.caption(
    "Team-season metrics (FBS-vs-FBS games), served from a Snowflake star schema "
    "built by an Airflow pipeline: API → S3 → PySpark → Snowflake → dbt."
)

seasons = query(
    "select distinct season from CFB.ANALYTICS.TEAM_SEASON_SUMMARY order by season desc"
)
season = st.sidebar.selectbox("Season", seasons["SEASON"].tolist())
st.sidebar.markdown(
    "**About**\n\nRecords count FBS-vs-FBS games only, so they can differ from "
    "official records by games against FCS opponents."
)

summary = query(
    f"""
    select team, games_played, wins, losses, points_per_game,
           points_allowed_per_game, yards_per_game, yards_per_play,
           third_down_pct, turnover_margin
    from CFB.ANALYTICS.TEAM_SEASON_SUMMARY
    where season = {season}
    order by wins desc, points_per_game desc
    """
)

# ---------------------------------------------------------------- KPI row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Teams", len(summary))
c2.metric("Avg points / game", round(summary["POINTS_PER_GAME"].mean(), 1))
c3.metric("Avg yards / play", round(summary["YARDS_PER_PLAY"].mean(), 2))
top_team = summary.iloc[0]
c4.metric("Most wins", f"{top_team['TEAM']} ({int(top_team['WINS'])})")

st.divider()

# ---------------------------------------------------------------- what wins games
st.subheader(f"What correlated with winning in {season}?")
st.caption("Correlation of each metric with season wins across all teams (−1 to 1).")
metric_cols = {
    "POINTS_PER_GAME": "Points / game",
    "POINTS_ALLOWED_PER_GAME": "Points allowed / game",
    "YARDS_PER_PLAY": "Yards / play",
    "THIRD_DOWN_PCT": "3rd down %",
    "TURNOVER_MARGIN": "Turnover margin",
}
corr = (
    summary[list(metric_cols) + ["WINS"]]
    .corr()["WINS"]
    .drop("WINS")
    .rename(index=metric_cols)
    .sort_values()
)
corr_df = corr.reset_index()
corr_df.columns = ["Metric", "Correlation with wins"]
corr_chart = (
    alt.Chart(corr_df)
    .mark_bar()
    .encode(
        x=alt.X("Correlation with wins:Q", scale=alt.Scale(domain=[-1, 1])),
        y=alt.Y("Metric:N", sort="-x"),
        color=alt.condition(
            alt.datum["Correlation with wins"] > 0,
            alt.value("#2e7d32"),
            alt.value("#c62828"),
        ),
        tooltip=["Metric", alt.Tooltip("Correlation with wins:Q", format=".2f")],
    )
    .properties(height=200)
)
st.altair_chart(corr_chart, use_container_width=True)

st.divider()

# ---------------------------------------------------------------- leaderboard (clickable)
st.subheader("Team leaderboard")
st.caption("👉 Click a team's row to see its game log below.")

NICE = {
    "TEAM": "Team", "GAMES_PLAYED": "Games", "WINS": "Wins", "LOSSES": "Losses",
    "POINTS_PER_GAME": "Pts / G", "POINTS_ALLOWED_PER_GAME": "Pts Allowed / G",
    "YARDS_PER_GAME": "Yards / G", "YARDS_PER_PLAY": "Yards / Play",
    "THIRD_DOWN_PCT": "3rd Down %", "TURNOVER_MARGIN": "TO Margin",
}
disp = summary.rename(columns=NICE)

event = st.dataframe(
    disp,
    use_container_width=True,
    hide_index=True,
    on_select="rerun",
    selection_mode="single-row",
    column_config={
        "Pts / G": st.column_config.NumberColumn(format="%.1f", help="Avg points scored"),
        "Pts Allowed / G": st.column_config.NumberColumn(
            format="%.1f", help="Avg points allowed (lower is better)"
        ),
        "Yards / G": st.column_config.NumberColumn(format="%.0f"),
        "Yards / Play": st.column_config.NumberColumn(
            format="%.2f", help="Yards ÷ plays — efficiency; correlates strongly with winning"
        ),
        "3rd Down %": st.column_config.NumberColumn(format="%.1f%%"),
        "TO Margin": st.column_config.NumberColumn(
            help="Takeaways − giveaways; strongly tied to winning"
        ),
    },
)

# which team is selected (default to the top team)
if event.selection.rows:
    selected_team = disp.iloc[event.selection.rows[0]]["Team"]
else:
    selected_team = disp.iloc[0]["Team"]

# ---------------------------------------------------------------- charts
left, right = st.columns(2)
with left:
    st.subheader("Top 10 scoring offenses")
    top = summary.nlargest(10, "POINTS_PER_GAME").set_index("TEAM")["POINTS_PER_GAME"]
    st.bar_chart(top, color="#2e7d32")
with right:
    st.subheader("Offense vs. defense")
    scatter = (
        alt.Chart(summary)
        .mark_circle(size=90, opacity=0.7)
        .encode(
            x=alt.X("POINTS_PER_GAME:Q", title="Points / game"),
            y=alt.Y(
                "POINTS_ALLOWED_PER_GAME:Q",
                title="Points allowed / game",
                scale=alt.Scale(reverse=True),  # better defense toward the top
            ),
            color=alt.Color("WINS:Q", scale=alt.Scale(scheme="greens"), title="Wins"),
            tooltip=["TEAM", "WINS", "POINTS_PER_GAME", "POINTS_ALLOWED_PER_GAME"],
        )
        .properties(height=340)
    )
    st.altair_chart(scatter, use_container_width=True)
    st.caption("Top-right = strong offense and defense.")

st.divider()

# ---------------------------------------------------------------- team game log
st.subheader(f"{selected_team} — {season} game log")
games = query(
    f"""
    select
        g.week as "Week",
        case when f.home_away = 'home' then g.away_team else g.home_team end as "Opponent",
        case when f.home_away = 'home' then 'vs' else '@' end as "Site",
        f.points as "PF",
        case when f.home_away = 'home' then g.away_points else g.home_points end as "PA",
        f.total_yards as "Yards",
        f.turnovers as "TO"
    from CFB.ANALYTICS.FACT_GAME_STATS f
    join CFB.ANALYTICS.DIM_GAMES g on f.game_id = g.game_id
    where f.season = {season} and f.team = '{selected_team.replace("'", "''")}'
    order by g.week
    """
)
if not games.empty:
    games["Result"] = games.apply(
        lambda r: "✅ W" if r["PF"] > r["PA"] else "🔴 L", axis=1
    )
    games["Score"] = games["PF"].astype(str) + "–" + games["PA"].astype(str)
    record = f"{(games['PF'] > games['PA']).sum()}–{(games['PF'] <= games['PA']).sum()}"
    st.caption(f"Record (FBS-vs-FBS): **{record}**")
    st.dataframe(
        games[["Week", "Result", "Site", "Opponent", "Score", "Yards", "TO"]],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No games found for this team/season.")
