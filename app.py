import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = st.secrets.get("SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY") or os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

st.set_page_config(page_title="PostHog Contributor Dashboard", layout="wide")
st.title("PostHog Contributor Impact Dashboard")
st.caption("Last 90 days · Data sourced from GitHub PRs")

with st.expander("How is Impact Score calculated?", expanded=True):
    st.markdown("""
    Each contributor is scored out of **100** using four normalized metrics:

    $$
    \\text{Impact Score} = 100 \\times \\left(
        w_1 \\cdot \\hat{M} +
        w_2 \\cdot \\hat{C} +
        w_3 \\cdot (1 - \\hat{R}) +
        w_4 \\cdot \\hat{V}
    \\right)
    $$

    | Symbol | Metric | Description |
    |---|---|---|
    | $\\hat{M}$ | **Merge Rate** | Fraction of PRs that were merged (higher = better) |
    | $\\hat{C}$ | **Code Churn** | Total lines added + deleted, normalized (higher = more output) |
    | $1 - \\hat{R}$ | **Change Request Ratio** | Fraction of reviews that requested changes, inverted (lower ratio = better) |
    | $\\hat{V}$ | **Reviews Given** | Number of reviews given on others' PRs, normalized (higher = more collaborative) |

    All metrics are **min-max normalized** across all authors before scoring.
    The weights $w_1, w_2, w_3, w_4$ are adjustable in the sidebar and always sum to 1.
    """)


@st.cache_data(ttl=300)
def load_table(table: str) -> list:
    rows = []
    page_size = 1000
    offset = 0
    while True:
        result = supabase.table(table).select("*").range(offset, offset + page_size - 1).execute()
        rows.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    return rows


def load_data():
    return pd.DataFrame(load_table("pull_requests")), pd.DataFrame(load_table("reviews"))


def compute_metrics(prs: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    # Merge rate
    pr_stats = prs.groupby("author").agg(
        total_prs=("id", "count"),
        merged_prs=("merged", "sum"),
        total_churn=("additions", "sum"),
        total_deletions=("deletions", "sum"),
    ).reset_index()
    pr_stats["churn"] = pr_stats["total_churn"] + pr_stats["total_deletions"]
    pr_stats["merge_rate"] = pr_stats["merged_prs"] / pr_stats["total_prs"]

    # Change request ratio (reviews received)
    pr_num_to_author = prs.set_index("number")["author"].to_dict()
    reviews["pr_author"] = reviews["pull_request_number"].map(pr_num_to_author)

    received = reviews[reviews["reviewer"] != reviews["pr_author"]]
    cr_stats = received.groupby("pr_author").agg(
        total_reviews_received=("id", "count"),
        change_requests_received=("state", lambda x: (x == "CHANGES_REQUESTED").sum()),
    ).reset_index().rename(columns={"pr_author": "author"})
    cr_stats["cr_ratio"] = cr_stats["change_requests_received"] / cr_stats["total_reviews_received"]

    # Review contribution (reviews given)
    given = reviews[reviews["reviewer"] != reviews["pr_author"]]
    given_stats = given.groupby("reviewer").agg(
        reviews_given=("id", "count"),
    ).reset_index().rename(columns={"reviewer": "author"})

    # Merge all
    df = pr_stats.merge(cr_stats, on="author", how="left")
    df = df.merge(given_stats, on="author", how="left")
    df["cr_ratio"] = df["cr_ratio"].fillna(0)
    df["reviews_given"] = df["reviews_given"].fillna(0)
    df["total_reviews_received"] = df["total_reviews_received"].fillna(0)

    return df


def normalize(series: pd.Series) -> pd.Series:
    rng = series.max() - series.min()
    if rng == 0:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - series.min()) / rng


def compute_score(df: pd.DataFrame, weights: dict) -> pd.DataFrame:
    df = df.copy()
    df["n_merge_rate"] = normalize(df["merge_rate"])
    df["n_churn"] = normalize(df["churn"])
    df["n_cr_ratio"] = 1 - normalize(df["cr_ratio"])  # lower is better
    df["n_reviews_given"] = normalize(df["reviews_given"])

    df["impact_score"] = (
        df["n_merge_rate"] * weights["merge_rate"]
        + df["n_churn"] * weights["churn"]
        + df["n_cr_ratio"] * weights["cr_ratio"]
        + df["n_reviews_given"] * weights["reviews_given"]
    ) * 100

    return df.sort_values("impact_score", ascending=False).reset_index(drop=True)


prs, reviews = load_data()

if prs.empty:
    st.warning("No data yet. Run `python fetch_data.py` first.")
    st.stop()

# --- Sidebar: weights ---
st.sidebar.header("Score Weights")
w_merge = st.sidebar.slider("Merge Rate", 0, 100, 30)
w_churn = st.sidebar.slider("Code Churn", 0, 100, 20)
w_cr = st.sidebar.slider("Change Request Ratio", 0, 100, 15)
w_reviews = st.sidebar.slider("Reviews Given", 0, 100, 35)

total_weight = w_merge + w_churn + w_cr + w_reviews
if total_weight == 0:
    st.sidebar.error("Weights must sum to more than 0.")
    st.stop()

weights = {
    "merge_rate": w_merge / total_weight,
    "churn": w_churn / total_weight,
    "cr_ratio": w_cr / total_weight,
    "reviews_given": w_reviews / total_weight,
}

st.sidebar.caption(f"Weights auto-normalize (total: {total_weight})")

metrics = compute_metrics(prs, reviews)
scored = compute_score(metrics, weights)

# --- Leaderboard ---
st.subheader("Leaderboard")

top_n = st.slider("Show top N authors", 5, 50, 5)
leaderboard = scored.head(top_n)[
    ["author", "impact_score", "total_prs", "merged_prs", "merge_rate",
     "churn", "reviews_given", "cr_ratio", "total_reviews_received"]
].copy()
leaderboard.index = leaderboard.index + 1
leaderboard.columns = [
    "Author", "Impact Score", "Total PRs", "Merged PRs", "Merge Rate",
    "Code Churn", "Reviews Given", "CR Ratio", "Reviews Received"
]
leaderboard["Impact Score"] = leaderboard["Impact Score"].round(1)
leaderboard["Merge Rate"] = leaderboard["Merge Rate"].map("{:.1%}".format)
leaderboard["CR Ratio"] = leaderboard["CR Ratio"].map("{:.1%}".format)
leaderboard["Code Churn"] = leaderboard["Code Churn"].map("{:,.0f}".format)

st.dataframe(leaderboard, use_container_width=True)

st.divider()

# --- Top stats ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total PRs", len(prs))
col2.metric("Unique Authors", prs["author"].nunique())
col3.metric("Total Reviews", len(reviews))
col4.metric("Overall Merge Rate", f"{prs['merged'].mean():.1%}")

st.divider()

# --- Bar charts ---
st.subheader(f"Metric Breakdown (Top {top_n})")
top_n_df = scored.head(top_n)

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Impact Score", "Code Churn", "Reviews Given", "Merge Rate", "CR Ratio"])

with tab1:
    fig = px.bar(top_n_df, x="author", y="impact_score", color="impact_score",
                 color_continuous_scale="Blues", labels={"impact_score": "Score", "author": ""})
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    fig = px.bar(top_n_df.sort_values("churn", ascending=False), x="author", y="churn",
                 labels={"churn": "Lines Changed", "author": ""})
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    fig = px.bar(top_n_df.sort_values("reviews_given", ascending=False), x="author", y="reviews_given",
                 labels={"reviews_given": "Reviews Given", "author": ""})
    st.plotly_chart(fig, use_container_width=True)

with tab4:
    fig = px.bar(top_n_df.sort_values("merge_rate", ascending=False), x="author", y="merge_rate",
                 labels={"merge_rate": "Merge Rate", "author": ""},
                 range_y=[0, 1])
    fig.update_layout(yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

with tab5:
    fig = px.bar(top_n_df.sort_values("cr_ratio", ascending=True), x="author", y="cr_ratio",
                 labels={"cr_ratio": "CR Ratio", "author": ""},
                 range_y=[0, 1])
    fig.update_layout(yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Author drilldown ---
st.subheader("Author Drilldown")
author = st.selectbox("Select an author", scored["author"].tolist())

if author:
    row = scored[scored["author"] == author].iloc[0]
    rank = scored[scored["author"] == author].index[0] + 1

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rank", f"#{rank}")
    c2.metric("Impact Score", f"{row['impact_score']:.1f}")
    c3.metric("Merged PRs", f"{int(row['merged_prs'])}/{int(row['total_prs'])}")
    c4.metric("Reviews Given", int(row["reviews_given"]))
    c5.metric("Code Churn", f"{int(row['churn']):,}")

    # PR timeline
    author_prs = prs[prs["author"] == author].copy()
    author_prs["created_at"] = pd.to_datetime(author_prs["created_at"])
    author_prs = author_prs.sort_values("created_at")
    author_prs["churn"] = author_prs["additions"] + author_prs["deletions"]

    fig = px.scatter(
        author_prs,
        x="created_at",
        y="churn",
        color="merged",
        hover_data=["title", "number"],
        labels={"created_at": "Date", "churn": "Lines Changed", "merged": "Merged"},
        title=f"PRs by {author}",
        color_discrete_map={True: "#4CAF50", False: "#F44336"},
    )
    st.plotly_chart(fig, use_container_width=True)

    # Reviews given by this author
    author_reviews = reviews[reviews["reviewer"] == author]
    if not author_reviews.empty:
        review_counts = author_reviews.groupby("state").size().reset_index(name="count")
        fig2 = px.pie(review_counts, names="state", values="count",
                      title=f"Reviews given by {author}",
                      color_discrete_map={
                          "APPROVED": "#4CAF50",
                          "CHANGES_REQUESTED": "#F44336",
                          "COMMENTED": "#2196F3",
                      })
        st.plotly_chart(fig2, use_container_width=True)
