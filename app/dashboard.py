import os
from typing import Dict, Any, List

import altair as alt
import pandas as pd
import requests
import streamlit as st

# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000")
DEFAULT_DROP_THRESHOLD = os.getenv("DROP_THRESHOLD", .20)
DEFAULT_NULL_MAX = os.getenv("NULL_MAX", .001)
DEFAULT_DUP_MAX = os.getenv("DUP_MAX", .001)
DEFAULT_RANGE_ERR_MAX = os.getenv("RANGE_ERR_MAX", .0)

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def to_df(items: List[Dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(items) if items else pd.DataFrame()


@st.cache_data(show_spinner=False, ttl=30)
def fetch_json(path: str, params: Dict[str, Any] | None = None) -> Any:
    url = f"{API_BASE.rstrip('/')}/{path.lstrip('/')}"
    r = requests.get(url, params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


def month_options() -> list[str]:
    data = fetch_json("/trend", params={"limit": 60})
    df = to_df(data)
    return sorted(df["load_month"].unique().tolist()) if not df.empty else []


def compute_trend_metrics(trend_df: pd.DataFrame) -> pd.DataFrame:
    """Add deltas and SLA checks."""
    if trend_df.empty:
        return trend_df
    df = trend_df.copy()
    df = df.sort_values("load_month")
    # Compute deltas vs previous month
    df["row_count_prev"] = df["row_count"].shift(1)
    df["row_count_delta_pct"] = (df["row_count"] / df["row_count_prev"] - 1.0) * 100.0
    return df


def add_sla_flags(df: pd.DataFrame, drop_threshold: float, null_max: float, dup_max: float, range_err_max: float) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    # Volume drop check (positive drop means decrease; we care about drops beyond threshold)
    # row_count_delta_pct < -threshold%  → violation
    out["drop_violation"] = out["row_count_delta_pct"] < -(drop_threshold * 100.0)
    # Quality checks
    out["null_ok"] = out["null_rate"] <= null_max
    out["dup_ok"] = out["duplicate_rate"] <= dup_max
    out["range_ok"] = out["range_error_rate"] <= range_err_max
    out["delta_ok"] = ~out["drop_violation"] | out["row_count_prev"].isna()
    out["overall_ok"] = out["delta_ok"] & out["null_ok"] & out["dup_ok"] & out["range_ok"]
    return out


def pct(x: float | int | None) -> str:
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "—"

# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
st.set_page_config(page_title="Clickstream DQ & SLA", layout="wide")
st.title("Clickstream Data Quality & SLA Monitor")

left, right = st.columns([2, 8])
with left:
    months = month_options()
    month = st.selectbox("Month", options=list(reversed(months)) if months else [], index=0 if months else None)

st.caption(f"API base: {API_BASE}")
st.divider()

# Monthly Metrics
if month:
    st.subheader(f"Monthly Metrics — {month}")
    try:
        m = fetch_json("/metrics", params={"month": month})
        mdf = pd.DataFrame([m])

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Rows", f"{int(m['row_count']):,}")
        c2.metric("Null rate (%)", f"{m['null_rate']:.6f}")
        c3.metric("Duplicate rate (%)", f"{m['duplicate_rate']:.6f}")
        c4.metric("Range-error rate (%)", f"{m['range_error_rate']:.6f}")
        c5.metric("Schema valid", "✅" if int(m["schema_valid"]) == 1 else "❌")

        st.dataframe(mdf, width="stretch")
    except requests.HTTPError as e:
        st.warning(f"No metrics for {month} ({e})")

st.divider()

# Trend (SLA & DQ oriented)
title_col, ctl_col0, ctl_col1, ctl_col2, ctl_col3, ctl_col4 = st.columns([3, 1.5, 1.5, 1.5, 1.5, 1.5])
with title_col:
    st.subheader("Trend — SLA & Quality Checks")
with ctl_col0:
    trend_limit = st.slider("Trend window (months)", min_value=3, max_value=24, value=6, step=1)
with ctl_col1:
    drop_threshold = st.number_input("Max monthly drop (%)", min_value=0.0, max_value=100.0, value=DEFAULT_DROP_THRESHOLD*100, step=1.0)
with ctl_col2:
    null_max = st.number_input("Null rate max", min_value=0.0, max_value=1.0, value=DEFAULT_NULL_MAX, step=0.0001, format="%.4f")
with ctl_col3:
    dup_max = st.number_input("Duplicate rate max", min_value=0.0, max_value=1.0, value=DEFAULT_DUP_MAX, step=0.0001, format="%.4f")
with ctl_col4:
    range_err_max = st.number_input("Range-error rate max", min_value=0.0, max_value=1.0, value=DEFAULT_RANGE_ERR_MAX, step=0.0001, format="%.4f")

trend_raw = fetch_json("/trend", params={"limit": trend_limit})
trend_df = to_df(trend_raw)

if trend_df.empty:
    st.info("No trend data yet.")
else:
    # Derive deltas & SLA flags
    trend_df = compute_trend_metrics(trend_df)
    trend_df = add_sla_flags(trend_df, drop_threshold/100.0, null_max, dup_max, range_err_max)

    show = trend_df.copy()
    show["row_count_delta_pct"] = show["row_count_delta_pct"].map(pct)
    show = show[[
        "load_month", "row_count", "row_count_delta_pct",
        "null_rate", "duplicate_rate", "range_error_rate",
        "delta_ok", "null_ok", "dup_ok", "range_ok", "overall_ok"
    ]].sort_values("load_month")
    st.caption("Trend Table — deltas vs previous month & threshold checks")
    st.dataframe(show, width="stretch")

    # ===== Chart 1: Row-count Delta % with Threshold Band =====
    base = alt.Chart(trend_df.dropna(subset=["row_count_delta_pct"])).encode(
        x=alt.X("load_month:N", title="Month")
    )

    band = alt.Chart(pd.DataFrame({
        "y1": [-(drop_threshold)], "y2": [(drop_threshold)]  # drop allowed from -threshold% to 0%
    })).mark_rect(opacity=0.15).encode(
        y=alt.Y("y1:Q", title="Δ rows (%)"),
        y2="y2:Q"
    ).properties(title=f"Row-count Δ (%) vs previous month — allowed drop ≤ {drop_threshold:.0f}%")

    line = base.mark_line(point=True).encode(
        y=alt.Y("row_count_delta_pct:Q", title="Δ rows (%)"),
        color=alt.condition(
            alt.datum.drop_violation,
            alt.value("#d62728"),   # red if violation
            alt.value("#1f77b4")    # default line color
        )
    )

    rule0 = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(strokeDash=[4,4]).encode(y="y:Q")

    st.altair_chart(band + line + rule0, width='stretch')

    # ===== Chart 2: SLA Compliance Heatmap =====
    # Melt boolean flags for a compact overview
    flags = trend_df[["load_month", "delta_ok", "null_ok", "dup_ok", "range_ok", "overall_ok"]].copy()
    heat = flags.melt(id_vars=["load_month"], var_name="check", value_name="ok")
    heat["ok_label"] = heat["ok"].map(lambda v: "OK" if v else "FAIL")

    heat_chart = alt.Chart(heat).mark_rect().encode(
        x=alt.X("load_month:N", title="Month"),
        y=alt.Y("check:N", title="Check"),
        color=alt.condition(
            alt.datum.ok,
            alt.value("#2ca02c"),
            alt.value("#d62728")
        ),
        tooltip=["load_month", "check", "ok_label"]
    ).properties(title="SLA / Quality Compliance (OK vs FAIL)")

    st.altair_chart(heat_chart, width='stretch')

st.divider()

# -----------------------------------------------------------------------------
# Ingestion Audit
# -----------------------------------------------------------------------------
c_title, c_slider = st.columns([4, 1])
with c_title:
    st.subheader("Ingestion Audit")
with c_slider:
    audit_limit = st.slider("Rows", min_value=5, max_value=50, value=15, step=5, label_visibility="collapsed")

ad = fetch_json("/audit", params={"limit": audit_limit})
adf = to_df(ad)

if adf.empty:
    st.info("No ingestion audit records yet.")
else:
    st.dataframe(adf, width="stretch")
