"""
Data Lakehouse Dashboard — Streamlit
Reads from Go API (Gold layer) + PostgreSQL metastore
"""

import os
import time
from decimal import Decimal

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

API_BASE = os.getenv("API_BASE", "http://api:8080")

PLOTLY = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#161b22",
    font_color="#e6edf3",
    font_family="Inter, sans-serif",
    margin=dict(l=0, r=0, t=36, b=0),
    title_font_size=13,
)
GREEN  = "#23c45e"
RED    = "#f85149"
YELLOW = "#e3b341"
BLUE   = "#388bfd"
PURPLE = "#a78bfa"

DARK_CSS = """
<style>
html,body,[data-testid="stAppViewContainer"],[data-testid="stAppViewBlockContainer"],.main{
    background-color:#0d1117!important;color:#e6edf3!important;}
.main .block-container{background:#0d1117!important;padding:1.5rem 2rem!important;max-width:100%!important;}
section[data-testid="stSidebar"],section[data-testid="stSidebar"]>div{
    background:#0d1117!important;border-right:1px solid #21262d!important;}
section[data-testid="stSidebar"] *{color:#e6edf3!important;}
[data-testid="metric-container"]{background:#161b22!important;border:1px solid #21262d!important;
    border-radius:8px!important;padding:1rem!important;}
[data-testid="stMetricLabel"]{font-size:.72rem!important;color:#8b949e!important;
    text-transform:uppercase;letter-spacing:.06em;}
[data-testid="stMetricValue"]{font-size:1.6rem!important;font-weight:700!important;color:#e6edf3!important;}
.stTabs [data-baseweb="tab-list"]{background:transparent!important;border-bottom:1px solid #21262d!important;}
.stTabs [data-baseweb="tab"]{background:transparent!important;color:#8b949e!important;
    border:none!important;border-bottom:2px solid transparent!important;font-size:.84rem!important;}
.stTabs [aria-selected="true"]{color:#23c45e!important;border-bottom-color:#23c45e!important;}
h1{font-size:1.8rem!important;font-weight:700!important;color:#e6edf3!important;}
h2,h3{color:#e6edf3!important;font-weight:600!important;}
hr{border-color:#21262d!important;}
#MainMenu,footer,header{visibility:hidden!important;}
</style>
"""

st.set_page_config(page_title="Data Lakehouse", page_icon="🏔️",
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(DARK_CSS, unsafe_allow_html=True)


# ── API helpers ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=30, show_spinner=False)
def api_get(path: str, params: dict = None):
    try:
        r = requests.get(f"{API_BASE}{path}", params=params, timeout=8)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error ({path}): {e}")
        return None


def api_health() -> bool:
    try:
        return requests.get(f"{API_BASE}/health", timeout=2).status_code == 200
    except Exception:
        return False


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar():
    with st.sidebar:
        st.markdown('<div style="font-size:1rem;font-weight:600;padding-bottom:.8rem">🏔️ Data <span style="color:#23c45e">Lakehouse</span></div>',
                    unsafe_allow_html=True)
        ok = api_health()
        color = "#23c45e" if ok else "#f85149"
        bg    = "#0F6E56" if ok else "#791F1F"
        label = "● API Connected" if ok else "✕ API unreachable"
        st.markdown(f'<div style="background:{bg};color:{color};font-weight:600;font-size:.78rem;border-radius:6px;padding:.35rem 1rem;text-align:center">{label}</div>',
                    unsafe_allow_html=True)
        st.markdown(f'<div style="font-family:monospace;font-size:.7rem;color:#23c45e;background:rgba(35,196,94,.08);border:1px solid rgba(35,196,94,.2);border-radius:4px;padding:.2rem .5rem;margin:.3rem 0">{API_BASE}</div>',
                    unsafe_allow_html=True)
        st.divider()
        if st.button("↺ Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        if st.checkbox("Auto-refresh 30s"):
            time.sleep(30)
            st.cache_data.clear()
            st.rerun()

        st.divider()
        st.markdown('<p style="font-size:.72rem;color:#6e7681">Layers</p>', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:.75rem;color:#8b949e;line-height:2.2">
        <span style="color:#e3b341">🥉 Bronze</span> — Raw landing zone<br>
        <span style="color:#c0c0c0">🥈 Silver</span> — Cleaned & validated<br>
        <span style="color:#FFD700">🥇 Gold</span> — Business aggregates<br>
        <span style="color:#23c45e">📊 dbt</span> — SQL transformations
        </div>""", unsafe_allow_html=True)


# ── Tab: Overview ─────────────────────────────────────────────────────────────
def tab_overview():
    summary = api_get("/api/v1/summary")
    if not summary:
        st.info("Waiting for pipeline to run…"); return

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("💰 Revenue (30d)",   f"${float(summary.get('total_revenue',0)):,.0f}")
    c2.metric("🛒 Orders (30d)",    f"{int(summary.get('total_orders',0)):,}")
    c3.metric("👥 Customers",       f"{int(summary.get('unique_customers',0)):,}")
    c4.metric("💎 Avg Order",       f"${float(summary.get('avg_order_value',0)):.2f}")
    c5.metric("👑 VIP Customers",   f"{int(summary.get('vip_customers',0)):,}")
    c6.metric("⚠️ At Risk",          f"{int(summary.get('at_risk_customers',0)):,}")

    st.divider()
    col_l, col_r = st.columns([1.6, 1])

    with col_l:
        st.markdown("#### Revenue — last 30 days")
        data = api_get("/api/v1/daily-sales", {"days": 30})
        if data:
            df = pd.DataFrame(data)
            df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df["date"], y=df["total_revenue"],
                                  marker_color=BLUE, marker_line_width=0, opacity=0.7,
                                  name="Revenue"))
            fig.add_trace(go.Scatter(x=df["date"], y=df["total_orders"],
                                      line_color=GREEN, yaxis="y2", name="Orders",
                                      mode="lines+markers", line_width=2, marker_size=4))
            fig.update_layout(**PLOTLY, height=300,
                              yaxis2=dict(overlaying="y", side="right", showgrid=False),
                              legend=dict(orientation="h", y=1.1))
            st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.markdown("#### Revenue trend (7d avg)")
        data = api_get("/api/v1/revenue-trends")
        if data:
            df = pd.DataFrame(data).head(30)
            df["revenue_7d_avg"] = pd.to_numeric(df["revenue_7d_avg"], errors="coerce")
            fig = px.line(df, x="date", y="revenue_7d_avg",
                          color_discrete_sequence=[PURPLE])
            fig.update_traces(line_width=2, fill="tozeroy",
                              fillcolor="rgba(167,139,250,0.1)")
            fig.update_layout(**{**PLOTLY, "margin": dict(l=0,r=0,t=10,b=0)}, height=300)
            st.plotly_chart(fig, use_container_width=True)


# ── Tab: Products ─────────────────────────────────────────────────────────────
def tab_products():
    st.markdown("#### 🏆 Top Products")

    data = api_get("/api/v1/top-products", {"limit": 15})
    if not data:
        st.info("No product data yet."); return

    df = pd.DataFrame(data)
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    col_l, col_r = st.columns(2)
    with col_l:
        fig = px.bar(df.head(10), x="revenue", y="product_name", orientation="h",
                     color="category", title="Revenue by product ($)")
        fig.update_layout(**PLOTLY, height=350, yaxis={"categoryorder":"total ascending"})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        fig = px.bar(df.head(10), x="units_sold", y="product_name", orientation="h",
                     color="category", title="Units sold")
        fig.update_layout(**PLOTLY, height=350, yaxis={"categoryorder":"total ascending"})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### By category")
    if not df.empty:
        cat = df.groupby("category").agg(revenue=("revenue","sum"),
                                          units=("units_sold","sum")).reset_index()
        fig = px.pie(cat, names="category", values="revenue",
                     color_discrete_sequence=px.colors.qualitative.Vivid, hole=0.4)
        fig.update_layout(**{**PLOTLY, "margin": dict(l=0,r=0,t=10,b=0)}, height=280)
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df[["rank","product_name","category","units_sold","revenue",
                      "avg_price","return_rate_pct"]],
                 use_container_width=True, hide_index=True,
                 column_config={
                     "revenue":         st.column_config.NumberColumn("Revenue",   format="$%.2f"),
                     "avg_price":       st.column_config.NumberColumn("Avg Price", format="$%.2f"),
                     "return_rate_pct": st.column_config.NumberColumn("Return %",  format="%.1f%%"),
                 })


# ── Tab: Customers ────────────────────────────────────────────────────────────
def tab_customers():
    st.markdown("#### 👥 Customer Segments")

    data = api_get("/api/v1/customer-segments")
    if data:
        df = pd.DataFrame(data)
        df["avg_spent"] = pd.to_numeric(df["avg_spent"], errors="coerce")

        col1, col2, col3 = st.columns(3)
        colors = {"vip": "#FFD700", "premium": PURPLE, "regular": BLUE,
                  "new": GREEN, "at_risk": RED}

        for i, row in enumerate(df.itertuples()):
            col = [col1, col2, col3][i % 3]
            color = colors.get(row.segment, "#8b949e")
            col.markdown(f"""
            <div style="background:#161b22;border:1px solid #21262d;border-left:3px solid {color};
                        border-radius:8px;padding:1rem;margin:.4rem 0">
              <div style="font-size:.8rem;color:{color};font-weight:600;text-transform:uppercase">{row.segment}</div>
              <div style="font-size:1.5rem;font-weight:700;color:#e6edf3">{int(row.count):,}</div>
              <div style="font-size:.75rem;color:#8b949e">Avg spent: ${float(row.avg_spent):,.0f}</div>
              <div style="font-size:.75rem;color:#8b949e">Avg orders: {float(row.avg_orders):.1f}</div>
            </div>""", unsafe_allow_html=True)

    st.divider()
    st.markdown("#### Customer LTV lookup")
    cid = st.number_input("Customer ID", min_value=1, max_value=500, value=1, step=1)
    if st.button("Lookup", use_container_width=False):
        ltv = api_get(f"/api/v1/customer/{cid}/ltv")
        if ltv:
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Segment",       ltv.get("segment","—"))
            col2.metric("RFM Segment",   ltv.get("rfm_segment","—"))
            col3.metric("Total Spent",   f"${float(ltv.get('total_spent',0)):,.2f}")
            col4.metric("CLV Score",     f"{float(ltv.get('clv_score',0)):.4f}")
            st.json(ltv)


# ── Tab: Pipeline ─────────────────────────────────────────────────────────────
def tab_pipeline():
    st.markdown("#### ⚙️ Pipeline Runs")

    data = api_get("/api/v1/pipeline-runs")
    if data:
        df = pd.DataFrame(data)
        if not df.empty:
            # Color by status
            success = len(df[df["status"] == "success"])
            failed  = len(df[df["status"] == "failed"])
            running = len(df[df["status"] == "running"])

            c1,c2,c3 = st.columns(3)
            c1.metric("✅ Success", success)
            c2.metric("❌ Failed",  failed)
            c3.metric("🔄 Running", running)

            st.dataframe(df[["id","pipeline_name","layer","status",
                              "rows_read","rows_written","duration_ms","started_at"]],
                         use_container_width=True, hide_index=True,
                         column_config={
                             "status": st.column_config.TextColumn("Status"),
                             "duration_ms": st.column_config.NumberColumn("Duration (ms)"),
                         })

    st.divider()
    st.markdown("#### 📋 Table Catalog")
    cat = api_get("/api/v1/catalog")
    if cat:
        df_cat = pd.DataFrame(cat)
        if not df_cat.empty:
            st.dataframe(df_cat, use_container_width=True, hide_index=True,
                         column_config={
                             "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                             "size_bytes": st.column_config.NumberColumn("Size (bytes)"),
                         })

    st.divider()
    st.markdown("#### 🔍 Data Quality Results")
    dq = api_get("/api/v1/dq-results")
    if dq:
        df_dq = pd.DataFrame(dq)
        if not df_dq.empty:
            pass_rate = len(df_dq[df_dq["status"] == "pass"]) / len(df_dq) * 100
            st.metric("DQ Pass rate", f"{pass_rate:.1f}%")
            st.dataframe(df_dq[["table_name","layer","check_name","status",
                                  "rows_tested","rows_failed","run_at"]],
                         use_container_width=True, hide_index=True)


# ── Main ──────────────────────────────────────────────────────────────────────
def render_ml_tab():
    st.header("🤖 ML Insights")

    try:
        summary = requests.get(f"{API_BASE}/api/v1/ml/features/summary", timeout=5).json()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Customers analysés", summary.get("total_customers", 0))
        c2.metric("🔴 Haut risque", summary.get("high_risk_count", 0))
        c3.metric("Churn prob. moy.", f"{summary.get('avg_churn_probability', 0):.1%}")
        c4.metric("CLV moyen", f"${summary.get('avg_clv_score', 0):,.0f}")
    except:
        st.warning("Lance d'abord : docker compose run --rm ml-feature-pipeline")

    st.subheader("🚨 Clients à haut risque de churn")
    try:
        churn = requests.get(f"{API_BASE}/api/v1/ml/churn-risk", timeout=5).json()
        if churn:
            import pandas as pd
            df = pd.DataFrame(churn)
            df["churn_probability"] = df["churn_probability"].map(lambda x: f"{x:.1%}")
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Aucune donnée — feature pipeline non encore exécuté")
    except Exception as e:
        st.error(str(e))

    st.subheader("🔍 Analyse individuelle")
    cid = st.text_input("Customer ID")
    if cid:
        try:
            r = requests.get(f"{API_BASE}/api/v1/ml/customer/{cid}", timeout=5)
            if r.status_code == 200:
                d = r.json()
                c1, c2, c3 = st.columns(3)
                c1.metric("Churn probability", f"{d['churn_probability']:.1%}")
                c2.metric("CLV Score", f"${d['clv_score']:,.0f}")
                c3.metric("Jours sans achat", d['days_since_last'])
                st.json(d)
            else:
                st.warning("Client non trouvé")
        except Exception as e:
            st.error(str(e))

    st.info("**Modèles** : GradientBoosting (churn) + RandomForest (LTV) | "
            "**Features** : RFM depuis Iceberg Silver | "
            "**Tracking** : MLflow → http://localhost:5001")


def render_clickhouse_tab():
    st.header("⚡ ClickHouse Analytics")
    st.caption("OLAP engine — requêtes analytiques sur milliards de lignes en <1s")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Stats globales")
        try:
            r = requests.get(f"{API_BASE}/api/v1/clickhouse/stats", timeout=5).json()
            if "data" in r and r["data"]:
                row = r["data"][0]
                total_rows = int(str(row.get('total_rows', 0)))
                total_rev = float(row.get('total_revenue', 0))
                st.metric("Total rows", f"{total_rows:,}")
                st.metric("Revenue total", f"${total_rev:,.0f}")
                st.metric("Dernière date", str(row.get('latest_date', '-')))
            else:
                st.info("Lance d'abord le sync : `docker compose run --rm clickhouse-sync`")
        except Exception as e:
            st.warning(f"Stats non disponibles: {e}")

    with col2:
        st.subheader("🎯 Churn Analytics")
        try:
            r = requests.get(f"{API_BASE}/api/v1/clickhouse/churn-stats", timeout=5).json()
            if "data" in r and r["data"]:
                row = r["data"][0]
                st.metric("🔴 Haut risque", int(float(row.get('high_risk', 0))))
                st.metric("🟡 Risque moyen", int(float(row.get('medium_risk', 0))))
                st.metric("🟢 Faible risque", int(float(row.get('low_risk', 0))))
            else:
                st.info("Données non disponibles")
        except Exception as e:
            st.warning(f"Erreur: {e}")

    st.subheader("🏆 Top Produits (ClickHouse)")
    try:
        r = requests.get(f"{API_BASE}/api/v1/clickhouse/top-products", timeout=5).json()
        if "data" in r and r["data"]:
            import pandas as pd
            df = pd.DataFrame(r["data"])
            df["revenue"] = df["revenue"].astype(float).map(lambda x: f"${x:,.0f}")
            st.dataframe(df, use_container_width=True)
        else:
            st.info("Lance d'abord : `docker compose run --rm clickhouse-sync`")
    except Exception as e:
        st.warning(f"Erreur: {e}")

    st.info("""
    **ClickHouse** : moteur OLAP columnar — même données que PostgreSQL mais optimisé
    pour les agrégations sur des milliards de lignes.
    Endpoint HTTP : **http://localhost:8123**
    """)


def main():
    render_sidebar()
    st.title("🏔️ Data Lakehouse")

    t1, t2, t3, t4, t5, t6 = st.tabs([
        "📊 Overview", "📦 Products", "👥 Customers", "⚙️ Pipeline",
        "🤖 ML Insights",
        "⚡ ClickHouse"
    ])
    with t1: tab_overview()
    with t2: tab_products()
    with t3: tab_customers()
    with t4: tab_pipeline()
    with t5: render_ml_tab()
    with t6: render_clickhouse_tab()


if __name__ == "__main__":
    main()
