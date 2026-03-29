"""
dashboard/app.py
Streamlit Analytics Dashboard
Deploy FREE at: share.streamlit.io
Run locally: streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RetailLake Dashboard",
    page_icon="🏬",
    layout="wide"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #0e1117; }
    .metric-card {
        background: linear-gradient(135deg, #1e2130, #252a3d);
        border: 1px solid #2d3250;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #4fc3f7;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #8892b0;
        margin-top: 4px;
    }
    .section-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #ccd6f6;
        border-left: 3px solid #4fc3f7;
        padding-left: 10px;
        margin: 20px 0 10px 0;
    }
    .stMetric label { color: #8892b0 !important; }
    .stMetric [data-testid="metric-container"] { background: #1e2130; border-radius: 8px; padding: 10px; }
</style>
""", unsafe_allow_html=True)

# ── Data Loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data():
    base = "data/mart"
    data = {}
    files = {
        "fact":      "fact_orders.parquet",
        "monthly":   "mart_monthly_revenue.parquet",
        "customers": "mart_customer_segments.parquet",
        "products":  "mart_product_performance.parquet",
        "dim_cust":  "dim_customers.parquet",
        "dim_prod":  "dim_products.parquet",
    }
    for key, fname in files.items():
        path = os.path.join(base, fname)
        if os.path.exists(path):
            data[key] = pd.read_parquet(path)
        else:
            data[key] = pd.DataFrame()
    return data


data = load_data()
fact      = data["fact"]
monthly   = data["monthly"]
customers = data["customers"]
products  = data["products"]

# Completed orders only for revenue metrics
completed = fact[fact["status"] == "completed"] if len(fact) > 0 else fact

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/000000/shopping-cart.png", width=60)
    st.title("RetailLake — Analytics Dashboard")
    st.markdown("---")

    if len(fact) > 0:
        years = sorted(fact["order_year"].unique(), reverse=True)
        sel_year = st.selectbox("📅 Year", ["All"] + [str(y) for y in years])

        regions = ["All"] + sorted(fact["region"].dropna().unique().tolist())
        sel_region = st.selectbox("🌍 Region", regions)

        categories = ["All"] + sorted(fact["category"].dropna().unique().tolist())
        sel_cat = st.selectbox("📦 Category", categories)

        st.markdown("---")
        st.markdown("**🛠️ Built With**")
        st.markdown("• Python + Pandas")
        st.markdown("• Delta Lake + Parquet")
        st.markdown("• Apache Airflow")
        st.markdown("• dbt (Star Schema)")
        st.markdown("• Streamlit")
        st.markdown("---")
        st.markdown("**👨‍💻 Karthik Surya J**")
        st.markdown("[GitHub](https://github.com/karthiksuryaarasan) | [LinkedIn](https://linkedin.com/in/karthik-surya-837219264)")

# ── Apply Filters ─────────────────────────────────────────────────────────────
filtered = completed.copy()
if len(filtered) > 0:
    if sel_year != "All":
        filtered = filtered[filtered["order_year"] == int(sel_year)]
    if sel_region != "All":
        filtered = filtered[filtered["region"] == sel_region]
    if sel_cat != "All":
        filtered = filtered[filtered["category"] == sel_cat]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏬 RetailLake — Analytics Platform")
st.caption("Modern Lakehouse Platform | Bronze → Silver → Gold | Delta Lake | Airflow | CI/CD")
st.markdown("---")

if len(fact) == 0:
    st.error("❌ No data found. Run `python run_pipeline.py` first!")
    st.stop()

# ── KPI Cards ─────────────────────────────────────────────────────────────────
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("Total Orders",      f"{len(filtered):,}")
with col2:
    st.metric("Total Revenue",     f"₹{filtered['net_revenue'].sum()/1e6:.1f}M")
with col3:
    st.metric("Avg Order Value",   f"₹{filtered['net_revenue'].mean():,.0f}")
with col4:
    st.metric("Unique Customers",  f"{filtered['customer_id'].nunique():,}")
with col5:
    cancel_rate = len(fact[fact["status"]=="cancelled"]) / len(fact) * 100
    st.metric("Cancellation Rate", f"{cancel_rate:.1f}%")

st.markdown("---")

# ── Revenue Over Time ─────────────────────────────────────────────────────────
st.markdown('<p class="section-title">📈 Monthly Revenue Trend</p>', unsafe_allow_html=True)

if len(filtered) > 0:
    monthly_agg = filtered.groupby(["order_year", "order_month"]).agg(
        revenue=("net_revenue", "sum"),
        orders=("order_id", "count")
    ).reset_index()
    monthly_agg["period"] = monthly_agg["order_year"].astype(str) + "-" + \
                             monthly_agg["order_month"].astype(str).str.zfill(2)
    monthly_agg = monthly_agg.sort_values("period")

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=monthly_agg["period"], y=monthly_agg["revenue"],
        name="Revenue (₹)", marker_color="#4fc3f7", opacity=0.8
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=monthly_agg["period"], y=monthly_agg["orders"],
        name="Orders", line=dict(color="#f48fb1", width=2), mode="lines+markers"
    ), secondary_y=True)
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="#ccd6f6", height=350,
        legend=dict(orientation="h", y=1.1),
        margin=dict(l=0, r=0, t=30, b=0)
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#1e2130")
    st.plotly_chart(fig, use_container_width=True)

# ── Row 2: Region + Category ──────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.markdown('<p class="section-title">🌍 Revenue by Region</p>', unsafe_allow_html=True)
    region_data = filtered.groupby("region")["net_revenue"].sum().reset_index()
    fig2 = px.pie(region_data, values="net_revenue", names="region",
                  color_discrete_sequence=px.colors.sequential.Blues_r,
                  hole=0.5)
    fig2.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="#ccd6f6", height=300,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=True
    )
    st.plotly_chart(fig2, use_container_width=True)

with col_right:
    st.markdown('<p class="section-title">📦 Top Categories by Revenue</p>', unsafe_allow_html=True)
    cat_data = filtered.groupby("category")["net_revenue"].sum().nlargest(8).reset_index()
    fig3 = px.bar(cat_data, x="net_revenue", y="category", orientation="h",
                  color="net_revenue", color_continuous_scale="Blues")
    fig3.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="#ccd6f6", height=300,
        margin=dict(l=0, r=0, t=10, b=0),
        coloraxis_showscale=False,
        yaxis=dict(autorange="reversed")
    )
    fig3.update_xaxes(showgrid=True, gridcolor="#1e2130")
    fig3.update_yaxes(showgrid=False)
    st.plotly_chart(fig3, use_container_width=True)

# ── Row 3: Order Status + Payment ─────────────────────────────────────────────
col3a, col3b = st.columns(2)

with col3a:
    st.markdown('<p class="section-title">📊 Order Status Distribution</p>', unsafe_allow_html=True)
    status_data = fact.groupby("status")["order_id"].count().reset_index()
    status_data.columns = ["status", "count"]
    colors = {"completed": "#4fc3f7", "pending": "#ffd54f",
              "cancelled": "#ef9a9a", "refunded": "#ce93d8"}
    fig4 = px.bar(status_data, x="status", y="count",
                  color="status", color_discrete_map=colors)
    fig4.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="#ccd6f6", height=280,
        showlegend=False, margin=dict(l=0, r=0, t=10, b=0)
    )
    st.plotly_chart(fig4, use_container_width=True)

with col3b:
    st.markdown('<p class="section-title">💳 Payment Method Split</p>', unsafe_allow_html=True)
    pay_data = filtered.groupby("payment_method")["net_revenue"].sum().reset_index()
    fig5 = px.pie(pay_data, values="net_revenue", names="payment_method",
                  color_discrete_sequence=px.colors.qualitative.Set3, hole=0.4)
    fig5.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="#ccd6f6", height=280,
        margin=dict(l=0, r=0, t=10, b=0)
    )
    st.plotly_chart(fig5, use_container_width=True)

# ── Top Products Table ────────────────────────────────────────────────────────
st.markdown("---")
st.markdown('<p class="section-title">🏆 Top 10 Products by Revenue</p>', unsafe_allow_html=True)

if len(products) > 0:
    top_prods = products.nlargest(10, "total_revenue")[[
        "product_id", "category", "brand",
        "total_orders", "total_revenue", "profit_margin_pct"
    ]].copy()
    top_prods["total_revenue"] = top_prods["total_revenue"].apply(lambda x: f"₹{x:,.0f}")
    top_prods["profit_margin_pct"] = top_prods["profit_margin_pct"].apply(lambda x: f"{x:.1f}%")
    st.dataframe(top_prods, use_container_width=True, hide_index=True)

# ── Customer Segment ──────────────────────────────────────────────────────────
st.markdown('<p class="section-title">👥 Customer Segment Analysis</p>', unsafe_allow_html=True)

if len(customers) > 0:
    seg_data = customers.groupby("segment").agg(
        customers=("customer_id", "count"),
        total_revenue=("total_spent", "sum"),
        avg_spent=("total_spent", "mean")
    ).reset_index()

    col_s1, col_s2, col_s3 = st.columns(3)
    for i, row in seg_data.iterrows():
        col = [col_s1, col_s2, col_s3][i % 3]
        with col:
            st.metric(
                f"{row['segment']} Customers",
                f"{row['customers']:,}",
                f"Avg ₹{row['avg_spent']:,.0f}"
            )

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "**Built by Karthik Surya J** | "
    "Stack: Python • Delta Lake • Parquet • Airflow • dbt • Streamlit | "
    "[GitHub](https://github.com/karthiksuryaarasan)"
)
