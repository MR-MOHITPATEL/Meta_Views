"""
app.py — Streamlit UI for the NL Analytics System.

Architecture:
  Data loads once (cached 1hr) → User asks question → Router classifies →
  Query function aggregates → LLM explains → UI renders structured output.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import datetime
import logging
import sys
import os

logger = logging.getLogger(__name__)

# Add the project root to sys.path so 'analytics' package can be found
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics.data_layer   import get_campaign_analytics_data
from analytics.query_router import route_query
from analytics.llm_layer    import get_llm_explanation
from analytics.query_layer   import apply_view_filters, compute_direct_answer
from analytics.config import (
    COL_DATE, COL_CAMPAIGN, COL_AD, COL_ADSET,
    COL_SPEND, COL_PURCHASES, COL_REVENUE,
    DEFAULT_CPT_THRESHOLD, DEFAULT_PURCHASE_THRESHOLD
)

# ── Page Config ────────────────────────────────────────────────
st.set_page_config(
    page_title="ZenJeevani Campaign Analytics",
    page_icon="🎯",
    layout="wide",
)

# ── Custom Styles ──────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800&display=swap');
    
    html, body, [class*="css"] { font-family: 'Outfit', sans-serif; }

    .main-header {
        background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 100%);
        padding: 3rem 2rem; border-radius: 24px; margin-bottom: 2.5rem;
        color: white; text-align: center;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .main-header h1 { font-size: 2.5rem; font-weight: 800; margin: 0; letter-spacing: -0.02em; }
    .main-header p  { opacity: 0.7; margin-top: 0.8rem; font-size: 1.1rem; font-weight: 300; }

    /* The core Answer highlight */
    .answer-card {
        background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
        padding: 2.5rem; border-radius: 20px; color: white;
        text-align: center; margin-bottom: 2rem;
        box-shadow: 0 20px 25px -5px rgba(79, 70, 229, 0.2);
    }
    .answer-card .number { font-size: 4rem; font-weight: 800; line-height: 1; }
    .answer-card .label  { font-size: 1.1rem; opacity: 0.9; margin-top: 1rem; font-weight: 400; text-transform: uppercase; letter-spacing: 0.05em; }

    /* Insights & Actions - ENFORCED DARK TEXT FOR READABILITY */
    .insight-card {
        background: #ffffff; 
        color: #0f172a !important; 
        border-left: 6px solid #4f46e5;
        padding: 1.25rem 1.5rem; border-radius: 12px; margin-bottom: 1rem;
        font-size: 1rem; line-height: 1.6;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    .action-card {
        background: #f0fdf4; 
        color: #166534 !important; 
        border-left: 6px solid #22c55e;
        padding: 1.25rem 1.5rem; border-radius: 12px; margin-bottom: 1rem;
        font-size: 1rem; line-height: 1.6;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }
    
    .insight-card p, .action-card p { color: inherit !important; }

    .source-badge {
        background: #f1f5f9; color: #475569; padding: 0.4rem 1rem;
        border-radius: 9999px; font-size: 0.8rem; font-weight: 600;
        border: 1px solid #e2e8f0;
    }

    /* Metric Boxes (KPIs) */
    .metric-box {
        background: white; border: 1px solid #e2e8f0;
        border-radius: 16px; padding: 1.5rem; text-align: center;
        transition: transform 0.2s, box-shadow 0.2s;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    .metric-box:hover { transform: translateY(-2px); box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1); }
    .metric-box .val { font-size: 2rem; font-weight: 800; color: #1e293b; }
    .metric-box .lbl { font-size: 0.9rem; color: #64748b; margin-top: 0.4rem; font-weight: 500; text-transform: uppercase; }
    
    /* Inputs & Buttons */
    div[data-testid="stTextInput"] > label { font-weight: 600; color: #334155; }
    .stButton > button {
        background: #4f46e5;
        color: white !important; border: none; padding: 0.75rem 2.5rem;
        border-radius: 12px; font-weight: 700; transition: all 0.2s;
        box-shadow: 0 4px 6px -1px rgba(79, 70, 229, 0.2);
        width: 100%;
    }
    .stButton > button:hover { background: #4338ca; box-shadow: 0 10px 15px -3px rgba(79, 70, 229, 0.3); transform: translateY(-1px); }
    
    /* Tables and Dataframes */
    [data-testid="stDataFrame"] {
        border-radius: 16px; overflow: hidden; border: 1px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)


# ── Header ─────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1>🎯 ZenJeevani Campaign Analytics</h1>
    <p>Ask questions about your Meta Ads performance in plain English</p>
</div>
""", unsafe_allow_html=True)


# ── Load Data ──────────────────────────────────────────────────
try:
    df_raw, data_source, error_msg = get_campaign_analytics_data()
    if error_msg:
        st.sidebar.warning(f"⚠️ Google Sheets Error: {error_msg}. Using local fallback instead.")
except RuntimeError as e:
    st.error(f"❌ Could not load data: {e}")
    st.stop()


# ── Sidebar Filters ────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🔧 Filters")
    st.markdown(f'<span class="source-badge">📡 {data_source}</span>', unsafe_allow_html=True)
    st.markdown(f"**{len(df_raw):,}** total records loaded")
    st.divider()

    # Date range
    if COL_DATE in df_raw.columns:
        dates = pd.to_datetime(df_raw[COL_DATE])
        min_date = dates.min().date()
        max_date = dates.max().date()
        date_range = st.date_input(
            "📅 Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    else:
        date_range = None

    # Campaign filter
    campaigns = ["All"] + sorted(df_raw[COL_CAMPAIGN].dropna().unique().tolist()) if COL_CAMPAIGN in df_raw.columns else ["All"]
    selected_campaign = st.selectbox("📣 Campaign", campaigns)

    # Ad/Creative filter
    ads = ["All"] + sorted(df_raw[COL_AD].dropna().unique().tolist()) if COL_AD in df_raw.columns else ["All"]
    selected_ad = st.selectbox("🎨 Ad / Creative", ads)

    st.divider()
    st.markdown("### ⚙️ Query Defaults")
    cpt_threshold      = st.number_input("CPT Threshold (₹)", value=DEFAULT_CPT_THRESHOLD, step=10)
    purchase_threshold = st.number_input("Min Purchases",      value=DEFAULT_PURCHASE_THRESHOLD, step=1)

    st.divider()
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# ── Apply Filters ──────────────────────────────────────────────
df = df_raw.copy()

if date_range and len(date_range) == 2 and COL_DATE in df.columns:
    start_d, end_d = date_range
    df[COL_DATE] = pd.to_datetime(df[COL_DATE])
    df = df[(df[COL_DATE].dt.date >= start_d) & (df[COL_DATE].dt.date <= end_d)]

if selected_campaign != "All" and COL_CAMPAIGN in df.columns:
    df = df[df[COL_CAMPAIGN] == selected_campaign]

if selected_ad != "All" and COL_AD in df.columns:
    df = df[df[COL_AD] == selected_ad]


# ── KPI Summary Row ────────────────────────────────────────────
st.markdown("### 📊 Overview")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

total_spend      = df[COL_SPEND].sum()    if COL_SPEND     in df.columns else 0
total_purchases  = df[COL_PURCHASES].sum() if COL_PURCHASES in df.columns else 0
total_revenue    = df[COL_REVENUE].sum()   if "revenue"     in df.columns else 0
overall_cpt      = round(total_spend / total_purchases, 2) if total_purchases > 0 else 0
overall_roas     = round(total_revenue / total_spend, 2)   if total_spend > 0     else 0

with kpi1:
    st.markdown(f'<div class="metric-box"><div class="val">₹{total_spend:,.0f}</div><div class="lbl">Total Spend</div></div>', unsafe_allow_html=True)
with kpi2:
    st.markdown(f'<div class="metric-box"><div class="val">{total_purchases:,.0f}</div><div class="lbl">Purchases</div></div>', unsafe_allow_html=True)
with kpi3:
    st.markdown(f'<div class="metric-box"><div class="val">₹{total_revenue:,.0f}</div><div class="lbl">Revenue</div></div>', unsafe_allow_html=True)
with kpi4:
    st.markdown(f'<div class="metric-box"><div class="val">₹{overall_cpt:,.0f}</div><div class="lbl">Avg CPT</div></div>', unsafe_allow_html=True)
with kpi5:
    st.markdown(f'<div class="metric-box"><div class="val">{overall_roas:.2f}x</div><div class="lbl">ROAS</div></div>', unsafe_allow_html=True)


# ── NL Question Input ──────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown("### 💬 Ask a Question")

EXAMPLE_QUESTIONS = [
    "Daily pincode usage for last 7 days",
    "Pincode wise performance in April",
    "Creative performance summary",
    "Show me the winning creatives",
    "Best performing pincodes by purchases",
]

col_q, col_ex = st.columns([3, 1])
with col_q:
    question = st.text_input(
        "Ask your question",
        placeholder="e.g. Daily pincode usage for last 7 days",
        label_visibility="collapsed",
    )
with col_ex:
    example = st.selectbox("💡 Examples", [""] + EXAMPLE_QUESTIONS, label_visibility="collapsed")
    if example:
        question = example

ask_col, _ = st.columns([1, 3])
with ask_col:
    run_query = st.button("🔍 Analyse", use_container_width=True)


# ── Execute Query ──────────────────────────────────────────────
if run_query and question:
    st.divider()

    # STEP 1 & 2: Route and Select Sheet
    with st.spinner("🧠 Routing to correct dataset..."):
        route = route_query(question)
        sheet_to_use = route["selected_sheet"]
    
    # Load the specific view
    try:
        df_view, data_source, error_msg = get_campaign_analytics_data(sheet_to_use)
    except Exception as e:
        st.error(f"❌ Failed to load dataset '{sheet_to_use}': {e}")
        st.stop()

    # STEP 3: Apply Filters
    
    # Extract time filter from question (simple regex fallback)
    time_filter = 30 # Default
    if "7 days" in question.lower(): time_filter = 7
    elif "10 days" in question.lower(): time_filter = 10
    elif "30 days" in question.lower(): time_filter = 30

    with st.spinner(f"⚙️ Filtering {sheet_to_use}..."):
        # We also pass sidebar filters if applicable
        camp_filter = selected_campaign if selected_campaign != "All" else None
        
        output = apply_view_filters(
            df_view, 
            sheet_to_use, 
            time_filter_days=time_filter,
            campaign_filter=camp_filter
        )
    
    data_df = output["data"]
    summary = output["summary"]

    # STEP 4, 5, 6: AI Analysis & Answer
    if not data_df.empty:
        with st.spinner("🤖 Synthesizing answer..."):
            llm_out = get_llm_explanation(question, sheet_to_use, summary, data_df)
        
        # ── UI Display ─────────────────────────────────────────
        st.markdown(f"### 🎯 Results from `{sheet_to_use}`")
        
        # STEP 4: Deterministic direct answer — ALWAYS computed from the dataset
        direct_answer = compute_direct_answer(question, summary)
        llm_explanation = ""
        llm_used_label = ""

        # Try LLM for a richer explanation (non-blocking)
        try:
            with st.spinner("🤖 Computing explanation..."):
                llm_out = get_llm_explanation(question, sheet_to_use, summary, data_df)
                llm_explanation = llm_out.get("explanation", "")
                llm_used_label = llm_out.get("llm_used", "")
        except Exception as e:
            llm_explanation = ""
            logger.warning(f"LLM explanation skipped: {e}")
        
        # Answer Card — deterministic value always shown
        st.markdown(f"""
        <div class="answer-card">
            <div class="label">📊 Direct Answer</div>
            <div style="font-size: 1.6rem; font-weight: 700; margin-top: 12px; line-height: 1.4;">
                {direct_answer}
            </div>
        </div>""", unsafe_allow_html=True)
        
        # Show LLM explanation if available
        if llm_explanation:
            st.markdown(f"> 🤖 *{llm_explanation}*&nbsp;&nbsp;<span class='source-badge'>{llm_used_label}</span>", unsafe_allow_html=True)
        # Data Actions
        col_dl, col_src = st.columns([1, 4])
        with col_dl:
            csv = data_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{sheet_to_use.lower()}_report.csv",
                mime="text/csv",
                use_container_width=True
            )
        with col_src:
            st.markdown(f'<span class="source-badge">📡 Data Source: {data_source}</span>', unsafe_allow_html=True)

        # Table Display
        st.markdown("### 📋 Filtered Data View")
        st.dataframe(data_df, use_container_width=True, hide_index=True)
        
        # Plots (Contextual)
        if COL_DATE in data_df.columns and len(data_df) > 1:
            # Dynamically select the first numeric column for the Y axis
            numeric_cols = data_df.select_dtypes(include=['number']).columns.tolist()
            y_axis = numeric_cols[0] if numeric_cols else data_df.columns[-1]
            
            fig = px.line(data_df, x=COL_DATE, y=y_axis, 
                         title=f"📈 {y_axis.title()} Trend Over Time", 
                         template="plotly_white")
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info(f"ℹ️ No matching records found in `{sheet_to_use}` for your filters.")

elif run_query and not question:
    st.warning("⚠️ Please enter a question first.")

else:
    # Landing state — show quick examples
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 🚀 Try These Queries")
    cols = st.columns(3)
    for i, q in enumerate(EXAMPLE_QUESTIONS):
        with cols[i % 3]:
            st.markdown(f"""
            <div class="insight-card" style="cursor:pointer;">
                💬 {q}
            </div>""", unsafe_allow_html=True)
