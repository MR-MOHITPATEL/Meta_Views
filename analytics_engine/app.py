"""
BCT Analytics Engine — Streamlit Chat UI

Run with:
    streamlit run app.py
"""

import io
import os
import subprocess
import sys
from datetime import date

import pandas as pd
import requests
import streamlit as st

from aggregator import compute
from chart_builder import build_chart
from output_formatter import build_csv_bytes, build_excel_bytes, build_json_output, build_plain_text
from query_parser import parse_query
from sheets_loader import load_sheet
from view_builder import build_all_views
from sheets_writer import write_all_views
from custom_view import (
    ALL_DIMENSIONS, ALL_METRICS, DEFAULT_METRICS, PCT_METRICS,
    build_custom_view, build_formatted_excel,
    load_saved_configs, save_config, delete_config,
    write_custom_view_to_sheets,
)

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="BCT Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("📊 BCT Analytics Engine")
st.caption("Ask any question about your campaign data in plain English.")

# ── Top-level tabs ─────────────────────────────────────────────────────────────
_tab_chat, _tab_builder = st.tabs(["💬 Chat", "🔨 View Builder"])

# ── Session state ──────────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = []
if "question_history" not in st.session_state:
    st.session_state.question_history = []   # ordered list, newest first, unique
if "rerun_prompt" not in st.session_state:
    st.session_state.rerun_prompt = None
if "chart_visible" not in st.session_state:
    st.session_state.chart_visible = {}      # {msg_index: bool} — True = chart shown
if "img_url_cache" not in st.session_state:
    st.session_state.img_url_cache = {}      # {ad_id: fresh_url} — refreshed this session


# ── Meta image URL refresh ─────────────────────────────────────────────────────
def _get_meta_creds() -> tuple[str, str]:
    """Return (access_token, api_version) from .env."""
    from dotenv import load_dotenv
    from pathlib import Path
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    return os.getenv("META_ACCESS_TOKEN", ""), os.getenv("API_VERSION", "v19.0")


def _fresh_image_url(ad_id: str, stale_url: str = "") -> str:
    """
    Re-fetch a live image URL from Meta Graph API using ad_id.
    Cached in session_state so same ad is only fetched once per session.
    Falls back to stale_url if the API call fails.
    """
    if not ad_id or str(ad_id) in ("", "nan", "None"):
        return stale_url

    cache = st.session_state.img_url_cache
    if ad_id in cache:
        return cache[ad_id]

    try:
        token, version = _get_meta_creds()
        if not token:
            return stale_url

        resp = requests.get(
            f"https://graph.facebook.com/{version}/{ad_id}",
            params={"fields": "creative{image_url,thumbnail_url}", "access_token": token},
            timeout=6,
        )
        resp.raise_for_status()
        creative = resp.json().get("creative", {})
        url = creative.get("image_url") or creative.get("thumbnail_url") or stale_url
        cache[ad_id] = url
        return url
    except Exception:
        cache[ad_id] = stale_url
        return stale_url


def _refresh_image_urls_in_view(view_df: pd.DataFrame, progress_fn=None) -> pd.DataFrame:
    """
    For every unique ad_id in the creative_performance view,
    fetch a fresh image_url from Meta API and store it back.
    Returns the updated DataFrame.
    """
    if "ad_id" not in view_df.columns:
        return view_df

    token, version = _get_meta_creds()
    if not token:
        if progress_fn:
            progress_fn("Skipping image refresh — META_ACCESS_TOKEN not set.")
        return view_df

    unique_ads = (
        view_df[["ad_id"]]
        .dropna()
        .replace("", pd.NA)
        .dropna()
        ["ad_id"].unique().tolist()
    )
    if not unique_ads:
        return view_df

    if progress_fn:
        progress_fn(f"Refreshing image URLs for {len(unique_ads)} ads from Meta API…")

    # Batch fetch: 50 ad_ids per request
    fresh_map: dict[str, str] = {}
    batch_size = 50
    for i in range(0, len(unique_ads), batch_size):
        batch = unique_ads[i : i + batch_size]
        try:
            resp = requests.get(
                f"https://graph.facebook.com/{version}/",
                params={
                    "ids": ",".join(str(a) for a in batch),
                    "fields": "id,creative{image_url,thumbnail_url}",
                    "access_token": token,
                },
                timeout=15,
            )
            resp.raise_for_status()
            for ad_id, info in resp.json().items():
                creative = info.get("creative", {})
                url = creative.get("image_url") or creative.get("thumbnail_url") or ""
                fresh_map[str(ad_id)] = url
        except Exception as e:
            if progress_fn:
                progress_fn(f"  Warning: batch {i//batch_size + 1} failed — {e}")

    # Apply fresh URLs back to the DataFrame
    if fresh_map:
        view_df = view_df.copy()
        view_df["image_url"] = view_df["ad_id"].astype(str).map(fresh_map).fillna(
            view_df.get("image_url", "")
        )
        # Also update session cache
        st.session_state.img_url_cache.update(fresh_map)

    if progress_fn:
        progress_fn(f"  Image URLs refreshed for {len(fresh_map)} ads.")

    return view_df

# ── Query type badge ───────────────────────────────────────────────────────────
_QUERY_LABELS = {
    "creative_pc_days":  ("🎨 Creative × PC Days", "blue"),
    "pc_wise":           ("📍 PC-Wise Breakdown", "green"),
    "daily_consumption": ("📅 Daily PC Consumption", "orange"),
    "winners":           ("🏆 Winner Creatives", "red"),
    "pincode_count":     ("📌 Pincode Count", "violet"),
    "daily_pincode":     ("🗺️ Daily Pincode Breakdown", "blue"),
    "comparison":        ("⚖️ Period Comparison", "orange"),
    "overview":          ("📊 Performance Overview", "green"),
    "campaign_detail":   ("📢 Campaign Detail", "blue"),
    "generic":           ("🔍 General Query", "gray"),
}


@st.cache_data(ttl=300, show_spinner=False)
def _load_filter_df() -> pd.DataFrame:
    """Load Campaign / Ad set / Ad name columns for cascading sidebar filters."""
    try:
        df = load_sheet("creative_performance")
        cols = [c for c in ("Campaign name", "Ad set name", "Ad name") if c in df.columns]
        return df[cols].dropna(subset=cols[:1]).drop_duplicates() if cols else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _load_filter_options() -> tuple[list, list, list]:
    """Return flat lists (all options, no cascade) for initial population."""
    df = _load_filter_df()
    if df.empty:
        return [], [], []
    campaigns = sorted(df["Campaign name"].dropna().unique().tolist()) if "Campaign name" in df.columns else []
    adsets    = sorted(df["Ad set name"].dropna().unique().tolist())   if "Ad set name"   in df.columns else []
    ads       = sorted(df["Ad name"].dropna().unique().tolist())       if "Ad name"       in df.columns else []
    return campaigns, adsets, ads


def _cascade_options(filter_df: pd.DataFrame) -> tuple[list, list, list, list, list]:
    """
    Return (all_campaigns, available_adsets, all_adsets, available_ads, all_ads)
    based on currently selected campaigns/adsets in session_state.
    """
    all_campaigns = sorted(filter_df["Campaign name"].dropna().unique().tolist()) \
        if "Campaign name" in filter_df.columns else []
    all_adsets    = sorted(filter_df["Ad set name"].dropna().unique().tolist()) \
        if "Ad set name" in filter_df.columns else []
    all_ads       = sorted(filter_df["Ad name"].dropna().unique().tolist()) \
        if "Ad name" in filter_df.columns else []

    sel_camps  = st.session_state.get("sel_campaigns", [])
    sel_adsets = st.session_state.get("sel_adsets", [])

    # Cascade: if campaigns selected → narrow adsets and ads
    scoped = filter_df.copy()
    if sel_camps and "Campaign name" in scoped.columns:
        scoped = scoped[scoped["Campaign name"].isin(sel_camps)]

    available_adsets = sorted(scoped["Ad set name"].dropna().unique().tolist()) \
        if "Ad set name" in scoped.columns else all_adsets

    # Cascade: if adsets also selected → narrow ads further
    if sel_adsets and "Ad set name" in scoped.columns:
        scoped = scoped[scoped["Ad set name"].isin(sel_adsets)]

    available_ads = sorted(scoped["Ad name"].dropna().unique().tolist()) \
        if "Ad name" in scoped.columns else all_ads

    return all_campaigns, available_adsets, all_adsets, available_ads, all_ads

def _badge(query_type: str) -> str:
    label, _ = _QUERY_LABELS.get(query_type, ("🔍 Query", "gray"))
    return f"**{label}**"


def _show_table(df: pd.DataFrame, key_prefix: str) -> None:
    """
    Render a dataframe with a column selector above it.
    If an 'image_url' column is present:
      - It is shown as an inline thumbnail column in the table
      - A gallery section below shows each Ad name with a larger image preview
    Column order follows the order the user selected in the multiselect.
    CTR and CVR are displayed as percentages (multiplied by 100).
    """
    has_images = "image_url" in df.columns and df["image_url"].astype(str).str.startswith("http").any()

    all_cols = list(df.columns)
    # Default: hide image_url from column selector (shown in gallery instead)
    default_cols = [c for c in all_cols if c != "image_url"] if has_images else all_cols

    # Preserve the previously selected order; if no prior selection use default_cols order
    _prev_selected = st.session_state.get(f"cols_{key_prefix}", default_cols)
    # Keep only cols that still exist (in case df changed)
    _prev_valid = [c for c in _prev_selected if c in all_cols]
    # Add any new cols not yet in the selection (appended at end)
    _prev_valid += [c for c in default_cols if c not in _prev_valid]

    selected = st.multiselect(
        "Columns to display",
        options=all_cols,
        default=_prev_valid,
        key=f"cols_{key_prefix}",
    )
    # Honour the exact order the user chose in the multiselect
    view = df[selected] if selected else df

    # Build column config
    col_config = {}
    if "image_url" in view.columns:
        col_config["image_url"] = st.column_config.ImageColumn(
            "Ad Preview",
            help="Thumbnail preview of the ad creative",
            width="medium",
        )

    # Percentage metrics: stored as decimals, display as ×100 with % sign
    view = view.copy()
    for _pct_col in PCT_METRICS:
        if _pct_col in view.columns:
            view[_pct_col] = pd.to_numeric(view[_pct_col], errors="coerce") * 100
            col_config[_pct_col] = st.column_config.NumberColumn(
                f"{_pct_col} (%)",
                format="%.2f%%",
            )

    st.dataframe(view, use_container_width=True, column_config=col_config or None)

    # ── Ad Image Gallery ───────────────────────────────────────────────────────
    if has_images:
        # Build unique Ad name → {image_url, ad_id} mapping
        img_cols = [c for c in ("Ad name", "image_url", "ad_id") if c in df.columns]
        img_rows = (
            df[img_cols]
            .replace("", pd.NA)
            .dropna(subset=["image_url"])
            .drop_duplicates("Ad name")
        ) if "Ad name" in df.columns else pd.DataFrame()

        if not img_rows.empty:
            with st.expander("🖼️ Ad Image Gallery (click to expand)", expanded=False):
                st.caption("Images fetched fresh from Meta during last Refresh Views. Click to open full size.")
                cols_per_row = 3
                chunks = [
                    img_rows.iloc[i : i + cols_per_row]
                    for i in range(0, len(img_rows), cols_per_row)
                ]
                for chunk in chunks:
                    grid = st.columns(cols_per_row)
                    for col_idx, (_, r) in enumerate(chunk.iterrows()):
                        stale_url = str(r.get("image_url", ""))
                        ad_id     = str(r.get("ad_id", ""))
                        name      = str(r.get("Ad name", ""))
                        # Refresh URL if expired
                        url = _fresh_image_url(ad_id, stale_url)
                        if url and url.startswith("http"):
                            with grid[col_idx]:
                                st.image(url, caption=name, use_container_width=True)


def _show_chart(fig, msg_idx: int) -> None:
    """
    Render a Plotly chart with a minimize/expand toggle button.
    State is stored per message index so each chart is independent.
    """
    if fig is None:
        return

    # Default to HIDDEN on first render
    visible = st.session_state.chart_visible.get(msg_idx, False)

    btn_col, _ = st.columns([1, 6])
    with btn_col:
        label = "📉 Hide Chart" if visible else "📈 Show Chart"
        if st.button(label, key=f"chart_toggle_{msg_idx}", use_container_width=True):
            # Toggle and immediately re-render
            st.session_state.chart_visible[msg_idx] = not visible
            st.rerun()

    if visible:
        st.plotly_chart(fig, use_container_width=True, key=f"fig_{msg_idx}")


def _inject_sidebar_filters(intent: dict) -> None:
    """
    Merge sidebar multiselect selections into the intent filters.
    Sidebar selections OVERRIDE anything the query parser extracted,
    so the user always gets exactly the scope they chose.
    """
    f = intent.setdefault("filters", {})
    sel_camps  = st.session_state.get("sel_campaigns", [])
    sel_adsets = st.session_state.get("sel_adsets", [])
    sel_ads    = st.session_state.get("sel_ads", [])

    if sel_camps:
        f["campaigns"] = sel_camps          # list → OR match in aggregator
    if sel_adsets:
        f["adsets"] = sel_adsets
    if sel_ads:
        # Treat selected ads as a creative filter (list)
        f["ads_list"] = sel_ads


with _tab_chat:
    # ── Render chat history ───────────────────────────────────────────────────
    for _i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg.get("content", ""))
            if msg.get("badge"):
                st.markdown(msg["badge"])
            if msg.get("figure"):
                _show_chart(msg["figure"], msg_idx=_i)
            if msg.get("table_df") is not None:
                _show_table(msg["table_df"], key_prefix=f"hist_{_i}")
            if msg.get("xl_bytes"):
                _hc1, _hc2 = st.columns(2)
                with _hc1:
                    st.download_button(
                        label="⬇️ Download Excel (with images)",
                        data=msg["xl_bytes"],
                        file_name=msg.get("xl_filename", "result.xlsx"),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_hist_xl_{_i}",
                    )
                with _hc2:
                    if msg.get("csv_bytes"):
                        st.download_button(
                            label="⬇️ Download CSV",
                            data=msg["csv_bytes"],
                            file_name=msg.get("csv_filename", "result.csv"),
                            mime="text/csv",
                            key=f"dl_hist_csv_{_i}",
                        )
            elif msg.get("csv_bytes"):
                st.download_button(
                    label="⬇️ Download full result as CSV",
                    data=msg["csv_bytes"],
                    file_name=msg.get("csv_filename", "result.csv"),
                    mime="text/csv",
                    key=f"dl_hist_{_i}",
                )
            if msg.get("json_output"):
                with st.expander("View JSON output", expanded=False):
                    st.json(msg["json_output"])

    # ── Chat input ────────────────────────────────────────────────────────────
    prompt = st.chat_input("e.g. Creative wise PC days for last 30 days")
    if st.session_state.rerun_prompt:
        prompt = st.session_state.rerun_prompt
        st.session_state.rerun_prompt = None

    if prompt:
        hist = st.session_state.question_history
        if prompt in hist:
            hist.remove(prompt)
        hist.insert(0, prompt)
        st.session_state.question_history = hist[:20]

        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Analysing…"):
                try:
                    today_str = date.today().isoformat()

                    intent = parse_query(prompt, today_str)
                    query_type = intent.get("query_type", "generic")
                    badge = _badge(query_type)

                    _inject_sidebar_filters(intent)

                    dataset_key = intent.get("dataset", "creative_performance")
                    df = load_sheet(dataset_key)

                    if df.empty:
                        st.warning(f"The sheet tab **{dataset_key}** returned no data. "
                                   "Check your Sheet ID and tab names in config.py.")
                        st.stop()

                    result = compute(df, intent)

                    plain     = build_plain_text(result, prompt)
                    json_out  = build_json_output(result, prompt)
                    csv_bytes = build_csv_bytes(result)
                    fig       = build_chart(result.get("table"), intent, result)
                    table_df  = result.get("table")
                    ts        = str(int(date.today().toordinal()))
                    filename  = f"bct_{query_type}_{today_str}"

                    has_img_col = isinstance(table_df, pd.DataFrame) and "image_url" in table_df.columns
                    if has_img_col and isinstance(table_df, pd.DataFrame):
                        fresh_result = dict(result)
                        fresh_df = table_df.copy()
                        if "ad_id" in fresh_df.columns:
                            fresh_df["image_url"] = [
                                _fresh_image_url(str(r.get("ad_id", "")), str(r.get("image_url", "")))
                                for _, r in fresh_df.iterrows()
                            ]
                        fresh_result["table"] = fresh_df
                        xl_bytes = build_excel_bytes(fresh_result)
                    else:
                        xl_bytes = None

                    st.markdown(badge)
                    st.markdown(plain)

                    new_msg_idx = len(st.session_state.messages)
                    if fig:
                        _show_chart(fig, msg_idx=new_msg_idx)

                    if isinstance(table_df, pd.DataFrame) and not table_df.empty:
                        _show_table(table_df, key_prefix=f"new_{len(st.session_state.messages)}")

                    _dl_key_base = f"dl_new_{len(st.session_state.messages)}"
                    if xl_bytes:
                        dl_col1, dl_col2 = st.columns(2)
                        with dl_col1:
                            st.download_button(
                                label="⬇️ Download Excel (with images)",
                                data=xl_bytes,
                                file_name=f"{filename}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key=f"{_dl_key_base}_xl",
                            )
                        with dl_col2:
                            if csv_bytes:
                                st.download_button(
                                    label="⬇️ Download CSV",
                                    data=csv_bytes,
                                    file_name=f"{filename}.csv",
                                    mime="text/csv",
                                    key=f"{_dl_key_base}_csv",
                                )
                    elif csv_bytes:
                        st.download_button(
                            label="⬇️ Download full result as CSV",
                            data=csv_bytes,
                            file_name=f"{filename}.csv",
                            mime="text/csv",
                            key=_dl_key_base,
                        )

                    with st.expander("View JSON output"):
                        st.json(json_out)

                    st.session_state.messages.append({
                        "role":        "assistant",
                        "content":     plain,
                        "badge":       badge,
                        "figure":      fig,
                        "table_df":    table_df,
                        "json_output": json_out,
                        "csv_bytes":   csv_bytes,
                        "csv_filename": f"{filename}.csv",
                        "xl_bytes":    xl_bytes,
                        "xl_filename": f"{filename}.xlsx",
                        "ts":          ts,
                    })

                except Exception as e:
                    import traceback
                    err = f"**Error:** {e}"
                    st.error(err)
                    with st.expander("Full traceback"):
                        st.code(traceback.format_exc())
                    st.session_state.messages.append({"role": "assistant", "content": err})

# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — VIEW BUILDER
# ════════════════════════════════════════════════════════════════════════════════
with _tab_builder:
    st.subheader("🔨 Custom View Builder")
    st.caption("Pick dimensions, metrics and filters → build any view from Raw Dump → download as formatted Excel.")

    # ── Load saved configs ────────────────────────────────────────────────────
    _saved = load_saved_configs()

    _col_saved, _col_new = st.columns([1, 2])
    with _col_saved:
        st.markdown("**📂 Saved Views**")
        if _saved:
            _chosen_saved = st.selectbox(
                "Load a saved view",
                options=["— select —"] + list(_saved.keys()),
                key="vb_saved_select",
            )
            if _chosen_saved != "— select —":
                if st.button("📂 Load", key="vb_load_btn", use_container_width=True):
                    st.session_state["vb_loaded"] = _saved[_chosen_saved]
                    st.rerun()
                if st.button("🗑️ Delete", key="vb_delete_btn", use_container_width=True):
                    delete_config(_chosen_saved)
                    st.success(f"Deleted '{_chosen_saved}'")
                    st.rerun()
        else:
            st.caption("No saved views yet.")

    # Restore loaded config into widget state
    _loaded_cfg = st.session_state.pop("vb_loaded", None)

    # If a saved config was just loaded, push its values into session state.
    # This must happen BEFORE the widgets render so they pick up the new values.
    if _loaded_cfg:
        st.session_state["vb_dimensions"]       = _loaded_cfg.get("dimensions", ["Campaign name", "Ad name"])
        st.session_state["vb_metrics"]          = _loaded_cfg.get("metrics", DEFAULT_METRICS)
        st.session_state["vb_flt_camps"]        = _loaded_cfg.get("filter_campaigns", [])
        st.session_state["vb_flt_adsets"]       = _loaded_cfg.get("filter_adsets", [])
        st.session_state["vb_flt_ads"]          = _loaded_cfg.get("filter_ads", [])
        _df_str = _loaded_cfg.get("date_from")
        _dt_str = _loaded_cfg.get("date_to")
        st.session_state["vb_date_from"]        = pd.Timestamp(_df_str).date() if _df_str else None
        st.session_state["vb_date_to"]          = pd.Timestamp(_dt_str).date() if _dt_str else None
        st.session_state["vb_cpt_enabled"]      = bool(_loaded_cfg.get("cpt_min") or _loaded_cfg.get("cpt_max"))
        st.session_state["vb_ctr_enabled"]      = bool(_loaded_cfg.get("ctr_min") or _loaded_cfg.get("ctr_max"))
        st.session_state["vb_rev_enabled"]      = bool(_loaded_cfg.get("revenue_min") or _loaded_cfg.get("revenue_max"))
        st.session_state["vb_cpt_min"]          = float(_loaded_cfg["cpt_min"]) if _loaded_cfg.get("cpt_min") else 0.0
        st.session_state["vb_cpt_max"]          = float(_loaded_cfg["cpt_max"]) if _loaded_cfg.get("cpt_max") else 0.0
        st.session_state["vb_ctr_min"]          = float(_loaded_cfg["ctr_min"]) if _loaded_cfg.get("ctr_min") else 0.0
        st.session_state["vb_ctr_max"]          = float(_loaded_cfg["ctr_max"]) if _loaded_cfg.get("ctr_max") else 0.0
        st.session_state["vb_rev_min"]          = float(_loaded_cfg["revenue_min"]) if _loaded_cfg.get("revenue_min") else 0.0
        st.session_state["vb_rev_max"]          = float(_loaded_cfg["revenue_max"]) if _loaded_cfg.get("revenue_max") else 0.0
        st.session_state["vb_sheets_tab_input"] = _loaded_cfg.get("sheets_tab", "")

    # Initialize defaults only on first render (setdefault never overwrites existing state)
    st.session_state.setdefault("vb_dimensions", ["Campaign name", "Ad name"])
    st.session_state.setdefault("vb_metrics", DEFAULT_METRICS)
    st.session_state.setdefault("vb_flt_camps", [])
    st.session_state.setdefault("vb_flt_adsets", [])
    st.session_state.setdefault("vb_flt_ads", [])
    st.session_state.setdefault("vb_date_from", None)
    st.session_state.setdefault("vb_date_to", None)
    st.session_state.setdefault("vb_cpt_enabled", False)
    st.session_state.setdefault("vb_ctr_enabled", False)
    st.session_state.setdefault("vb_rev_enabled", False)
    st.session_state.setdefault("vb_cpt_min", 0.0)
    st.session_state.setdefault("vb_cpt_max", 0.0)
    st.session_state.setdefault("vb_ctr_min", 0.0)
    st.session_state.setdefault("vb_ctr_max", 0.0)
    st.session_state.setdefault("vb_rev_min", 0.0)
    st.session_state.setdefault("vb_rev_max", 0.0)
    st.session_state.setdefault("vb_sheets_tab_input", "")

    st.divider()

    # ── Dimension & Metric selectors ──────────────────────────────────────────
    _d_col, _m_col = st.columns(2)

    with _d_col:
        st.markdown("**📐 Dimensions** *(group by)*")
        _sel_dims = st.multiselect(
            "Select dimensions",
            options=ALL_DIMENSIONS,
            key="vb_dimensions",
            help="Rows in your result table. Order matters — first = outermost group.",
        )

    with _m_col:
        st.markdown("**📊 Metrics** *(columns)*")
        _sel_mets = st.multiselect(
            "Select metrics",
            options=ALL_METRICS,
            key="vb_metrics",
            help="Additive metrics are summed; ratio metrics (CTR, CPT, ROAS…) are recomputed.",
        )

    # ── Date range ────────────────────────────────────────────────────────────
    st.markdown("**📅 Date Range**")
    _r1, _r2, _r3 = st.columns(3)
    with _r1:
        _date_from = st.date_input("From", key="vb_date_from")
    with _r2:
        _date_to = st.date_input("To", key="vb_date_to")
    with _r3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("✖ Clear dates", key="vb_clear_dates"):
            st.session_state.vb_date_from = None
            st.session_state.vb_date_to   = None
            st.rerun()

    # ── Entity filters ────────────────────────────────────────────────────────
    st.markdown("**🔍 Filters** *(optional — narrows the Raw Dump before grouping)*")
    _f1, _f2, _f3 = st.columns(3)

    # Populate filter options from creative_performance view (faster than raw dump)
    try:
        _flt_df = _load_filter_df()
        _camp_opts  = sorted(_flt_df["Campaign name"].dropna().unique()) if "Campaign name" in _flt_df.columns else []
        _adset_opts = sorted(_flt_df["Ad set name"].dropna().unique())   if "Ad set name"   in _flt_df.columns else []
        _ad_opts    = sorted(_flt_df["Ad name"].dropna().unique())       if "Ad name"       in _flt_df.columns else []
    except Exception:
        _camp_opts = _adset_opts = _ad_opts = []

    with _f1:
        _flt_camps = st.multiselect("Campaigns", options=_camp_opts, key="vb_flt_camps")
    with _f2:
        _flt_adsets = st.multiselect("Ad Sets", options=_adset_opts, key="vb_flt_adsets")
    with _f3:
        _flt_ads = st.multiselect("Ad Names", options=_ad_opts, key="vb_flt_ads")

    st.divider()

    # ── Metric Threshold Filters ──────────────────────────────────────────────
    st.markdown("**🎯 Metric Filters** *(tick a checkbox to enable that filter)*")

    # Helper to safely read saved float values (reads from session state, not _loaded_cfg)
    def _saved_float(key):
        return float(st.session_state.get(f"vb_{key}", 0.0) or 0.0)

    _mf1, _mf2, _mf3 = st.columns(3)

    # ── CPT filter ────────────────────────────────────────────────────────────
    with _mf1:
        _cpt_enabled = st.checkbox("Filter by CPT (₹)", key="vb_cpt_enabled")
        if _cpt_enabled:
            _cc1, _cc2 = st.columns(2)
            with _cc1:
                _cpt_min_val = st.number_input("Min ₹", min_value=0.0, step=10.0,
                                               key="vb_cpt_min", format="%.2f")
            with _cc2:
                _cpt_max_val = st.number_input("Max ₹", min_value=0.0, step=10.0,
                                               key="vb_cpt_max", format="%.2f")
            _cpt_min_val = _cpt_min_val if _cpt_min_val > 0 else None
            _cpt_max_val = _cpt_max_val if _cpt_max_val > 0 else None
        else:
            _cpt_min_val = _cpt_max_val = None

    # ── CTR filter ────────────────────────────────────────────────────────────
    with _mf2:
        _ctr_enabled = st.checkbox("Filter by CTR (%)", key="vb_ctr_enabled")
        if _ctr_enabled:
            _tc1, _tc2 = st.columns(2)
            with _tc1:
                _ctr_min_val = st.number_input("Min %", min_value=0.0, step=0.1,
                                               key="vb_ctr_min", format="%.2f")
            with _tc2:
                _ctr_max_val = st.number_input("Max %", min_value=0.0, step=0.1,
                                               key="vb_ctr_max", format="%.2f")
            _ctr_min_val = _ctr_min_val if _ctr_min_val > 0 else None
            _ctr_max_val = _ctr_max_val if _ctr_max_val > 0 else None
        else:
            _ctr_min_val = _ctr_max_val = None

    # ── Revenue filter ────────────────────────────────────────────────────────
    with _mf3:
        _rev_enabled = st.checkbox("Filter by Revenue (₹)", key="vb_rev_enabled")
        if _rev_enabled:
            _rc1, _rc2 = st.columns(2)
            with _rc1:
                _rev_min_val = st.number_input("Min ₹", min_value=0.0, step=100.0,
                                               key="vb_rev_min", format="%.2f")
            with _rc2:
                _rev_max_val = st.number_input("Max ₹", min_value=0.0, step=100.0,
                                               key="vb_rev_max", format="%.2f")
            _rev_min_val = _rev_min_val if _rev_min_val > 0 else None
            _rev_max_val = _rev_max_val if _rev_max_val > 0 else None
        else:
            _rev_min_val = _rev_max_val = None

    st.divider()

    # ── Build button ──────────────────────────────────────────────────────────
    _b1, _b2 = st.columns([1, 4])
    with _b1:
        _build_clicked = st.button("▶ Build View", type="primary",
                                   use_container_width=True, key="vb_build")

    if _build_clicked:
        if not _sel_dims:
            st.warning("Select at least one dimension.")
        elif not _sel_mets:
            st.warning("Select at least one metric.")
        else:
            with st.spinner("Loading Raw Dump and building view…"):
                try:
                    _raw = load_sheet("raw_dump")
                    _df_from = _date_from.isoformat() if _date_from else None
                    _df_to   = _date_to.isoformat()   if _date_to   else None
                    _result_df = build_custom_view(
                        raw_df=_raw,
                        dimensions=_sel_dims,
                        metrics=_sel_mets,
                        date_from=_df_from,
                        date_to=_df_to,
                        filter_campaigns=_flt_camps or None,
                        filter_adsets=_flt_adsets or None,
                        filter_ads=_flt_ads or None,
                        cpt_min=_cpt_min_val,
                        cpt_max=_cpt_max_val,
                        ctr_min=_ctr_min_val,
                        ctr_max=_ctr_max_val,
                        revenue_min=_rev_min_val,
                        revenue_max=_rev_max_val,
                    )
                    st.session_state["vb_result"] = _result_df
                    st.session_state["vb_result_dims"]  = _sel_dims
                    st.session_state["vb_result_mets"]  = _sel_mets
                    st.session_state["vb_result_dfrom"] = _df_from
                    st.session_state["vb_result_dto"]   = _df_to
                except Exception as _e:
                    import traceback as _tb
                    st.error(f"Build failed: {_e}")
                    with st.expander("Traceback"):
                        st.code(_tb.format_exc())

    # ── Display result ────────────────────────────────────────────────────────
    _res = st.session_state.get("vb_result")
    if isinstance(_res, pd.DataFrame) and not _res.empty:
        st.success(f"View built — **{len(_res):,} rows** × **{len(_res.columns)} columns**")

        _show_table(_res, key_prefix="vb_result")

        # ── Download / Sheets buttons ─────────────────────────────────────────
        _dl1, _dl2, _dl3, _dl4 = st.columns([1, 1, 1, 2])

        # CSV
        _csv_buf = io.StringIO()
        _res.to_csv(_csv_buf, index=False)
        _csv_bytes = _csv_buf.getvalue().encode()

        with _dl1:
            st.download_button(
                "⬇️ Download CSV",
                data=_csv_bytes,
                file_name=f"bct_custom_view_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
                key="vb_dl_csv",
                use_container_width=True,
            )

        # Excel (formatted + images)
        with _dl2:
            with st.spinner("Preparing Excel…"):
                _xl_bytes = build_formatted_excel(_res, view_name="Custom View")
            st.download_button(
                "⬇️ Download Excel",
                data=_xl_bytes,
                file_name=f"bct_custom_view_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="vb_dl_xl",
                use_container_width=True,
            )

        # Save to Google Sheets
        with _dl3:
            _sheets_tab_name = st.session_state.get("vb_sheets_tab", "")
            if st.button("☁️ Save to Sheets", key="vb_save_sheets", use_container_width=True):
                _tab_input = st.session_state.get("vb_sheets_tab_input", "").strip()
                if not _tab_input:
                    st.warning("Enter a tab name below first.")
                else:
                    with st.spinner(f"Writing to Google Sheets tab '{_tab_input}'…"):
                        _ok = write_custom_view_to_sheets(_res, _tab_input)
                    if _ok:
                        st.success(f"Saved to tab '{_tab_input}'!")
                    else:
                        st.error("Failed to write to Sheets. Check logs.")

        # Tab name input (shown below buttons)
        st.text_input(
            "Google Sheets tab name (for ☁️ Save to Sheets)",
            placeholder="e.g. Campaign × Creative – Apr 2025",
            key="vb_sheets_tab_input",
        )

        # ── Save view config ──────────────────────────────────────────────────
        st.divider()
        st.markdown("**💾 Save this view configuration**")
        _s1, _s2 = st.columns([2, 1])
        with _s1:
            _save_name = st.text_input("View name", placeholder="e.g. Campaign + Creative last 30 days",
                                       key="vb_save_name")
        with _s2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("💾 Save", key="vb_save_btn", use_container_width=True):
                if not _save_name.strip():
                    st.warning("Enter a name first.")
                else:
                    save_config(_save_name.strip(), {
                        "dimensions":       st.session_state.get("vb_dimensions", []),
                        "metrics":          st.session_state.get("vb_metrics", []),
                        "date_from":        st.session_state.get("vb_result_dfrom"),
                        "date_to":          st.session_state.get("vb_result_dto"),
                        "filter_campaigns": st.session_state.get("vb_flt_camps", []),
                        "filter_adsets":    st.session_state.get("vb_flt_adsets", []),
                        "filter_ads":       st.session_state.get("vb_flt_ads", []),
                        "sheets_tab":       st.session_state.get("vb_sheets_tab_input", "").strip(),
                        "cpt_min":          (_cpt_min_val if st.session_state.get("vb_cpt_enabled") else None),
                        "cpt_max":          (_cpt_max_val if st.session_state.get("vb_cpt_enabled") else None),
                        "ctr_min":          (_ctr_min_val if st.session_state.get("vb_ctr_enabled") else None),
                        "ctr_max":          (_ctr_max_val if st.session_state.get("vb_ctr_enabled") else None),
                        "revenue_min":      (_rev_min_val if st.session_state.get("vb_rev_enabled") else None),
                        "revenue_max":      (_rev_max_val if st.session_state.get("vb_rev_enabled") else None),
                    })
                    st.success(f"Saved '{_save_name.strip()}'!")
                    st.rerun()

    elif _build_clicked and isinstance(_res, pd.DataFrame) and _res.empty:
        st.warning("No data matched your filters. Try widening the date range or removing filters.")


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("⚙️ Controls")

    # ── Context Filters (auto-scope all queries) ───────────────────────────────
    st.subheader("🔍 Context Filters")
    st.caption("Selections cascade: picking a Campaign narrows Ad Sets and Ads.")

    _filter_df = _load_filter_df()

    if _filter_df.empty:
        st.caption("_(Load data to populate filters)_")
        st.session_state.setdefault("sel_campaigns", [])
        st.session_state.setdefault("sel_adsets", [])
        st.session_state.setdefault("sel_ads", [])
    else:
        _all_camps, _avail_adsets, _all_adsets, _avail_ads, _all_ads = _cascade_options(_filter_df)
        _all_camps_list = sorted(_filter_df["Campaign name"].dropna().unique().tolist()) \
            if "Campaign name" in _filter_df.columns else []

        # Campaigns — always shows all
        st.multiselect(
            "Campaigns",
            options=_all_camps_list,
            key="sel_campaigns",
            placeholder="All campaigns",
        )

        # Ad Sets — narrows based on selected campaigns
        # Remove stale selections that are no longer in the available options
        _cur_adsets = [a for a in st.session_state.get("sel_adsets", []) if a in _avail_adsets]
        if _cur_adsets != st.session_state.get("sel_adsets", []):
            st.session_state.sel_adsets = _cur_adsets

        st.multiselect(
            "Ad Sets",
            options=_avail_adsets,
            key="sel_adsets",
            placeholder="All ad sets" if not st.session_state.get("sel_campaigns") else "Filtered by campaign",
        )

        # Ad Names — narrows based on selected campaigns + adsets
        _cur_ads = [a for a in st.session_state.get("sel_ads", []) if a in _avail_ads]
        if _cur_ads != st.session_state.get("sel_ads", []):
            st.session_state.sel_ads = _cur_ads

        st.multiselect(
            "Ad Names",
            options=_avail_ads,
            key="sel_ads",
            placeholder="All ads" if not st.session_state.get("sel_adsets") else "Filtered by ad set",
        )

    # Show active filter summary
    _active = []
    if st.session_state.get("sel_campaigns"):
        _active.append(f"{len(st.session_state.sel_campaigns)} campaign(s)")
    if st.session_state.get("sel_adsets"):
        _active.append(f"{len(st.session_state.sel_adsets)} adset(s)")
    if st.session_state.get("sel_ads"):
        _active.append(f"{len(st.session_state.sel_ads)} ad(s)")
    if _active:
        st.success(f"Active filters: {', '.join(_active)}")
    else:
        st.caption("No filters active — querying all data.")

    if st.button("🗑️ Clear filters", use_container_width=True):
        st.session_state.sel_campaigns = []
        st.session_state.sel_adsets    = []
        st.session_state.sel_ads       = []
        st.rerun()

    st.divider()

    # ── Fetch Current Results (run full Meta pipeline) ─────────────────────────
    st.subheader("📥 Fetch Current Results")
    st.caption("Runs the Meta Ads pipeline to pull fresh data from the API, combines it, and uploads to Raw Dump.")
    if st.button("📥 Fetch Current Results", type="primary", use_container_width=True):
        fetch_box = st.empty()
        try:
            import os
            pipeline_dir = os.path.normpath(
                os.path.join(os.path.dirname(__file__), "..", "meta_ads_raw_dump")
            )
            run_all_script = os.path.join(pipeline_dir, "run_all.py")
            fetch_box.info("Running Meta Ads pipeline… this may take a few minutes.")
            result = subprocess.run(
                [sys.executable, run_all_script],
                cwd=pipeline_dir,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                load_sheet.clear()
                fetch_box.success("Pipeline completed! Raw Dump updated in Google Sheets.")
            else:
                fetch_box.error("Pipeline failed.")
                with st.expander("Pipeline output"):
                    st.code(result.stdout[-3000:] + "\n" + result.stderr[-2000:])
        except Exception as e:
            import traceback
            fetch_box.error(f"Error: {e}")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

    st.divider()

    # ── Refresh Data ───────────────────────────────────────────────────────────
    st.subheader("🔄 Refresh Views")
    st.caption("Pulls latest Raw Dump, rebuilds all 4 views, pushes back to Sheets.")
    if st.button("🔄 Refresh Views", type="secondary", use_container_width=True):
        progress_box = st.empty()
        try:
            progress_box.info("Step 1/3 — Loading Raw Dump from Google Sheets…")
            raw_df = load_sheet("raw_dump")

            if raw_df.empty:
                progress_box.error("Raw Dump sheet is empty. Nothing to refresh.")
            else:
                progress_box.info(f"Step 2/4 — Building views from {len(raw_df):,} rows…")
                views = build_all_views(raw_df)

                # Step 3: Refresh image URLs in creative_performance from Meta API
                if "creative_performance" in views:
                    def _img_prog(msg):
                        progress_box.info(f"Step 3/4 — {msg}")
                    views["creative_performance"] = _refresh_image_urls_in_view(
                        views["creative_performance"], progress_fn=_img_prog
                    )

                log_lines = []
                for k, v in views.items():
                    log_lines.append(f"  • {k}: {len(v)} rows")
                progress_box.info("Step 4/4 — Writing views to Google Sheets…\n" + "\n".join(log_lines))

                def _prog(msg):
                    progress_box.info(f"Step 4/4 — {msg}")

                results = write_all_views(views, progress_callback=_prog)

                # Clear cached sheet data so next query reads fresh views
                load_sheet.clear()

                failed = [k for k, ok in results.items() if not ok]
                if failed:
                    progress_box.warning(f"Refresh done with errors on: {', '.join(failed)}")
                else:
                    row_summary = " | ".join(f"{k}: {len(v)}" for k, v in views.items())
                    progress_box.success(f"All views refreshed successfully!\n{row_summary}")

                # Step 5: Refresh saved custom views to their Google Sheets tabs
                _saved_cfgs = load_saved_configs()
                _custom_tabs_with_sheets = {
                    name: cfg for name, cfg in _saved_cfgs.items()
                    if cfg.get("sheets_tab")
                }
                if _custom_tabs_with_sheets:
                    _cv_errors = []
                    _cv_total  = len(_custom_tabs_with_sheets)
                    for _cv_idx, (_cv_name, _cv_cfg) in enumerate(_custom_tabs_with_sheets.items(), 1):
                        progress_box.info(
                            f"Step 5/5 — Custom view {_cv_idx}/{_cv_total}: '{_cv_name}'…"
                        )
                        try:
                            _cv_df = build_custom_view(
                                raw_df=raw_df,
                                dimensions=_cv_cfg.get("dimensions", []),
                                metrics=_cv_cfg.get("metrics", []),
                                date_from=_cv_cfg.get("date_from"),
                                date_to=date.today().isoformat(),
                                filter_campaigns=_cv_cfg.get("filter_campaigns") or None,
                                filter_adsets=_cv_cfg.get("filter_adsets") or None,
                                filter_ads=_cv_cfg.get("filter_ads") or None,
                                cpt_min=_cv_cfg.get("cpt_min"),
                                cpt_max=_cv_cfg.get("cpt_max"),
                                ctr_min=_cv_cfg.get("ctr_min"),
                                ctr_max=_cv_cfg.get("ctr_max"),
                                revenue_min=_cv_cfg.get("revenue_min"),
                                revenue_max=_cv_cfg.get("revenue_max"),
                            )
                            _cv_tab = _cv_cfg["sheets_tab"]
                            write_custom_view_to_sheets(
                                _cv_df, _cv_tab,
                                column_renames=_cv_cfg.get("column_renames"),
                                extra_cols=_cv_cfg.get("extra_cols"),
                            )
                        except Exception as _cv_err:
                            _cv_errors.append(f"'{_cv_name}': {_cv_err}")

                    if _cv_errors:
                        progress_box.warning(
                            "Custom views done with errors:\n" + "\n".join(_cv_errors)
                        )
                    else:
                        progress_box.success(
                            f"All {_cv_total} custom view(s) refreshed successfully!"
                        )

        except Exception as e:
            import traceback
            progress_box.error(f"Refresh failed: {e}")
            with st.expander("Traceback"):
                st.code(traceback.format_exc())

    st.divider()

    if st.button("🗑️ Clear chat"):
        st.session_state.messages = []
        st.rerun()

    st.divider()
    st.subheader("🕘 Recent questions")
    if st.session_state.question_history:
        for _qi, _q in enumerate(st.session_state.question_history):
            if st.button(_q, key=f"rq_{_qi}_{hash(_q) % 99999}", use_container_width=True):
                st.session_state.rerun_prompt = _q
                st.rerun()
    else:
        st.caption("No questions asked yet.")

    st.divider()
    st.subheader("Query types supported")
    st.markdown("""
**🎨 Q1 — Creative × PC Days**
> "Creative wise performance for active PCs"

**📍 Q2 — PC Wise (with date)**
> "PC wise extraction by creative and date"

**📅 Q3 — Daily PC Consumption**
> "Daily kitne PCs consume hue by creative"

**🏆 Q4 — Winners**
> "Winner creatives with CPT<250 and purchases>2 for last 30 days"

**📌 Q5 — Pincode Count**
> "How many pincodes were used in last 7 days?"

**🗺️ Q6 — Daily Pincode Breakdown**
> "Which pincodes were active each day and their spend?"

**🔗 Q7 — Pincode × Creative Ranking**
> "Which pincodes are generating the highest purchases by creative?"

**📢 Q8 — Campaign Performance**
> "Campaign wise performance last 30 days"
""")

    st.divider()
    st.subheader("Available datasets")
    st.markdown("""
- `Creative_Performance_View` — Date × Creative
- `PC_Creative_Date_View` — Date × Pincode × Creative
- `Daily_PC_Consumption` — Daily totals
- `Winning_Creatives_View` — Creative lifetime
- `Pincode_Creative_View` — Pincode × Creative lifetime
- `Campaign_Performance_View` — Date × Campaign
- `Raw Dump` _(use: "raw data / raw dump")_
""")
    st.divider()
    st.subheader("Threshold filters")
    st.markdown("""
You can add conditions to any query:
- `CPT < 250`
- `Purchases > 2`
- `ROAS >= 1.5`

Example: _"Winner creatives with CPT<250 and purchases>2 for last 30 days"_
""")
