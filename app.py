import gc
import io
from datetime import date as _date

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

pd.set_option("styler.render.max_elements", 10_000_000)


def direction_to_sign(direction: str) -> int:
    """Convert Direction string to +1 or -1 for quantity calculation."""
    d = str(direction).strip().lower()
    if d == "in":
        return 1
    if d in ("out", "within"):
        return -1
    # REVISIT: unknown direction — returns 0, Net Qty = 0 for this row; surfaced as warning in UI
    return 0


_TX_TYPE_SIGN = {
    "transfer out": -1,
    "adjustment": -1,
    "sales": -1,
    "customer return": 1,
    "non returnable goods issue": 1,
    "receiving": 1,
    "vendor return": -1,
}


def transaction_sign(tx_type: str, direction: str) -> int:
    """Determine sign from Transaction Type; fall back to Direction if type is unknown."""
    t = str(tx_type).strip().lower()
    if t in _TX_TYPE_SIGN:
        return _TX_TYPE_SIGN[t]
    return direction_to_sign(direction)


def _read_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Read xlsx or csv based on file extension."""
    if filename.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(file_bytes))
    return pd.read_excel(io.BytesIO(file_bytes), engine="calamine")


@st.cache_data
def parse_starting_inventory(file_bytes: bytes, filename: str) -> pd.DataFrame:
    df = _read_file(file_bytes, filename)[["SKU", "LocCode", "Total Qty"]]
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["LocCode"] = df["LocCode"].astype(str).str.strip().str.upper()
    df = df.rename(columns={"Total Qty": "Starting Qty"})
    df["Starting Qty"] = pd.to_numeric(df["Starting Qty"], errors="coerce").fillna(0)
    return df.groupby(["SKU", "LocCode"], as_index=False)["Starting Qty"].sum()


@st.cache_data
def parse_ending_inventory(file_bytes: bytes, filename: str) -> pd.DataFrame:
    df = _read_file(file_bytes, filename)[["SKU", "LocCode", "Total Qty"]]
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["LocCode"] = df["LocCode"].astype(str).str.strip().str.upper()
    df = df.rename(columns={"Total Qty": "Actual Ending Qty"})
    df["Actual Ending Qty"] = pd.to_numeric(df["Actual Ending Qty"], errors="coerce").fillna(0)
    return df.groupby(["SKU", "LocCode"], as_index=False)["Actual Ending Qty"].sum()


@st.cache_data
def parse_price(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse price file with SKU + price columns (column name detected case-insensitively)."""
    df = _read_file(file_bytes, filename)
    col_map = {c.strip().lower(): c for c in df.columns}
    sku_col = col_map.get("sku")
    price_col = next(
        (col_map[k] for k in col_map
         if k in ("price", "unit price", "unitprice", "cost", "last_purchase_price", "last purchase price")),
        None,
    )
    if sku_col is None:
        raise KeyError("Price file must contain a 'SKU' column.")
    if price_col is None:
        raise KeyError(f"Price file must contain a price column (e.g. 'Price'). Found: {list(df.columns)}")
    df = df[[sku_col, price_col]].rename(columns={sku_col: "SKU", price_col: "Price"})
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0)
    return df.drop_duplicates(subset="SKU", keep="first").rename(columns={"Price": "Unit Price"})


@st.cache_data
def parse_transactions(files_bytes: tuple, filenames: tuple) -> pd.DataFrame:
    """files_bytes: tuple of bytes; filenames: tuple of original file names."""
    if not files_bytes:
        return pd.DataFrame(columns=["SKU", "LocCode", "Quantity", "Direction",
                                      "Transaction Type", "Transaction Date", "Reference No",
                                      "Sign", "Net Qty"])
    dfs = []
    for b, name in zip(files_bytes, filenames):
        raw = _read_file(b, name)
        # DECIDED: case-insensitive column lookup — source files use "locCode" but some exports use "LocCode"
        col_lower = {c.lower(): c for c in raw.columns}
        required = {"loccode": "LocCode", "sku": "SKU", "quantity": "Quantity",
                    "direction": "Direction", "transaction type": "Transaction Type",
                    "transaction date": "Transaction Date", "reference no": "Reference No"}
        missing = [std for key, std in required.items() if key not in col_lower]
        if missing:
            raise KeyError(f"'{name}' is missing columns: {missing}. Found: {list(raw.columns)}")
        rename_map = {col_lower[k]: std for k, std in required.items()}
        df = raw[[col_lower[k] for k in required]].rename(columns=rename_map)
        dfs.append(df)
    tx = pd.concat(dfs, ignore_index=True)
    tx = tx.rename(columns={"locCode": "LocCode"})
    tx["SKU"] = tx["SKU"].astype(str).str.strip().str.upper()
    tx["LocCode"] = tx["LocCode"].astype(str).str.strip().str.upper()
    tx["Quantity"] = pd.to_numeric(tx["Quantity"], errors="coerce").fillna(0)
    tx["Sign"] = tx.apply(lambda r: transaction_sign(r["Transaction Type"], r["Direction"]), axis=1)
    tx["Net Qty"] = tx["Quantity"] * tx["Sign"]
    tx["Transaction Date"] = pd.to_datetime(tx["Transaction Date"], dayfirst=True, errors="coerce")
    return tx


def reconcile(
    starting_df: pd.DataFrame,
    ending_df: pd.DataFrame,
    tx_df: pd.DataFrame,
) -> pd.DataFrame:
    starting_df = starting_df.copy()
    ending_df = ending_df.copy()
    tx_df = tx_df.copy()
    for df in [starting_df, ending_df, tx_df]:
        if "SKU" in df.columns:
            df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
        if "LocCode" in df.columns:
            df["LocCode"] = df["LocCode"].astype(str).str.strip().str.upper()

    if "Net Qty" in tx_df.columns:
        net_tx = (
            tx_df.groupby(["SKU", "LocCode"], as_index=False)["Net Qty"]
            .sum()
            .rename(columns={"Net Qty": "Net Transactions"})
        )
    else:
        net_tx = pd.DataFrame(columns=["SKU", "LocCode", "Net Transactions"])

    all_keys = pd.concat([
        starting_df[["SKU", "LocCode"]],
        ending_df[["SKU", "LocCode"]],
        net_tx[["SKU", "LocCode"]],
    ]).drop_duplicates()

    df = all_keys.merge(starting_df, on=["SKU", "LocCode"], how="left")
    df = df.merge(ending_df, on=["SKU", "LocCode"], how="left")
    df = df.merge(net_tx, on=["SKU", "LocCode"], how="left")

    df["Starting Qty"] = pd.to_numeric(df["Starting Qty"], errors="coerce").fillna(0)
    df["Net Transactions"] = pd.to_numeric(df["Net Transactions"], errors="coerce").fillna(0)
    df["Actual Ending Qty"] = pd.to_numeric(df["Actual Ending Qty"], errors="coerce").fillna(0)
    df["Expected Ending Qty"] = df["Starting Qty"] + df["Net Transactions"]
    df["Variance"] = df["Expected Ending Qty"] - df["Actual Ending Qty"]

    def classify(row):
        if row["Variance"] == 0:
            return "Matched"
        return "Discrepancy"

    df["Status"] = df.apply(classify, axis=1)

    if "Transaction Type" in tx_df.columns and "Net Qty" in tx_df.columns:
        pivot = (
            tx_df.groupby(["SKU", "LocCode", "Transaction Type"])["Net Qty"]
            .sum()
            .unstack(fill_value=0)
            .reset_index()
        )
        pivot.columns.name = None
        pivot = pivot.rename(
            columns={c: f"TX: {c}" for c in pivot.columns if c not in ["SKU", "LocCode"]}
        )
        df = df.merge(pivot, on=["SKU", "LocCode"], how="left")
        for col in [c for c in df.columns if c.startswith("TX: ")]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


# ─────────────────────────────────────────────────────────────────────────────
# UI — CSS
# ─────────────────────────────────────────────────────────────────────────────

def _css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    *, html, body, [class*="css"], [data-testid] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
        box-sizing: border-box;
    }

    /* ── Hide Streamlit chrome ── */
    [data-testid="stSidebar"], [data-testid="collapsedControl"],
    button[kind="header"], .stDeployButton { display: none !important; }
    #MainMenu, footer, header { visibility: hidden; }

    /* ── Page shell ── */
    .stApp { background: #06091A !important; }
    .block-container {
        padding: 1.75rem 2.5rem 4rem !important;
        max-width: 1440px !important;
    }

    /* ── Text defaults ── */
    p, span, div, label { color: #94A3B8; }

    /* ── Text inputs ── */
    .stTextInput input, .stDateInput input {
        background: #0C1425 !important;
        border: 1px solid rgba(255,255,255,0.07) !important;
        color: #F1F5F9 !important;
        border-radius: 10px !important;
        padding: 0.5rem 0.85rem !important;
    }
    .stTextInput input:focus, .stDateInput input:focus {
        border-color: rgba(99,102,241,0.5) !important;
        box-shadow: 0 0 0 3px rgba(99,102,241,0.08) !important;
        outline: none !important;
    }
    .stTextInput label, .stDateInput label {
        color: #374151 !important;
        font-size: 0.7rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
    }

    /* ── Select / Multiselect ── */
    [data-baseweb="select"] > div {
        background: #0C1425 !important;
        border-color: rgba(255,255,255,0.07) !important;
        border-radius: 10px !important;
        min-height: 42px !important;
        flex-wrap: wrap !important;
    }
    [data-baseweb="select"] span { color: #CBD5E1 !important; }
    [data-baseweb="select"] input { color: #F1F5F9 !important; min-width: 60px !important; }

    /* ── Dropdown menu — full width, no clipping ── */
    [data-baseweb="menu"] {
        background: #111827 !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 10px !important;
        max-height: 280px !important;
        overflow-y: auto !important;
    }
    [data-baseweb="option"] {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        font-size: 0.83rem !important;
        color: #94A3B8 !important;
        padding: 8px 14px !important;
    }
    [data-baseweb="option"]:hover { background: rgba(99,102,241,0.12) !important; }

    /* ── Selected tags — no overflow ── */
    [data-baseweb="tag"] {
        background: rgba(99,102,241,0.13) !important;
        border-color: rgba(99,102,241,0.28) !important;
        border-radius: 6px !important;
        max-width: 140px !important;
        overflow: hidden !important;
    }
    [data-baseweb="tag"] span {
        color: #818CF8 !important;
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        display: block !important;
        max-width: 110px !important;
    }

    /* ── Selectbox single value ── */
    [data-baseweb="select"] [data-testid="stMarkdownContainer"] p,
    [data-baseweb="single-value"] {
        white-space: nowrap !important;
        overflow: hidden !important;
        text-overflow: ellipsis !important;
        color: #F1F5F9 !important;
    }

    .stSelectbox label, .stMultiSelect label {
        color: #374151 !important;
        font-size: 0.7rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.5px !important;
    }

    /* ── File uploader ── */
    [data-testid="stFileUploader"] {
        background: rgba(12,20,37,0.7) !important;
        border: 1.5px dashed rgba(255,255,255,0.07) !important;
        border-radius: 14px !important;
        transition: border-color 0.2s, background 0.2s;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: rgba(99,102,241,0.4) !important;
        background: rgba(99,102,241,0.03) !important;
    }
    [data-testid="stFileUploaderDropzone"] { background: transparent !important; }
    [data-testid="stFileUploaderDropzone"] p,
    [data-testid="stFileUploaderDropzone"] span,
    [data-testid="stFileUploaderDropzoneInstructions"] span {
        color: #1E293B !important;
        font-size: 0.78rem !important;
    }
    [data-testid="stFileUploaderLabel"] { color: #374151 !important; font-size: 0.7rem !important; }

    /* ── Primary button ── */
    div.stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #4F46E5 0%, #7C3AED 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        font-size: 0.95rem !important;
        font-weight: 700 !important;
        padding: 0.8rem 2rem !important;
        box-shadow: 0 4px 20px rgba(99,102,241,0.3) !important;
        transition: all 0.2s !important;
        width: 100% !important;
    }
    div.stButton > button[kind="primary"]:hover {
        box-shadow: 0 8px 32px rgba(99,102,241,0.5) !important;
        transform: translateY(-1px) !important;
    }

    /* ── Secondary button ── */
    div.stButton > button:not([kind="primary"]) {
        background: rgba(255,255,255,0.04) !important;
        color: #64748B !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        border-radius: 10px !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        transition: all 0.2s !important;
    }
    div.stButton > button:not([kind="primary"]):hover {
        background: rgba(255,255,255,0.07) !important;
        color: #F1F5F9 !important;
        border-color: rgba(255,255,255,0.14) !important;
    }

    /* ── Download button ── */
    div.stDownloadButton > button {
        background: rgba(99,102,241,0.07) !important;
        color: #818CF8 !important;
        border: 1px solid rgba(99,102,241,0.22) !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        font-size: 0.85rem !important;
        transition: all 0.2s !important;
    }
    div.stDownloadButton > button:hover {
        background: rgba(99,102,241,0.14) !important;
        border-color: rgba(99,102,241,0.42) !important;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(255,255,255,0.02);
        border-radius: 12px;
        padding: 4px;
        border: 1px solid rgba(255,255,255,0.06);
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 9px;
        color: #475569 !important;
        font-weight: 500;
        font-size: 0.83rem;
        padding: 8px 16px;
        border: none !important;
        background: transparent !important;
        transition: all 0.2s;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #4F46E5, #7C3AED) !important;
        color: white !important;
        font-weight: 700 !important;
        box-shadow: 0 2px 10px rgba(99,102,241,0.35) !important;
    }

    /* ── Dataframe ── */
    [data-testid="stDataFrame"] {
        border-radius: 12px !important;
        overflow: hidden !important;
        border: 1px solid rgba(255,255,255,0.06) !important;
    }

    /* ── Expander ── */
    [data-testid="stExpander"] {
        background: rgba(255,255,255,0.015) !important;
        border: 1px solid rgba(255,255,255,0.06) !important;
        border-radius: 12px !important;
        overflow: visible !important;
    }
    [data-testid="stExpander"] summary {
        color: #64748B !important;
        font-size: 0.84rem !important;
        font-weight: 600 !important;
        padding: 0.75rem 1rem !important;
        letter-spacing: 0.2px !important;
    }
    [data-testid="stExpander"] summary:hover { color: #94A3B8 !important; }
    [data-testid="stExpander"] > div[data-testid="stExpanderDetails"] {
        padding: 0.25rem 0.75rem 0.75rem !important;
    }

    /* ── Dropdown popover — must float above everything ── */
    [data-baseweb="popover"], [data-baseweb="tooltip"] {
        z-index: 999999 !important;
    }
    [data-baseweb="menu"] { z-index: 999999 !important; }

    /* ── Alerts / Divider / Spinner ── */
    .stAlert { border-radius: 10px !important; }
    [data-testid="stSpinner"] p { color: #374151 !important; }
    [data-testid="stDivider"] hr {
        border-color: rgba(255,255,255,0.06) !important;
        margin: 0.5rem 0 1.25rem !important;
    }

    /* ── Utility classes ── */
    .row-count {
        font-size: 0.72rem;
        color: #1E293B;
        font-weight: 600;
        margin-bottom: 0.5rem;
        letter-spacing: 0.3px;
    }
    </style>
    """, unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# UI — COMPONENTS
# ─────────────────────────────────────────────────────────────────────────────

def _topbar(phase: int):
    chips = ""
    for i, lbl in enumerate(["Upload Files", "Run", "Results"], 1):
        if i < phase:
            chips += (
                f'<span style="display:inline-flex;align-items:center;gap:.3rem;'
                f'padding:.25rem .8rem;border-radius:999px;font-size:.68rem;font-weight:600;'
                f'background:rgba(16,185,129,0.1);color:#10B981;border:1px solid rgba(16,185,129,0.2);">'
                f'&#10003;&nbsp;{lbl}</span>'
            )
        elif i == phase:
            chips += (
                f'<span style="display:inline-flex;align-items:center;gap:.3rem;'
                f'padding:.25rem .8rem;border-radius:999px;font-size:.68rem;font-weight:700;'
                f'background:rgba(99,102,241,0.14);color:#818CF8;border:1px solid rgba(99,102,241,0.3);">'
                f'{i}.&nbsp;{lbl}</span>'
            )
        else:
            chips += (
                f'<span style="display:inline-flex;align-items:center;gap:.3rem;'
                f'padding:.25rem .8rem;border-radius:999px;font-size:.68rem;font-weight:500;'
                f'background:rgba(255,255,255,0.03);color:#1E293B;border:1px solid rgba(255,255,255,0.06);">'
                f'{i}.&nbsp;{lbl}</span>'
            )
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;
                padding:0 0 1rem;border-bottom:1px solid rgba(255,255,255,0.05);
                margin-bottom:1.75rem;">
        <div style="display:flex;align-items:center;gap:.75rem;">
            <div style="width:36px;height:36px;border-radius:10px;flex-shrink:0;
                        background:linear-gradient(135deg,#4F46E5,#7C3AED);
                        display:flex;align-items:center;justify-content:center;font-size:1.1rem;">
                &#128230;
            </div>
            <div>
                <div style="font-size:1.05rem;font-weight:800;color:#F1F5F9;line-height:1.1;">RECO</div>
                <div style="font-size:.56rem;color:#1E293B;letter-spacing:1.5px;
                            text-transform:uppercase;font-weight:600;">Inventory Reconciliation</div>
            </div>
        </div>
        <div style="display:flex;align-items:center;gap:.45rem;">{chips}</div>
    </div>
    """, unsafe_allow_html=True)


def _kpi_card(col, label, value, sub, accent, glow):
    sub_html = (
        f'<div style="font-size:.7rem;color:#1E293B;margin-top:.3rem;">{sub}</div>'
        if sub else ""
    )
    with col:
        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.02);
                    border:1px solid rgba(255,255,255,0.06);
                    border-top:2px solid {accent};
                    border-radius:14px;
                    padding:1.4rem 1.5rem;
                    box-shadow:0 4px 24px {glow};
                    position:relative;
                    overflow:hidden;">
            <div style="position:absolute;top:-20px;right:-20px;width:80px;height:80px;
                        border-radius:50%;background:{glow};pointer-events:none;opacity:.6;"></div>
            <div style="font-size:.58rem;text-transform:uppercase;letter-spacing:1.8px;
                        color:#1E293B;font-weight:700;">{label}</div>
            <div style="font-size:2.2rem;font-weight:800;line-height:1.2;margin-top:.4rem;
                        color:#F1F5F9;">{value}</div>
            {sub_html}
        </div>
        """, unsafe_allow_html=True)


def _kpi_cards(total, matched, pct_matched, discrepancies):
    cols = st.columns(3)
    _kpi_card(cols[0], "Total SKU \u00b7 Loc Pairs", f"{total:,}",          "",                          "#6366F1", "rgba(99,102,241,0.12)")
    _kpi_card(cols[1], "Match Rate",                 f"{pct_matched}%",     f"{matched:,} matched rows", "#10B981", "rgba(16,185,129,0.12)")
    _kpi_card(cols[2], "Discrepancies",              f"{discrepancies:,}",  "rows with variance \u2260 0","#EF4444", "rgba(239,68,68,0.12)")


def _price_kpi_cards(total_inv_value, total_var_value, skus_priced, total):
    pct_priced = round(skus_priced / total * 100, 1) if total > 0 else 0.0
    cols = st.columns(3)
    _kpi_card(cols[0], "Total Inventory Value", f"{total_inv_value:,.0f}",  "",                                "#6366F1", "rgba(99,102,241,0.12)")
    _kpi_card(cols[1], "Total Variance Value",  f"{total_var_value:,.0f}",  "absolute sum",                    "#EF4444", "rgba(239,68,68,0.12)")
    _kpi_card(cols[2], "SKU+Loc Rows Priced",   f"{skus_priced:,}",         f"{pct_priced}% of {total:,} rows","#10B981", "rgba(16,185,129,0.12)")


def _status_donut(df):
    counts = df["Status"].value_counts().reset_index()
    counts.columns = ["Status", "Count"]
    color_map = {"Matched": "#10B981", "Discrepancy": "#EF4444"}
    colors = [color_map.get(s, "#334155") for s in counts["Status"]]
    total = counts["Count"].sum()
    matched_pct = round(df[df["Status"] == "Matched"].shape[0] / total * 100, 1) if total else 0

    fig = go.Figure(go.Pie(
        labels=counts["Status"], values=counts["Count"], hole=0.65,
        marker=dict(colors=colors, line=dict(color="#06091A", width=3)),
        textinfo="percent", textfont_size=11, textfont_color="white",
        hovertemplate="<b>%{label}</b><br>%{value:,} records (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        annotations=[dict(
            text=f"<b>{matched_pct}%</b><br><span style='font-size:11px;color:#475569'>Matched</span>",
            x=0.5, y=0.5, font_size=16, showarrow=False, font_color="#F1F5F9",
        )],
        legend=dict(orientation="v", yanchor="middle", y=0.5, x=1.02,
                    font_size=12, font_color="#64748B"),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=5, b=5, l=5, r=5), height=300, showlegend=True,
    )
    return fig


def _variance_histogram(df):
    disc = df[df["Variance"] != 0][["Variance"]].copy()
    if disc.empty:
        return None
    fig = px.histogram(disc, x="Variance", nbins=min(60, len(disc)),
                       color_discrete_sequence=["#6366F1"])
    fig.update_traces(
        marker_line_color="#4F46E5", marker_line_width=0.5,
        hovertemplate="Variance: %{x}<br>Count: %{y}<extra></extra>",
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=300, margin=dict(t=5, b=40, l=40, r=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Variance (Expected \u2212 Actual)",
                   linecolor="rgba(255,255,255,0.07)", color="#475569"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)", title="Count",
                   linecolor="rgba(255,255,255,0.07)", color="#475569"),
        showlegend=False, bargap=0.05, font=dict(color="#475569"),
    )
    return fig


def _tx_type_chart(df):
    tx_cols = [c for c in df.columns if c.startswith("TX: ")]
    if not tx_cols:
        return None
    totals = df[tx_cols].sum().reset_index()
    totals.columns = ["Type", "Net Qty"]
    totals["Type"] = totals["Type"].str.replace("TX: ", "", regex=False)
    totals = totals.sort_values("Net Qty")
    totals["Color"] = totals["Net Qty"].apply(lambda v: "#EF4444" if v < 0 else "#10B981")
    fig = go.Figure(go.Bar(
        x=totals["Net Qty"], y=totals["Type"], orientation="h",
        marker_color=totals["Color"], marker_line_width=0,
        hovertemplate="<b>%{y}</b><br>Net Qty: %{x:,}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=300, margin=dict(t=5, b=10, l=10, r=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)", linecolor="rgba(255,255,255,0.07)",
                   title="Net Quantity", color="#475569"),
        yaxis=dict(gridcolor="rgba(0,0,0,0)", color="#475569"),
        showlegend=False, font=dict(color="#475569"),
    )
    return fig


def _style_table(df):
    status_colors = {
        "Matched":     "color:#10B981;font-weight:600",
        "Discrepancy": "color:#EF4444;font-weight:600",
    }

    def color_status(val):
        return status_colors.get(val, "")

    def color_numeric(v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return ""
        if v < 0:
            return "color:#EF4444;font-weight:600"
        if v > 0:
            return "color:#10B981;font-weight:600"
        return ""

    styler = df.style
    if "Status" in df.columns:
        styler = styler.map(color_status, subset=["Status"])
    for col in ("Variance", "Variance Value"):
        if col in df.columns:
            styler = styler.map(color_numeric, subset=[col])
    return styler


def _chart_wrap(title: str, fig):
    # DECIDED: label rendered above chart separately — Streamlit sanitises each st.markdown call
    # independently so open/close div across calls doesn't work; container gives correct grouping
    with st.container():
        st.markdown(
            f'<div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.6px;'
            f'color:#475569;font-weight:700;margin-bottom:.25rem;padding:.5rem .25rem 0;">'
            f'{title}</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(fig, use_container_width=True)


def _section(label: str):
    st.markdown(
        f'<div style="font-size:.6rem;text-transform:uppercase;letter-spacing:1.6px;'
        f'color:#1E293B;font-weight:700;margin:.75rem 0 .5rem;">{label}</div>',
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="RECO \u00b7 Inventory Reconciliation",
        layout="wide",
        page_icon="\U0001f4e6",
    )
    _css()

    # ── Phase detection ──────────────────────────────────────────────────────
    has_results = "recon_result" in st.session_state
    _stored_sig = st.session_state.get("_file_sig")
    _had_files = bool(_stored_sig and any(v is not None for v in (_stored_sig[0], _stored_sig[1])))
    phase = 3 if has_results else (2 if _had_files else 1)

    _topbar(phase)

    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 3 — RESULTS
    # ═══════════════════════════════════════════════════════════════════════
    if has_results:

        # ── Action bar (Re-run) ─────────────────────────────────────────
        _, rb = st.columns([5, 1])
        with rb:
            rerun_clicked = st.button("\u21ba\u2002Re-run", use_container_width=True)

        # ── File manager ────────────────────────────────────────────────
        with st.expander("\U0001f4c1\u2002Manage Files", expanded=False):
            e1, e2, e3, e4 = st.columns(4)
            with e1:
                starting_file = st.file_uploader("Starting Inventory", type=["xlsx", "csv"], key="starting")
            with e2:
                ending_file = st.file_uploader("Ending Inventory", type=["xlsx", "csv"], key="ending")
            with e3:
                tx_files = st.file_uploader("Transaction Files", type=["xlsx", "csv"],
                                            accept_multiple_files=True, key="transactions")
            with e4:
                price_file = st.file_uploader("Price File (optional)", type=["xlsx", "csv"], key="price")

        has_files = bool(starting_file and ending_file and tx_files)

        # File-signature change → auto-clear stale results
        # DECIDED: include file size so re-uploading a same-named different file is detected
        sig = (
            (starting_file.name, starting_file.size) if starting_file else None,
            (ending_file.name, ending_file.size) if ending_file else None,
            tuple(sorted((f.name, f.size) for f in tx_files)) if tx_files else (),
            (price_file.name, price_file.size) if price_file else None,
        )
        if st.session_state.get("_file_sig") != sig:
            for k in ("recon_result", "tx_data", "price_data", "_zero_sign"):
                st.session_state.pop(k, None)
            st.session_state["_file_sig"] = sig
            st.rerun()

        # ── Re-run handler ───────────────────────────────────────────────
        if rerun_clicked:
            if has_files:
                try:
                    with st.spinner("Re-running reconciliation..."):
                        s = parse_starting_inventory(starting_file.getvalue(), starting_file.name)
                        e = parse_ending_inventory(ending_file.getvalue(), ending_file.name)
                        t = parse_transactions(
                            tuple(f.getvalue() for f in tx_files),
                            tuple(f.name for f in tx_files),
                        )
                        zs = int((t["Sign"] == 0).sum()) if "Sign" in t.columns else 0
                        st.session_state["recon_result"] = reconcile(s, e, t)
                        st.session_state["tx_data"] = t
                        st.session_state["_zero_sign"] = zs
                        del s, e, t
                        gc.collect()
                        if price_file:
                            st.session_state["price_data"] = parse_price(
                                price_file.getvalue(), price_file.name
                            )
                        else:
                            st.session_state.pop("price_data", None)
                    st.rerun()
                except KeyError as ex:
                    st.error(f"Column not found: {ex}. Check that the uploaded file has the required columns.")
                except Exception as ex:
                    st.error(f"Failed to process files: {ex}")
            else:
                st.warning("Upload files first via 'Manage Files' above.")

        # Show zero-sign warning if any (stored from previous run)
        # DECIDED: stored in session_state because st.rerun() is called right after processing,
        # so a warning inside the spinner context would never render before the rerun
        zs_count = st.session_state.get("_zero_sign", 0)
        if zs_count > 0:
            st.warning(
                f"{zs_count:,} transaction row(s) have an unrecognised Transaction Type and Direction "
                f"\u2014 their quantity contributes 0 to reconciliation. "
                f"Check the TX Breakdown tab for affected SKUs."
            )

        recon = st.session_state["recon_result"]
        transactions = st.session_state["tx_data"]

        # ── Filters ─────────────────────────────────────────────────────
        _section("Filters")
        fc1, fc2 = st.columns([4, 2])
        with fc1:
            loc_options = sorted(recon["LocCode"].unique().tolist())
            selected_locs = st.multiselect("Location", loc_options, default=loc_options)
        with fc2:
            sku_filter = st.text_input("SKU Search").strip().upper()

        st.divider()

        if not selected_locs:
            st.warning("No locations selected \u2014 please select at least one location.")
            st.stop()

        # Apply filters
        filtered_recon = recon[recon["LocCode"].isin(selected_locs)].copy()
        if sku_filter:
            filtered_recon = filtered_recon[
                filtered_recon["SKU"].str.contains(sku_filter, na=False, regex=False)
            ]
        filtered_tx = transactions[transactions["LocCode"].isin(selected_locs)].copy()
        if sku_filter:
            filtered_tx = filtered_tx[
                filtered_tx["SKU"].str.contains(sku_filter, na=False, regex=False)
            ]

        tx_type_cols = [c for c in filtered_recon.columns if c.startswith("TX: ")]
        detail_cols = (
            ["SKU", "LocCode", "Starting Qty"]
            + tx_type_cols
            + ["Net Transactions", "Expected Ending Qty", "Actual Ending Qty", "Variance", "Status"]
        )

        # ── Tabs ─────────────────────────────────────────────────────────
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "\U0001f4ca\u2002Summary",
            "\U0001f4cb\u2002Full Detail",
            "\u26a0\ufe0f\u2002Discrepancies",
            "\U0001f50d\u2002TX Breakdown",
            "\U0001f4b0\u2002Price Analysis",
            "\U0001f3ed\u2002Warehouse Report",
        ])

        # ── Tab 1: Summary ───────────────────────────────────────────────
        with tab1:
            total   = len(filtered_recon)
            matched = int((filtered_recon["Status"] == "Matched").sum())
            disc    = int((filtered_recon["Status"] == "Discrepancy").sum())
            pct_m   = round(matched / total * 100, 1) if total > 0 else 0.0

            st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
            _kpi_cards(total, matched, pct_m, disc)
            st.markdown("<div style='height:.75rem'></div>", unsafe_allow_html=True)

            col1, col2 = st.columns(2)
            with col1:
                _chart_wrap("Status Breakdown", _status_donut(filtered_recon))
            with col2:
                hist = _variance_histogram(filtered_recon)
                if hist:
                    _chart_wrap("Variance Distribution (non-zero only)", hist)
                else:
                    st.markdown("""
                    <div style="background:rgba(16,185,129,0.07);border:1px solid rgba(16,185,129,0.18);
                                border-radius:14px;padding:2.5rem;text-align:center;margin-top:1.5rem;">
                        <div style="font-size:1.5rem;margin-bottom:.6rem;">&#9989;</div>
                        <div style="font-size:.95rem;font-weight:600;color:#10B981;">
                            Everything matches \u2014 no variances found!
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

            tx_bar = _tx_type_chart(filtered_recon)
            if tx_bar:
                st.markdown("<div style='height:.4rem'></div>", unsafe_allow_html=True)
                _chart_wrap("Net Quantity by Transaction Type", tx_bar)

        # ── Tab 2: Full Detail ───────────────────────────────────────────
        with tab2:
            tf1, tf2, tf3 = st.columns([2, 1, 5])
            with tf1:
                status_opts = ["All"] + sorted(filtered_recon["Status"].unique().tolist())
                status_filter = st.selectbox("Filter by Status", status_opts, key="detail_status")
            detail_df = (
                filtered_recon if status_filter == "All"
                else filtered_recon[filtered_recon["Status"] == status_filter]
            )
            with tf3:
                st.markdown(
                    f'<div style="padding-top:1.85rem;font-size:.72rem;color:#1E293B;font-weight:600;">'
                    f'{len(detail_df):,} rows</div>',
                    unsafe_allow_html=True,
                )
            st.dataframe(_style_table(detail_df[detail_cols]), use_container_width=True, height=560)

        # ── Tab 3: Discrepancies ─────────────────────────────────────────
        with tab3:
            disc_df = (
                filtered_recon[filtered_recon["Variance"] != 0]
                .copy()
                .assign(AbsVariance=lambda d: d["Variance"].abs())
                .sort_values("AbsVariance", ascending=False)
                .drop(columns=["AbsVariance"])
            )
            dc1, dc2 = st.columns([6, 2])
            with dc1:
                st.markdown(
                    f'<div class="row-count" style="color:#EF4444;">{len(disc_df):,} discrepant rows</div>',
                    unsafe_allow_html=True,
                )
            with dc2:
                st.download_button(
                    "\u2b07\ufe0f\u2002Download CSV",
                    data=disc_df[detail_cols].to_csv(index=False).encode("utf-8"),
                    file_name="discrepancies.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            if disc_df.empty:
                st.markdown("""
                <div style="background:rgba(16,185,129,0.07);border:1px solid rgba(16,185,129,0.18);
                            border-radius:14px;padding:2.5rem;text-align:center;margin-top:1rem;">
                    <div style="font-size:1.5rem;margin-bottom:.6rem;">&#9989;</div>
                    <div style="font-size:.95rem;font-weight:600;color:#10B981;">
                        No discrepancies in the selected filters!
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.dataframe(
                    _style_table(disc_df[detail_cols]), use_container_width=True, height=580
                )

        # ── Tab 4: TX Breakdown ──────────────────────────────────────────
        with tab4:
            key_options = sorted(
                (filtered_recon["SKU"] + " | " + filtered_recon["LocCode"]).tolist()
            )
            if not key_options:
                st.info("No data matches the current filters.")
            else:
                selected_key = st.selectbox("Select SKU + Location", key_options)
                sel_sku, sel_loc = [x.strip() for x in selected_key.split("|", maxsplit=1)]
                tx_detail = filtered_tx[
                    (filtered_tx["SKU"] == sel_sku) & (filtered_tx["LocCode"] == sel_loc)
                ].copy()
                if tx_detail.empty:
                    st.info(
                        f"No transactions found for **{sel_sku}** at **{sel_loc}** "
                        f"in the selected date range."
                    )
                else:
                    st.markdown(
                        f'<div class="row-count">{len(tx_detail)} transactions for '
                        f'<b style="color:#818CF8">{sel_sku}</b> at '
                        f'<b style="color:#818CF8">{sel_loc}</b></div>',
                        unsafe_allow_html=True,
                    )
                    st.dataframe(
                        tx_detail[["Transaction Date", "Transaction Type",
                                   "Quantity", "Direction", "Reference No"]]
                        .sort_values("Transaction Date"),
                        use_container_width=True,
                        height=500,
                    )

        # ── Tab 5: Price Analysis ────────────────────────────────────────
        with tab5:
            if "price_data" not in st.session_state:
                st.markdown("""
                <div style="background:rgba(255,255,255,0.02);
                            border:1px solid rgba(255,255,255,0.07);
                            border-radius:16px;padding:3rem;
                            max-width:480px;margin:2rem auto;text-align:center;">
                    <div style="font-size:2rem;margin-bottom:1rem;">&#128176;</div>
                    <div style="font-size:1rem;font-weight:600;color:#94A3B8;margin-bottom:.5rem;">
                        Price File Not Uploaded
                    </div>
                    <div style="font-size:.82rem;color:#1E293B;line-height:1.8;">
                        Open <b style="color:#475569">Manage Files</b> above,
                        upload a Price file with <b style="color:#475569">SKU</b>
                        and <b style="color:#475569">Price</b> columns, then Re-run.
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                price_df = st.session_state["price_data"]
                price_recon = filtered_recon.merge(price_df, on="SKU", how="left")
                no_price_mask = price_recon["Unit Price"].isna()
                price_recon["Unit Price"] = pd.to_numeric(
                    price_recon["Unit Price"], errors="coerce"
                ).fillna(0)
                price_recon["Starting Value"]        = price_recon["Starting Qty"]        * price_recon["Unit Price"]
                price_recon["Expected Ending Value"] = price_recon["Expected Ending Qty"] * price_recon["Unit Price"]
                price_recon["Actual Ending Value"]   = price_recon["Actual Ending Qty"]   * price_recon["Unit Price"]
                price_recon["Variance Value"]        = price_recon["Variance"]             * price_recon["Unit Price"]

                no_price = int(no_price_mask.sum())
                if no_price > 0:
                    st.warning(
                        f"{no_price} SKU+Location rows have no price match \u2014 value columns show 0."
                    )

                _price_kpi_cards(
                    price_recon["Actual Ending Value"].sum(),
                    price_recon["Variance Value"].abs().sum(),
                    int((~no_price_mask).sum()),
                    len(price_recon),
                )
                price_cols = [
                    "SKU", "LocCode", "Unit Price", "Starting Value",
                    "Expected Ending Value", "Actual Ending Value", "Variance Value", "Status",
                ]
                pc1, pc2 = st.columns([6, 2])
                with pc1:
                    st.markdown(
                        f'<div class="row-count">{len(price_recon):,} rows</div>',
                        unsafe_allow_html=True,
                    )
                with pc2:
                    st.download_button(
                        "\u2b07\ufe0f\u2002Download CSV",
                        data=price_recon[price_cols].to_csv(index=False).encode("utf-8"),
                        file_name="price_analysis.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )
                st.dataframe(
                    _style_table(price_recon[price_cols]), use_container_width=True, height=580
                )

        # ── Tab 6: Warehouse Report ──────────────────────────────────────
        with tab6:
            # Build per-warehouse summary from filtered_recon
            wh = (
                filtered_recon.groupby("LocCode")
                .agg(
                    SKUs=("SKU", "count"),
                    Matched=("Status", lambda s: (s == "Matched").sum()),
                    Discrepancies=("Status", lambda s: (s == "Discrepancy").sum()),
                    Starting_Qty=("Starting Qty", "sum"),
                    Net_Transactions=("Net Transactions", "sum"),
                    Expected_Ending=("Expected Ending Qty", "sum"),
                    Actual_Ending=("Actual Ending Qty", "sum"),
                    Total_Variance=("Variance", "sum"),
                    Abs_Variance=("Variance", lambda v: v.abs().sum()),
                )
                .reset_index()
            )
            wh["Match %"] = (wh["Matched"] / wh["SKUs"] * 100).round(1)
            wh = wh.rename(columns={
                "LocCode": "Warehouse",
                "SKUs": "SKU·Loc Pairs",
                "Starting_Qty": "Starting Qty",
                "Net_Transactions": "Net Transactions",
                "Expected_Ending": "Expected Ending",
                "Actual_Ending": "Actual Ending",
                "Total_Variance": "Net Variance",
                "Abs_Variance": "Abs Variance",
            })
            wh = wh.sort_values("Abs Variance", ascending=False).reset_index(drop=True)

            # ── KPI bar: top 3 worst warehouses ──────────────────────
            total_wh = len(wh)
            perfect = int((wh["Match %"] == 100).sum())
            worst_abs = wh.iloc[0]["Abs Variance"] if not wh.empty else 0
            worst_wh  = wh.iloc[0]["Warehouse"]    if not wh.empty else "—"

            kw1, kw2, kw3 = st.columns(3)
            _kpi_card(kw1, "Warehouses Analysed",  f"{total_wh}",         f"{perfect} fully matched", "#6366F1", "rgba(99,102,241,0.12)")
            _kpi_card(kw2, "Highest Abs Variance",  f"{worst_abs:,.0f}",  f"at {worst_wh}",            "#EF4444", "rgba(239,68,68,0.12)")
            _kpi_card(kw3, "Perfect Match Warehouses", f"{perfect}",      f"of {total_wh} total",      "#10B981", "rgba(16,185,129,0.12)")

            st.markdown("<div style='height:.75rem'></div>", unsafe_allow_html=True)

            # ── Charts ───────────────────────────────────────────────
            ch1, ch2 = st.columns(2)

            with ch1:
                # Match % by warehouse — horizontal bar, colour-coded
                fig_match = go.Figure(go.Bar(
                    x=wh["Match %"],
                    y=wh["Warehouse"],
                    orientation="h",
                    marker_color=[
                        "#10B981" if v == 100 else ("#F59E0B" if v >= 50 else "#EF4444")
                        for v in wh["Match %"]
                    ],
                    marker_line_width=0,
                    text=[f"{v}%" for v in wh["Match %"]],
                    textposition="outside",
                    textfont=dict(color="#64748B", size=11),
                    hovertemplate="<b>%{y}</b><br>Match Rate: %{x}%<extra></extra>",
                ))
                fig_match.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    height=max(280, 40 * len(wh)),
                    margin=dict(t=10, b=10, l=10, r=60),
                    xaxis=dict(range=[0, 115], gridcolor="rgba(255,255,255,0.05)",
                               linecolor="rgba(255,255,255,0.07)", color="#475569",
                               title="Match %"),
                    yaxis=dict(gridcolor="rgba(0,0,0,0)", color="#94A3B8", automargin=True),
                    showlegend=False, font=dict(color="#475569"),
                )
                _chart_wrap("Match Rate by Warehouse", fig_match)

            with ch2:
                # Abs variance by warehouse
                fig_var = go.Figure(go.Bar(
                    x=wh["Abs Variance"],
                    y=wh["Warehouse"],
                    orientation="h",
                    marker_color="#6366F1",
                    marker_line_width=0,
                    hovertemplate="<b>%{y}</b><br>Abs Variance: %{x:,}<extra></extra>",
                ))
                fig_var.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                    height=max(280, 40 * len(wh)),
                    margin=dict(t=10, b=10, l=10, r=10),
                    xaxis=dict(gridcolor="rgba(255,255,255,0.05)",
                               linecolor="rgba(255,255,255,0.07)", color="#475569",
                               title="Absolute Variance (units)"),
                    yaxis=dict(gridcolor="rgba(0,0,0,0)", color="#94A3B8", automargin=True),
                    showlegend=False, font=dict(color="#475569"),
                )
                _chart_wrap("Absolute Variance by Warehouse", fig_var)

            st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

            # ── Summary table ─────────────────────────────────────────
            display_cols = [
                "Warehouse", "SKU·Loc Pairs", "Matched", "Discrepancies", "Match %",
                "Starting Qty", "Net Transactions", "Expected Ending", "Actual Ending",
                "Net Variance", "Abs Variance",
            ]
            wh_dl, wh_rc = st.columns([2, 6])
            with wh_dl:
                st.download_button(
                    "\u2b07\ufe0f\u2002Download CSV",
                    data=wh[display_cols].to_csv(index=False).encode("utf-8"),
                    file_name="warehouse_report.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            def _style_wh(df):
                def color_match(v):
                    try:
                        v = float(v)
                    except (TypeError, ValueError):
                        return ""
                    if v == 100:
                        return "color:#10B981;font-weight:700"
                    if v >= 50:
                        return "color:#F59E0B;font-weight:600"
                    return "color:#EF4444;font-weight:600"

                def color_var(v):
                    try:
                        v = float(v)
                    except (TypeError, ValueError):
                        return ""
                    if v < 0:
                        return "color:#EF4444;font-weight:600"
                    if v > 0:
                        return "color:#F59E0B;font-weight:600"
                    return "color:#10B981;font-weight:600"

                s = df.style.map(color_match, subset=["Match %"])
                s = s.map(color_var, subset=["Net Variance"])
                return s

            st.dataframe(
                _style_wh(wh[display_cols]),
                use_container_width=True,
                height=min(600, 45 * (len(wh) + 2)),
            )

        return  # Phase 3 complete

    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 1 / 2 — UPLOAD
    # ═══════════════════════════════════════════════════════════════════════

    st.markdown("""
    <div style="text-align:center;padding:2rem 0 1.75rem;">
        <div style="font-size:1.85rem;font-weight:800;color:#F1F5F9;line-height:1.2;margin-bottom:.5rem;">
            Upload Your Files
        </div>
        <div style="font-size:.87rem;color:#2D3748;max-width:480px;margin:0 auto;line-height:1.6;">
            Upload starting inventory, ending inventory, and transaction files to run the reconciliation.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Upload grid ──────────────────────────────────────────────────────
    u1, u2, u3, u4 = st.columns(4)

    _slots = [
        (u1, "\u2460", "Starting Inventory", "SKU \u00b7 LocCode \u00b7 Total Qty", True,  "starting", False),
        (u2, "\u2461", "Ending Inventory",   "SKU \u00b7 LocCode \u00b7 Total Qty", True,  "ending",   False),
        (u3, "\u2462", "Transaction Files",  "Multiple files accepted",             True,  "transactions", True),
        (u4, "\u2463", "Price File",         "SKU \u00b7 Price",                   False, "price",     False),
    ]

    file_refs = {}
    for col, num, title, hint, req, key, multi in _slots:
        badge = (
            '<span style="font-size:.58rem;background:rgba(239,68,68,0.09);color:#F87171;'
            'border:1px solid rgba(239,68,68,0.18);border-radius:4px;'
            'padding:.08rem .38rem;margin-left:.4rem;">required</span>'
            if req else
            '<span style="font-size:.58rem;background:rgba(16,185,129,0.07);color:#34D399;'
            'border:1px solid rgba(16,185,129,0.15);border-radius:4px;'
            'padding:.08rem .38rem;margin-left:.4rem;">optional</span>'
        )
        with col:
            st.markdown(
                f'<div style="margin-bottom:.35rem;">'
                f'<span style="font-size:.68rem;text-transform:uppercase;'
                f'letter-spacing:1.2px;color:#374151;font-weight:700;">{num} {title}</span>'
                f'{badge}</div>'
                f'<div style="font-size:.65rem;color:#1E293B;margin-bottom:.35rem;">{hint}</div>',
                unsafe_allow_html=True,
            )
            file_refs[key] = st.file_uploader(
                title,
                type=["xlsx", "csv"],
                accept_multiple_files=multi,
                key=key,
                label_visibility="collapsed",
            )

    starting_file = file_refs["starting"]
    ending_file   = file_refs["ending"]
    tx_files      = file_refs["transactions"]
    price_file    = file_refs["price"]

    has_files = bool(starting_file and ending_file and tx_files)

    # File-signature tracking
    sig = (
        (starting_file.name, starting_file.size) if starting_file else None,
        (ending_file.name, ending_file.size) if ending_file else None,
        tuple(sorted((f.name, f.size) for f in tx_files)) if tx_files else (),
        (price_file.name, price_file.size) if price_file else None,
    )
    if st.session_state.get("_file_sig") != sig:
        for k in ("recon_result", "tx_data", "price_data", "_zero_sign"):
            st.session_state.pop(k, None)
        st.session_state["_file_sig"] = sig

    if not has_files:
        # Show which files are still needed
        missing = []
        if not starting_file:
            missing.append("Starting Inventory")
        if not ending_file:
            missing.append("Ending Inventory")
        if not tx_files:
            missing.append("Transaction Files")
        if missing:
            st.markdown(
                f'<div style="text-align:center;font-size:.78rem;color:#1E293B;margin-top:1.25rem;">'
                f'Waiting for: <b style="color:#374151">{" \u00b7 ".join(missing)}</b></div>',
                unsafe_allow_html=True,
            )
        st.stop()

    # ── Phase 2: Files ready card ────────────────────────────────────────
    st.markdown("<div style='height:.75rem'></div>", unsafe_allow_html=True)
    st.divider()

    fc1, fc2, fc3, fc4 = st.columns(4)
    for fcol, slot, name in [
        (fc1, "Starting",     starting_file.name),
        (fc2, "Ending",       ending_file.name),
        (fc3, "Transactions", f"{len(tx_files)} file(s)"),
        (fc4, "Price",        price_file.name if price_file else "\u2014"),
    ]:
        color = "#1E293B" if name == "\u2014" else "#10B981"
        border = "rgba(30,41,59,0.3)" if name == "\u2014" else "rgba(16,185,129,0.2)"
        bg = "rgba(30,41,59,0.15)" if name == "\u2014" else "rgba(16,185,129,0.06)"
        with fcol:
            st.markdown(
                f'<div style="background:{bg};border:1px solid {border};'
                f'border-radius:10px;padding:.65rem .9rem;text-align:center;">'
                f'<div style="font-size:.55rem;text-transform:uppercase;letter-spacing:1.2px;'
                f'color:#1E293B;font-weight:700;margin-bottom:.3rem;">{slot}</div>'
                f'<div style="font-size:.78rem;color:{color};font-weight:600;word-break:break-all;">{name}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("<div style='height:.75rem'></div>", unsafe_allow_html=True)

    # ── Run button ───────────────────────────────────────────────────────
    _, run_col, _ = st.columns([2, 3, 2])
    with run_col:
        run_clicked = st.button(
            "\u25b6\u2002 Run Reconciliation",
            type="primary",
            use_container_width=True,
        )

    if run_clicked:
        try:
            with st.spinner("Processing files and running reconciliation..."):
                starting = parse_starting_inventory(starting_file.getvalue(), starting_file.name)
                ending   = parse_ending_inventory(ending_file.getvalue(), ending_file.name)
                txs      = parse_transactions(
                    tuple(f.getvalue() for f in tx_files),
                    tuple(f.name for f in tx_files),
                )
                # DECIDED: store zero_sign in session_state — st.rerun() fires before any warning
                # rendered inside spinner would be visible to the user
                zs = int((txs["Sign"] == 0).sum()) if "Sign" in txs.columns else 0
                st.session_state["recon_result"] = reconcile(starting, ending, txs)
                st.session_state["tx_data"] = txs
                st.session_state["_zero_sign"] = zs
                del starting, ending, txs
                gc.collect()
                if price_file:
                    st.session_state["price_data"] = parse_price(
                        price_file.getvalue(), price_file.name
                    )
                else:
                    st.session_state.pop("price_data", None)
            st.rerun()
        except KeyError as ex:
            st.error(f"Column not found: {ex}. Check that the uploaded file has the required columns.")
        except Exception as ex:
            st.error(f"Failed to process files: {ex}")


if __name__ == "__main__":
    main()
