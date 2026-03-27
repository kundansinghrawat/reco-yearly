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
    return pd.read_excel(io.BytesIO(file_bytes))


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
    """Parse price file with SKU + Price columns."""
    df = _read_file(file_bytes, filename)[["SKU", "Price"]]
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["Price"] = pd.to_numeric(df["Price"], errors="coerce").fillna(0)
    return (
        df.drop_duplicates(subset="SKU", keep="first")
        .rename(columns={"Price": "Unit Price"})
    )


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
        df = raw[["locCode", "SKU", "Quantity", "Direction",
                  "Transaction Type", "Transaction Date", "Reference No"]]
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
    # Normalize keys (in case inputs come from tests with mixed case)
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
        if row["Starting Qty"] == 0 and row["Net Transactions"] == 0 and row["Actual Ending Qty"] > 0:
            return "New in Ending"
        if row["Actual Ending Qty"] == 0 and (row["Starting Qty"] > 0 or row["Net Transactions"] != 0):
            return "Missing in Ending"
        if row["Variance"] == 0:
            return "Matched"
        return "Discrepancy"

    df["Status"] = df.apply(classify, axis=1)

    # ── Transaction type bifurcation ─────────────────────────────────
    if "Transaction Type" in tx_df.columns and "Net Qty" in tx_df.columns:
        pivot = (
            tx_df.groupby(["SKU", "LocCode", "Transaction Type"])["Net Qty"]
            .sum()
            .unstack(fill_value=0)
            .reset_index()
        )
        pivot.columns.name = None
        pivot = pivot.rename(columns={c: f"TX: {c}" for c in pivot.columns if c not in ["SKU", "LocCode"]})
        df = df.merge(pivot, on=["SKU", "LocCode"], how="left")
        for col in [c for c in df.columns if c.startswith("TX: ")]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    return df


def _inject_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    * { font-family: 'Inter', -apple-system, sans-serif !important; }
    #MainMenu, footer, header { visibility: hidden; }
    .stDeployButton { display: none !important; }
    .stApp { background: #F0F4FF !important; }
    .block-container { padding: 1.5rem 2rem 2rem !important; max-width: 1440px !important; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] { background: linear-gradient(180deg,#0D1B2A 0%,#1B2A4A 100%) !important; }
    [data-testid="stSidebar"] section { background: transparent !important; }
    [data-testid="stSidebar"] label { color: #94A3B8 !important; font-size: 0.78rem !important; font-weight: 500 !important; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span { color: #CBD5E1 !important; }
    [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
        color: #F8FAFC !important; font-size: 0.68rem !important;
        text-transform: uppercase; letter-spacing: 1.6px; font-weight: 700;
    }
    [data-testid="stSidebar"] hr { border-color: rgba(255,255,255,0.08) !important; margin: 0.75rem 0 !important; }
    [data-testid="stSidebar"] [data-testid="stFileUploader"] {
        background: rgba(255,255,255,0.05) !important;
        border: 1px dashed rgba(255,255,255,0.15) !important;
        border-radius: 12px !important; padding: 0.25rem !important;
    }
    [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] { background: transparent !important; }
    [data-testid="stSidebar"] .stTextInput input {
        background: rgba(255,255,255,0.07) !important;
        border-color: rgba(255,255,255,0.12) !important;
        color: white !important; border-radius: 8px !important;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div {
        background: rgba(255,255,255,0.07) !important;
        border-color: rgba(255,255,255,0.12) !important;
        border-radius: 8px !important;
    }

    /* ── Primary button ── */
    div.stButton > button[kind="primary"] {
        background: linear-gradient(135deg,#2563EB 0%,#7C3AED 100%) !important;
        color: white !important; border: none !important; border-radius: 12px !important;
        font-size: 1rem !important; font-weight: 700 !important;
        padding: 0.75rem 2rem !important;
        box-shadow: 0 8px 24px rgba(37,99,235,0.4) !important;
        transition: all 0.3s ease !important; letter-spacing: 0.3px !important;
    }
    div.stButton > button[kind="primary"]:hover {
        box-shadow: 0 12px 32px rgba(37,99,235,0.55) !important;
        transform: translateY(-2px) !important;
    }

    /* ── Tabs ── */
    .stTabs [data-baseweb="tab-list"] {
        background: white; border-radius: 16px; padding: 6px;
        box-shadow: 0 2px 16px rgba(0,0,0,0.08); gap: 4px; border: none;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px; color: #64748B; font-weight: 500;
        font-size: 0.9rem; padding: 10px 24px;
        border: none !important; background: transparent !important; transition: all 0.2s;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg,#2563EB,#7C3AED) !important;
        color: white !important; font-weight: 700 !important;
        box-shadow: 0 4px 14px rgba(37,99,235,0.35) !important;
    }

    /* ── Download button ── */
    div.stDownloadButton > button {
        background: white !important; color: #2563EB !important;
        border: 2px solid #2563EB !important; border-radius: 10px !important;
        font-weight: 600 !important; transition: all 0.2s !important;
    }
    div.stDownloadButton > button:hover { background: #EFF6FF !important; }

    /* ── Dataframe ── */
    [data-testid="stDataFrame"] {
        border-radius: 14px !important; overflow: hidden !important;
        box-shadow: 0 2px 16px rgba(0,0,0,0.07) !important;
    }

    /* ── Chart cards ── */
    .chart-card {
        background: white; border-radius: 16px; padding: 1.25rem 1.5rem;
        box-shadow: 0 2px 16px rgba(0,0,0,0.07); margin-bottom: 1rem;
    }
    .chart-title {
        font-size: 0.72rem; text-transform: uppercase; letter-spacing: 1.4px;
        color: #94A3B8; font-weight: 700; margin-bottom: 0.5rem;
    }
    .stAlert { border-radius: 12px !important; }
    </style>
    """, unsafe_allow_html=True)


def _kpi_cards(total, matched, pct_matched, discrepancies, missing, new_in_ending):
    st.markdown(f"""
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:1rem;margin-bottom:1.5rem;">

        <div style="background:linear-gradient(135deg,#1E3A8A,#2563EB);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(37,99,235,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Total SKU + Location</div>
            <div style="font-size:2.3rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{total:,}</div>
        </div>

        <div style="background:linear-gradient(135deg,#064E3B,#10B981);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(16,185,129,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Matched</div>
            <div style="font-size:2.3rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{pct_matched}%</div>
            <div style="font-size:0.75rem;opacity:0.8;margin-top:0.1rem;">{matched:,} records</div>
        </div>

        <div style="background:linear-gradient(135deg,#7F1D1D,#EF4444);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(239,68,68,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Discrepancies</div>
            <div style="font-size:2.3rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{discrepancies:,}</div>
        </div>

        <div style="background:linear-gradient(135deg,#78350F,#F59E0B);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(245,158,11,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Missing in Ending</div>
            <div style="font-size:2.3rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{missing:,}</div>
        </div>

        <div style="background:linear-gradient(135deg,#4C1D95,#8B5CF6);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(139,92,246,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">New in Ending</div>
            <div style="font-size:2.3rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{new_in_ending:,}</div>
        </div>

    </div>
    """, unsafe_allow_html=True)


def _price_kpi_cards(total_inv_value, total_var_value, skus_priced, total):
    pct_priced = round(skus_priced / total * 100, 1) if total > 0 else 0.0
    st.markdown(f"""
    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:1rem;margin-bottom:1.5rem;">

        <div style="background:linear-gradient(135deg,#1E3A8A,#2563EB);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(37,99,235,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Total Inventory Value</div>
            <div style="font-size:2rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{total_inv_value:,.0f}</div>
        </div>

        <div style="background:linear-gradient(135deg,#7F1D1D,#EF4444);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(239,68,68,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">Total Variance Value</div>
            <div style="font-size:2rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{total_var_value:,.0f}</div>
            <div style="font-size:0.75rem;opacity:0.8;margin-top:0.1rem;">absolute sum</div>
        </div>

        <div style="background:linear-gradient(135deg,#064E3B,#10B981);border-radius:18px;
                    padding:1.4rem 1.5rem;box-shadow:0 8px 24px rgba(16,185,129,0.3);color:white;position:relative;overflow:hidden;">
            <div style="position:absolute;top:-15px;right:-15px;width:80px;height:80px;
                        border-radius:50%;background:rgba(255,255,255,0.08);"></div>
            <div style="font-size:0.65rem;text-transform:uppercase;letter-spacing:1.4px;opacity:0.75;font-weight:700;">SKU+Loc Rows Priced</div>
            <div style="font-size:2rem;font-weight:800;line-height:1.15;margin-top:0.5rem;">{skus_priced:,}</div>
            <div style="font-size:0.75rem;opacity:0.8;margin-top:0.1rem;">{pct_priced}% of {total:,} rows</div>
        </div>

    </div>
    """, unsafe_allow_html=True)


def _chart_card(title):
    st.markdown(f'<div class="chart-card"><div class="chart-title">{title}</div>', unsafe_allow_html=True)

def _chart_card_end():
    st.markdown('</div>', unsafe_allow_html=True)


def _status_donut(df):
    counts = df["Status"].value_counts().reset_index()
    counts.columns = ["Status", "Count"]
    color_map = {"Matched": "#10B981", "Discrepancy": "#EF4444",
                 "Missing in Ending": "#F59E0B", "New in Ending": "#8B5CF6"}
    colors = [color_map.get(s, "#94A3B8") for s in counts["Status"]]
    total = counts["Count"].sum()
    matched_pct = round(df[df["Status"] == "Matched"].shape[0] / total * 100, 1) if total else 0

    fig = go.Figure(go.Pie(
        labels=counts["Status"], values=counts["Count"], hole=0.65,
        marker=dict(colors=colors, line=dict(color="white", width=2)),
        textinfo="percent", textfont_size=11,
        hovertemplate="<b>%{label}</b><br>%{value:,} records (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        annotations=[dict(text=f"<b>{matched_pct}%</b><br><span style='font-size:11px'>Matched</span>",
                          x=0.5, y=0.5, font_size=15, showarrow=False, font_color="#1E293B")],
        legend=dict(orientation="v", yanchor="middle", y=0.5, x=1.02, font_size=12),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=5, b=5, l=5, r=5), height=300, showlegend=True,
    )
    return fig


def _variance_histogram(df):
    disc = df[df["Variance"] != 0][["Variance"]].copy()
    if disc.empty:
        return None
    fig = px.histogram(disc, x="Variance", nbins=min(60, len(disc)),
                       color_discrete_sequence=["#2563EB"])
    fig.update_traces(marker_line_color="#1E3A8A", marker_line_width=0.5,
                      hovertemplate="Variance: %{x}<br>Count: %{y}<extra></extra>")
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=300, margin=dict(t=5, b=40, l=40, r=10),
        xaxis=dict(gridcolor="#E2E8F0", title="Variance (Expected − Actual)", linecolor="#E2E8F0"),
        yaxis=dict(gridcolor="#E2E8F0", title="Count", linecolor="#E2E8F0"),
        showlegend=False, bargap=0.05,
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
        xaxis=dict(gridcolor="#E2E8F0", linecolor="#E2E8F0", title="Net Quantity"),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
        showlegend=False,
    )
    return fig


def _style_table(df):
    """Apply status and variance colour coding via pandas Styler."""
    status_colors = {
        "Matched":           "color:#27AE60;font-weight:600",
        "Discrepancy":       "color:#E74C3C;font-weight:600",
        "Missing in Ending": "color:#F39C12;font-weight:600",
        "New in Ending":     "color:#8E44AD;font-weight:600",
    }

    def color_status(val):
        return status_colors.get(val, "")

    def color_numeric(v):
        try:
            v = float(v)
        except (TypeError, ValueError):
            return ""
        if v < 0:
            return "color:#E74C3C;font-weight:600"
        if v > 0:
            return "color:#27AE60;font-weight:600"
        return ""

    styler = df.style
    if "Status" in df.columns:
        styler = styler.map(color_status, subset=["Status"])
    for col in ("Variance", "Variance Value"):
        if col in df.columns:
            styler = styler.map(color_numeric, subset=[col])
    return styler


def main():
    st.set_page_config(page_title="Inventory Reconciliation", layout="wide", page_icon="📦")
    _inject_css()

    # ── Header ───────────────────────────────────────────────────────
    st.markdown("""
    <div style="margin-bottom:1.5rem;">
        <div style="font-size:1.6rem;font-weight:700;color:#1B2A4A;line-height:1.2;">
            📦 Inventory Reconciliation
        </div>
        <div style="font-size:0.88rem;color:#6B7C93;margin-top:0.3rem;">
            Upload your inventory and transaction files, then run reconciliation to surface variances.
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Sidebar brand ────────────────────────────────────────────────
    st.sidebar.markdown("""
    <div style="padding:1rem 0.5rem 0.75rem;margin-bottom:0.5rem;">
        <div style="font-size:1.25rem;font-weight:800;color:#F8FAFC;letter-spacing:-0.3px;">📦 RECO</div>
        <div style="font-size:0.7rem;color:#64748B;letter-spacing:1.6px;text-transform:uppercase;font-weight:600;">Inventory Reconciliation</div>
    </div>
    """, unsafe_allow_html=True)

    # ── File Uploads ─────────────────────────────────────────────────
    st.sidebar.header("Upload Files")
    starting_file = st.sidebar.file_uploader(
        "Starting Inventory (.xlsx or .csv)", type=["xlsx", "csv"], key="starting"
    )
    ending_file = st.sidebar.file_uploader(
        "Ending Inventory (.xlsx or .csv)", type=["xlsx", "csv"], key="ending"
    )
    tx_files = st.sidebar.file_uploader(
        "Transaction Files (.xlsx or .csv) — select all quarters at once",
        type=["xlsx", "csv"],
        accept_multiple_files=True,
        key="transactions",
    )

    all_uploaded = starting_file and ending_file and tx_files
    if not all_uploaded:
        st.markdown("""
        <div style="background:white;border-radius:14px;padding:2rem 2.5rem;
                    box-shadow:0 2px 10px rgba(0,0,0,0.07);max-width:520px;margin:2rem auto;text-align:center;">
            <div style="font-size:2.5rem;margin-bottom:1rem;">📂</div>
            <div style="font-size:1.1rem;font-weight:600;color:#1B2A4A;margin-bottom:0.5rem;">Upload Files to Begin</div>
            <div style="font-size:0.85rem;color:#6B7C93;line-height:1.7;">
                1. Starting Inventory (xlsx or csv)<br>
                2. Ending Inventory (xlsx or csv)<br>
                3. All Transaction files (xlsx or csv)
            </div>
            <div style="margin-top:1rem;font-size:0.8rem;color:#A0AEC0;">Use the sidebar on the left ←</div>
        </div>
        """, unsafe_allow_html=True)
        st.stop()

    st.sidebar.divider()

    st.markdown(f"""
    <div style="background:white;border-radius:12px;padding:1rem 1.5rem;
                box-shadow:0 1px 6px rgba(0,0,0,0.06);margin-bottom:1rem;
                display:flex;align-items:center;gap:0.75rem;">
        <span style="font-size:1.3rem;">✅</span>
        <span style="color:#2C3E50;font-size:0.9rem;font-weight:500;">
            <b>{starting_file.name}</b> · <b>{ending_file.name}</b> · <b>{len(tx_files)} transaction file(s)</b> ready
        </span>
    </div>
    """, unsafe_allow_html=True)
    run_clicked = st.button("▶ Run Reconciliation", type="primary", use_container_width=True)

    if run_clicked:
        with st.spinner("Processing files and running reconciliation..."):
            starting = parse_starting_inventory(starting_file.getvalue(), starting_file.name)
            ending = parse_ending_inventory(ending_file.getvalue(), ending_file.name)
            transactions = parse_transactions(
                tuple(f.getvalue() for f in tx_files),
                tuple(f.name for f in tx_files),
            )
            st.session_state["recon_result"] = reconcile(starting, ending, transactions)
            st.session_state["tx_data"] = transactions

    if "recon_result" not in st.session_state:
        st.stop()

    recon = st.session_state["recon_result"]
    transactions = st.session_state["tx_data"]

    st.sidebar.divider()

    # ── Sidebar Filters ──────────────────────────────────────────────
    st.sidebar.header("Filters")

    loc_options = sorted(recon["LocCode"].unique().tolist())
    selected_locs = st.sidebar.multiselect("Location (LocCode)", loc_options, default=loc_options)

    sku_filter = st.sidebar.text_input("SKU Search (partial match)").strip().upper()

    _date_min = transactions["Transaction Date"].min()
    _date_max = transactions["Transaction Date"].max()
    safe_min = _date_min.date() if pd.notna(_date_min) else _date(2025, 1, 1)
    safe_max = _date_max.date() if pd.notna(_date_max) else _date.today()
    date_range = st.sidebar.date_input(
        "Transaction Date Range (Tab 4 only)",
        value=[safe_min, safe_max],
        min_value=safe_min,
        max_value=safe_max,
    )

    # Apply filters
    filtered_recon = recon[recon["LocCode"].isin(selected_locs)].copy()
    if sku_filter:
        filtered_recon = filtered_recon[filtered_recon["SKU"].str.contains(sku_filter, na=False, regex=False)]

    filtered_tx = transactions[transactions["LocCode"].isin(selected_locs)].copy()
    if sku_filter:
        filtered_tx = filtered_tx[filtered_tx["SKU"].str.contains(sku_filter, na=False, regex=False)]
    if len(date_range) == 2:
        start_dt = pd.Timestamp(date_range[0])
        end_dt = pd.Timestamp(date_range[1])
        filtered_tx = filtered_tx[
            (filtered_tx["Transaction Date"] >= start_dt) &
            (filtered_tx["Transaction Date"] <= end_dt)
        ]

    if not selected_locs:
        st.warning("No locations selected — please choose at least one location from the sidebar.")
        st.stop()

    # ── Tabs ─────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊  Summary", "📋  Full Detail", "⚠️  Discrepancies", "🔍  Transaction Breakdown"]
    )

    tx_type_cols = [c for c in filtered_recon.columns if c.startswith("TX: ")]
    detail_cols = (
        ["SKU", "LocCode", "Starting Qty"]
        + tx_type_cols
        + ["Net Transactions", "Expected Ending Qty", "Actual Ending Qty", "Variance", "Status"]
    )

    with tab1:
        total = len(filtered_recon)
        matched = int((filtered_recon["Status"] == "Matched").sum())
        discrepancies = int((filtered_recon["Status"] == "Discrepancy").sum())
        missing = int((filtered_recon["Status"] == "Missing in Ending").sum())
        new_in_ending = int((filtered_recon["Status"] == "New in Ending").sum())
        pct_matched = round(matched / total * 100, 1) if total > 0 else 0.0

        _kpi_cards(total, matched, pct_matched, discrepancies, missing, new_in_ending)

        col1, col2 = st.columns(2)
        with col1:
            _chart_card("Status Breakdown")
            st.plotly_chart(_status_donut(filtered_recon), use_container_width=True)
            _chart_card_end()

        with col2:
            _chart_card("Variance Distribution (non-zero)")
            hist = _variance_histogram(filtered_recon)
            if hist is not None:
                st.plotly_chart(hist, use_container_width=True)
            else:
                st.success("Everything matches — no variances found!")
            _chart_card_end()

        tx_bar = _tx_type_chart(filtered_recon)
        if tx_bar is not None:
            _chart_card("Net Quantity by Transaction Type")
            st.plotly_chart(tx_bar, use_container_width=True)
            _chart_card_end()

    # ── Tab 2: Full Detail ───────────────────────────────────────────
    with tab2:
        c1, c2 = st.columns([2, 6])
        with c1:
            status_opts = ["All"] + sorted(filtered_recon["Status"].unique().tolist())
            status_filter = st.selectbox("Filter by Status", status_opts, key="detail_status")
        detail_df = (
            filtered_recon if status_filter == "All"
            else filtered_recon[filtered_recon["Status"] == status_filter]
        )
        st.markdown(f"<div style='font-size:0.8rem;color:#8896A5;margin-bottom:0.5rem;'>{len(detail_df):,} rows</div>", unsafe_allow_html=True)
        st.dataframe(_style_table(detail_df[detail_cols]), use_container_width=True, height=580)

    # ── Tab 3: Discrepancies ─────────────────────────────────────────
    with tab3:
        disc_df = (
            filtered_recon[filtered_recon["Variance"] != 0]
            .copy()
            .assign(AbsVariance=lambda d: d["Variance"].abs())
            .sort_values("AbsVariance", ascending=False)
            .drop(columns=["AbsVariance"])
        )
        c1, c2 = st.columns([6, 2])
        with c1:
            st.markdown(f"<div style='font-size:0.8rem;color:#E74C3C;font-weight:600;margin-bottom:0.5rem;'>{len(disc_df):,} discrepant rows</div>", unsafe_allow_html=True)
        with c2:
            csv_bytes = disc_df[detail_cols].to_csv(index=False).encode("utf-8")
            st.download_button("⬇ Download CSV", data=csv_bytes, file_name="discrepancies.csv", mime="text/csv", use_container_width=True)
        st.dataframe(_style_table(disc_df[detail_cols]), use_container_width=True, height=580)

    # ── Tab 4: Transaction Breakdown ─────────────────────────────────
    with tab4:
        key_options = sorted(
            (filtered_recon["SKU"] + " | " + filtered_recon["LocCode"]).tolist()
        )
        if not key_options:
            st.info("No data matches current filters.")
        else:
            selected_key = st.selectbox("Select SKU + Location", key_options)
            sel_sku, sel_loc = [x.strip() for x in selected_key.split("|", maxsplit=1)]
            tx_detail = filtered_tx[
                (filtered_tx["SKU"] == sel_sku) &
                (filtered_tx["LocCode"] == sel_loc)
            ].copy()
            st.write(f"**{len(tx_detail)} transactions** for `{sel_sku}` at `{sel_loc}`")
            st.dataframe(
                tx_detail[[
                    "Transaction Date", "Transaction Type",
                    "Quantity", "Direction", "Reference No",
                ]].sort_values("Transaction Date"),
                use_container_width=True,
                height=500,
            )


if __name__ == "__main__":
    main()
