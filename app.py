import streamlit as st
import pandas as pd
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def direction_to_sign(direction: str) -> int:
    """Convert Direction string to +1 or -1 for quantity calculation."""
    d = str(direction).strip().lower()
    if d == "in":
        return 1
    if d in ("out", "within"):
        return -1
    return 0


@st.cache_data
def load_starting_inventory() -> pd.DataFrame:
    folder = os.path.join(BASE_DIR, "STARTING INVENTORY")
    files = glob.glob(os.path.join(folder, "*.xlsx"))
    if not files:
        return pd.DataFrame(columns=["SKU", "LocCode", "Starting Qty"])
    path = files[0]  # use the first (and typically only) file
    df = pd.read_excel(path, usecols=["SKU", "LocCode", "Total Qty"])
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["LocCode"] = df["LocCode"].astype(str).str.strip().str.upper()
    df = df.rename(columns={"Total Qty": "Starting Qty"})
    df["Starting Qty"] = pd.to_numeric(df["Starting Qty"], errors="coerce").fillna(0)
    return df.groupby(["SKU", "LocCode"], as_index=False)["Starting Qty"].sum()


@st.cache_data
def load_ending_inventory() -> pd.DataFrame:
    folder = os.path.join(BASE_DIR, "ENDING INVENTORY")
    files = glob.glob(os.path.join(folder, "*.csv"))
    if not files:
        return pd.DataFrame(columns=["SKU", "LocCode", "Actual Ending Qty"])
    dfs = [pd.read_csv(f, usecols=["SKU", "LocCode", "Total Qty"]) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["LocCode"] = df["LocCode"].astype(str).str.strip().str.upper()
    df = df.rename(columns={"Total Qty": "Actual Ending Qty"})
    df["Actual Ending Qty"] = pd.to_numeric(df["Actual Ending Qty"], errors="coerce").fillna(0)
    return df.groupby(["SKU", "LocCode"], as_index=False)["Actual Ending Qty"].sum()


@st.cache_data
def load_transactions() -> pd.DataFrame:
    base = os.path.join(BASE_DIR, "Transaction")
    files = glob.glob(os.path.join(base, "**", "*.xlsx"), recursive=True)
    if not files:
        return pd.DataFrame(columns=["SKU", "LocCode", "Quantity", "Direction",
                                      "Transaction Type", "Transaction Date", "Reference No",
                                      "Sign", "Net Qty"])
    dfs = []
    for f in files:
        df = pd.read_excel(
            f,
            usecols=["locCode", "SKU", "Quantity", "Direction",
                     "Transaction Type", "Transaction Date", "Reference No"],
        )
        dfs.append(df)
    tx = pd.concat(dfs, ignore_index=True)
    tx = tx.rename(columns={"locCode": "LocCode"})
    tx["SKU"] = tx["SKU"].astype(str).str.strip().str.upper()
    tx["LocCode"] = tx["LocCode"].astype(str).str.strip().str.upper()
    tx["Quantity"] = pd.to_numeric(tx["Quantity"], errors="coerce").fillna(0)
    tx["Sign"] = tx["Direction"].apply(direction_to_sign)
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
    return df


def main():
    st.set_page_config(page_title="Inventory Reconciliation", layout="wide")
    st.title("Inventory Reconciliation Dashboard")

    with st.spinner("Loading data (this may take a moment)..."):
        starting = load_starting_inventory()
        ending = load_ending_inventory()
        transactions = load_transactions()
        recon = reconcile(starting, ending, transactions)

    # ── Sidebar Filters ──────────────────────────────────────────────
    st.sidebar.header("Filters")

    loc_options = sorted(recon["LocCode"].unique().tolist())
    selected_locs = st.sidebar.multiselect("Location (LocCode)", loc_options, default=loc_options)

    sku_filter = st.sidebar.text_input("SKU Search (partial match)").strip().upper()

    date_min = transactions["Transaction Date"].min()
    date_max = transactions["Transaction Date"].max()
    date_range = st.sidebar.date_input(
        "Transaction Date Range",
        value=[date_min.date(), date_max.date()],
        min_value=date_min.date(),
        max_value=date_max.date(),
    )

    # Apply filters
    filtered_recon = recon[recon["LocCode"].isin(selected_locs)].copy()
    if sku_filter:
        filtered_recon = filtered_recon[filtered_recon["SKU"].str.contains(sku_filter, na=False)]

    filtered_tx = transactions[transactions["LocCode"].isin(selected_locs)].copy()
    if sku_filter:
        filtered_tx = filtered_tx[filtered_tx["SKU"].str.contains(sku_filter, na=False)]
    if len(date_range) == 2:
        start_dt = pd.Timestamp(date_range[0])
        end_dt = pd.Timestamp(date_range[1])
        filtered_tx = filtered_tx[
            (filtered_tx["Transaction Date"] >= start_dt) &
            (filtered_tx["Transaction Date"] <= end_dt)
        ]

    # ── Tabs ─────────────────────────────────────────────────────────
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Summary", "Full Detail", "Discrepancies", "Transaction Breakdown"]
    )

    with tab1:
        total = len(filtered_recon)
        matched = int((filtered_recon["Status"] == "Matched").sum())
        discrepancies = int((filtered_recon["Status"] == "Discrepancy").sum())
        missing = int((filtered_recon["Status"] == "Missing in Ending").sum())
        new_in_ending = int((filtered_recon["Status"] == "New in Ending").sum())
        pct_matched = round(matched / total * 100, 1) if total > 0 else 0.0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total SKU+Location", total)
        c2.metric("Matched", f"{pct_matched}%")
        c3.metric("Discrepancies", discrepancies)
        c4.metric("Missing in Ending", missing)
        c5.metric("New in Ending", new_in_ending)

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Status Breakdown")
            status_counts = (
                filtered_recon["Status"]
                .value_counts()
                .rename_axis("Status")
                .reset_index(name="Count")
            )
            st.bar_chart(status_counts.set_index("Status")["Count"])

        with col2:
            st.subheader("Variance Distribution (non-zero only)")
            disc_variances = filtered_recon[filtered_recon["Variance"] != 0]["Variance"]
            if not disc_variances.empty:
                st.bar_chart(disc_variances.value_counts().sort_index())
            else:
                st.info("No variances found — everything matches!")

    # ── Tab 2: Full Detail ───────────────────────────────────────────
    with tab2:
        status_opts = ["All"] + sorted(filtered_recon["Status"].unique().tolist())
        status_filter = st.selectbox("Filter by Status", status_opts, key="detail_status")
        detail_df = (
            filtered_recon
            if status_filter == "All"
            else filtered_recon[filtered_recon["Status"] == status_filter]
        )
        st.dataframe(
            detail_df[[
                "SKU", "LocCode", "Starting Qty", "Net Transactions",
                "Expected Ending Qty", "Actual Ending Qty", "Variance", "Status",
            ]],
            use_container_width=True,
            height=600,
        )

    # ── Tab 3: Discrepancies ─────────────────────────────────────────
    with tab3:
        disc_df = (
            filtered_recon[filtered_recon["Variance"] != 0]
            .copy()
            .assign(AbsVariance=lambda d: d["Variance"].abs())
            .sort_values("AbsVariance", ascending=False)
            .drop(columns=["AbsVariance"])
        )
        st.write(f"**{len(disc_df)} discrepant rows**")
        st.dataframe(
            disc_df[[
                "SKU", "LocCode", "Starting Qty", "Net Transactions",
                "Expected Ending Qty", "Actual Ending Qty", "Variance", "Status",
            ]],
            use_container_width=True,
            height=600,
        )
        csv_bytes = disc_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Discrepancies as CSV",
            data=csv_bytes,
            file_name="discrepancies.csv",
            mime="text/csv",
        )

    # ── Tab 4: Transaction Breakdown ─────────────────────────────────
    with tab4:
        key_options = sorted(
            filtered_recon.apply(lambda r: f"{r['SKU']} | {r['LocCode']}", axis=1).tolist()
        )
        if not key_options:
            st.info("No data matches current filters.")
        else:
            selected_key = st.selectbox("Select SKU + Location", key_options)
            sel_sku, sel_loc = [x.strip() for x in selected_key.split("|")]
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
