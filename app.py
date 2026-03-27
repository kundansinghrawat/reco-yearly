import io
from datetime import date as _date

import pandas as pd
import streamlit as st


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


def main():
    st.set_page_config(page_title="Inventory Reconciliation", layout="wide")
    st.title("Inventory Reconciliation Dashboard")

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
        st.info(
            "**Upload all three file sets in the sidebar to begin:**\n\n"
            "1. Starting Inventory (xlsx or csv)\n"
            "2. Ending Inventory (xlsx or csv)\n"
            "3. Transaction files — select all files at once (xlsx or csv)"
        )
        st.stop()

    st.sidebar.divider()

    st.success(f"✅ {len(tx_files)} transaction file(s) uploaded. Ready to reconcile.")
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

    tx_type_cols = [c for c in filtered_recon.columns if c.startswith("TX: ")]
    detail_cols = (
        ["SKU", "LocCode", "Starting Qty"]
        + tx_type_cols
        + ["Net Transactions", "Expected Ending Qty", "Actual Ending Qty", "Variance", "Status"]
    )

    # ── Tab 2: Full Detail ───────────────────────────────────────────
    with tab2:
        status_opts = ["All"] + sorted(filtered_recon["Status"].unique().tolist())
        status_filter = st.selectbox("Filter by Status", status_opts, key="detail_status")
        detail_df = (
            filtered_recon
            if status_filter == "All"
            else filtered_recon[filtered_recon["Status"] == status_filter]
        )
        st.dataframe(detail_df[detail_cols], use_container_width=True, height=600)

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
            disc_df[detail_cols],
            use_container_width=True,
            height=600,
        )
        csv_bytes = disc_df[detail_cols].to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Discrepancies as CSV",
            data=csv_bytes,
            file_name="discrepancies.csv",
            mime="text/csv",
        )

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
