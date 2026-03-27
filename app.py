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
