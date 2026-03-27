import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from app import direction_to_sign, reconcile


# --- direction_to_sign tests ---

def test_direction_in_returns_positive_one():
    assert direction_to_sign("In") == 1


def test_direction_out_returns_negative_one():
    assert direction_to_sign("Out") == -1


def test_direction_within_returns_negative_one():
    # WithIn = internal transfer out of this location
    assert direction_to_sign("WithIn") == -1


def test_direction_is_case_insensitive():
    assert direction_to_sign("IN") == 1
    assert direction_to_sign("OUT") == -1
    assert direction_to_sign("WITHIN") == -1


def test_direction_unknown_returns_zero():
    assert direction_to_sign("garbage") == 0


# --- reconcile tests ---

def _make_starting(rows):
    return pd.DataFrame(rows, columns=["SKU", "LocCode", "Starting Qty"])


def _make_ending(rows):
    return pd.DataFrame(rows, columns=["SKU", "LocCode", "Actual Ending Qty"])


def _make_tx(rows):
    """rows: list of (SKU, LocCode, Quantity, Direction)"""
    records = []
    for sku, loc, qty, direction in rows:
        sign = direction_to_sign(direction)
        records.append({
            "SKU": sku,
            "LocCode": loc,
            "Quantity": qty,
            "Direction": direction,
            "Sign": sign,
            "Net Qty": qty * sign,
            "Transaction Date": pd.NaT,
            "Transaction Type": "Test",
            "Reference No": "",
        })
    return pd.DataFrame(records)


def test_reconcile_matched_row():
    starting = _make_starting([["SKU1", "LOC1", 100]])
    ending = _make_ending([["SKU1", "LOC1", 110]])
    tx = _make_tx([("SKU1", "LOC1", 10, "In")])
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU1") & (result["LocCode"] == "LOC1")].iloc[0]
    assert row["Status"] == "Matched"
    assert row["Variance"] == 0
    assert row["Expected Ending Qty"] == 110


def test_reconcile_discrepancy_row():
    starting = _make_starting([["SKU1", "LOC1", 100]])
    ending = _make_ending([["SKU1", "LOC1", 115]])
    tx = _make_tx([("SKU1", "LOC1", 10, "In")])
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU1") & (result["LocCode"] == "LOC1")].iloc[0]
    assert row["Status"] == "Discrepancy"
    assert row["Variance"] == -5  # expected 110, actual 115


def test_reconcile_missing_in_ending():
    starting = _make_starting([["SKU1", "LOC1", 100]])
    ending = _make_ending([])
    tx = _make_tx([])
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU1") & (result["LocCode"] == "LOC1")].iloc[0]
    assert row["Status"] == "Missing in Ending"


def test_reconcile_new_in_ending():
    starting = _make_starting([])
    ending = _make_ending([["SKU2", "LOC2", 50]])
    tx = _make_tx([])
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU2") & (result["LocCode"] == "LOC2")].iloc[0]
    assert row["Status"] == "New in Ending"


def test_reconcile_multiple_transactions_net_correctly():
    starting = _make_starting([["SKU1", "LOC1", 50]])
    ending = _make_ending([["SKU1", "LOC1", 60]])
    tx = _make_tx([
        ("SKU1", "LOC1", 20, "In"),
        ("SKU1", "LOC1", 5, "Out"),
        ("SKU1", "LOC1", 5, "WithIn"),
    ])
    # Net = +20 - 5 - 5 = +10; Expected = 50+10 = 60; Actual = 60; Variance = 0
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU1") & (result["LocCode"] == "LOC1")].iloc[0]
    assert row["Net Transactions"] == 10
    assert row["Status"] == "Matched"


def test_reconcile_sku_normalization():
    # SKUs with different casing and spaces should match
    starting = _make_starting([[" sku1 ", "loc1", 100]])
    ending = _make_ending([["SKU1", "LOC1", 100]])
    tx = _make_tx([])
    result = reconcile(starting, ending, tx)
    row = result[(result["SKU"] == "SKU1") & (result["LocCode"] == "LOC1")].iloc[0]
    assert row["Status"] == "Matched"
