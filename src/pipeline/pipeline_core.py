"""
Core pipeline logic, extracted so it's importable by both the validation
harness (validation/run_pipeline_validation.py) and the unit test suite
(tests/test_pipeline_logic.py). This is the single source of truth for the
Bronze->Silver change-detection->merge logic being tested.
"""
import pandas as pd
import numpy as np

TRACKED_FIELDS = ["fuel_level", "payload_weight_t", "fault_code"]


def silver_data_quality_gate(df: pd.DataFrame):
    """Drops rows with null business keys or out-of-range sensor values,
    then dedupes on reading_id keeping the last occurrence."""
    before = len(df)
    if before == 0:
        return df.copy(), 0
    out = df.dropna(subset=["customer_id", "machine_id", "reading_id"])
    out = out[out["fuel_level"].between(0, 100)]
    out = out[out["payload_weight_t"].between(0, 60)]
    dropped_by_gate = before - len(out)
    out = out.drop_duplicates(subset=["reading_id"], keep="last")
    total_dropped = before - len(out)
    return out.reset_index(drop=True), total_dropped


def silver_change_detection(df: pd.DataFrame, prior_silver_df: pd.DataFrame = None):
    """Classifies each row as NEW, CHANGED, or UNCHANGED versus prior_silver_df
    (compared on TRACKED_FIELDS, joined on reading_id)."""
    df = df.copy()
    if prior_silver_df is None or len(prior_silver_df) == 0:
        df["_change_type"] = "NEW"
        return df

    prior_keyed = prior_silver_df[["reading_id"] + TRACKED_FIELDS].rename(
        columns={f: f"_prior_{f}" for f in TRACKED_FIELDS}
    )
    merged = df.merge(prior_keyed, on="reading_id", how="left", indicator=True)

    is_new = merged["_merge"] == "left_only"
    field_changed = pd.Series(False, index=merged.index)
    for f in TRACKED_FIELDS:
        field_changed |= (merged[f] != merged[f"_prior_{f}"]) & ~is_new

    change_type = np.select([is_new, field_changed], ["NEW", "CHANGED"], default="UNCHANGED")
    df["_change_type"] = change_type
    return df


def merge_upsert(prior_state: pd.DataFrame, to_upsert: pd.DataFrame):
    """Delta-MERGE-equivalent upsert: updates matching reading_id rows in
    place, inserts rows with no match. Rows not present in to_upsert (i.e.
    UNCHANGED rows the caller already filtered out) are left untouched."""
    to_upsert = to_upsert.drop(columns=["_change_type"], errors="ignore")
    if len(to_upsert) == 0:
        return prior_state.copy()

    state = prior_state.set_index("reading_id")
    updates = to_upsert.set_index("reading_id")
    state.update(updates)
    new_rows = updates[~updates.index.isin(state.index)]
    result = pd.concat([state, new_rows])
    result = result[~result.index.duplicated(keep="last")]
    return result.reset_index()
