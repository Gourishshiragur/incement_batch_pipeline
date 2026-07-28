"""
Generates realistic daily mining-telemetry SNAPSHOT extracts.

Design mirrors the real-world pattern this project models: each day's extract
is a re-pulled snapshot window from source systems (not a clean incremental
feed), so most rows repeat unchanged day-over-day, a small % are corrected
(late-arriving sensor corrections / re-transmits), and a small % are genuinely
new readings. This is exactly the scenario that makes snapshot-comparison +
MERGE upserts valuable versus a full reload.

Scale target: ~1.2-1.5M rows per daily snapshot, matching resume claim of
"1-2 million records per run".
"""
import numpy as np
import pandas as pd
import os

RNG = np.random.default_rng(42)

N_CUSTOMERS = 25
MACHINES_PER_CUSTOMER = (25, 55)   # random range per customer
READINGS_PER_MACHINE_PER_DAY = 360  # ~ every 4 min over a 24h operating window
N_DAYS = 5

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

FAULT_CODES = ["NONE"] * 20 + ["F101_LOW_FUEL", "F204_ENGINE_TEMP", "F310_HYDRAULIC",
               "F450_GPS_LOSS", "F512_PAYLOAD_OVERLOAD"]


def build_machine_roster():
    rows = []
    for c in range(1, N_CUSTOMERS + 1):
        customer_id = f"CUST{c:03d}"
        n_machines = RNG.integers(MACHINES_PER_CUSTOMER[0], MACHINES_PER_CUSTOMER[1] + 1)
        for m in range(1, n_machines + 1):
            machine_id = f"MCH{c:03d}{m:04d}"
            base_lat = RNG.uniform(-33.9, -12.4)   # spread across mining regions
            base_lon = RNG.uniform(115.8, 150.9)
            rows.append((customer_id, machine_id, base_lat, base_lon))
    return pd.DataFrame(rows, columns=["customer_id", "machine_id", "base_lat", "base_lon"])


def generate_day_readings(roster: pd.DataFrame, day_idx: int, start_reading_seq: int):
    """Generate a fresh block of readings for a given day; each reading gets a
    globally unique reading_id so we can later control which ones get
    re-included (unchanged), corrected (changed), or omitted (aged out)."""
    records = []
    seq = start_reading_seq
    for row in roster.itertuples(index=False):
        for r in range(READINGS_PER_MACHINE_PER_DAY):
            seq += 1
            fuel = round(float(np.clip(RNG.normal(55, 20), 2, 100)), 1)
            payload = round(float(np.clip(RNG.normal(28, 9), 0, 60)), 2)
            fault = FAULT_CODES[RNG.integers(0, len(FAULT_CODES))]
            records.append((
                seq,
                row.customer_id,
                row.machine_id,
                f"2026-0{6+day_idx if 6+day_idx<10 else 6+day_idx}-{14+day_idx:02d}T{(r*32)%24:02d}:{(r*32)%60:02d}:00",
                round(row.base_lat + RNG.uniform(-0.01, 0.01), 6),
                round(row.base_lon + RNG.uniform(-0.01, 0.01), 6),
                fuel,
                payload,
                fault,
            ))
    cols = ["reading_id", "customer_id", "machine_id", "event_ts", "gps_lat", "gps_lon",
            "fuel_level", "payload_weight_t", "fault_code"]
    return pd.DataFrame(records, columns=cols), seq


def main():
    roster = build_machine_roster()
    roster.to_csv(f"{OUT_DIR}/machine_roster.csv", index=False)
    print(f"Roster: {len(roster)} machines across {N_CUSTOMERS} customers")

    seq = 0
    all_day_blocks = []
    for d in range(N_DAYS):
        block, seq = generate_day_readings(roster, d, seq)
        all_day_blocks.append(block)
        print(f"Day {d} new-readings block generated: {len(block):,} rows (running seq -> {seq:,})")

    # Now assemble each day's SNAPSHOT extract, simulating a re-pulled window
    # covering "yesterday + today" readings, with a slice of yesterday's rows
    # arriving CORRECTED (value changed) to model late sensor re-transmits.
    prev_snapshot = None
    for d in range(N_DAYS):
        today_new = all_day_blocks[d].copy()

        if prev_snapshot is None:
            snapshot = today_new.copy()
        else:
            carry = prev_snapshot.copy()
            # ~4% of carried-forward rows get a corrected reading (simulates
            # late-arriving corrections to fuel/payload/fault values)
            n_correct = int(len(carry) * 0.04)
            correct_idx = RNG.choice(carry.index, size=n_correct, replace=False)
            carry.loc[correct_idx, "fuel_level"] = np.clip(
                carry.loc[correct_idx, "fuel_level"] + RNG.normal(0, 15, n_correct), 2, 100
            ).round(1)
            carry.loc[correct_idx, "payload_weight_t"] = np.clip(
                carry.loc[correct_idx, "payload_weight_t"] + RNG.normal(0, 6, n_correct), 0, 60
            ).round(2)
            carry.loc[correct_idx, "fault_code"] = [
                FAULT_CODES[RNG.integers(0, len(FAULT_CODES))] for _ in range(n_correct)
            ]
            # ~15% of the oldest carried rows age out of the extract window (not resent)
            n_drop = int(len(carry) * 0.15)
            drop_idx = RNG.choice(carry.index, size=n_drop, replace=False)
            carry = carry.drop(index=drop_idx)

            snapshot = pd.concat([carry, today_new], ignore_index=True)

        snapshot.to_csv(f"{OUT_DIR}/snapshot_day{d}.csv", index=False)
        print(f"snapshot_day{d}.csv written: {len(snapshot):,} rows")
        prev_snapshot = snapshot


if __name__ == "__main__":
    main()
