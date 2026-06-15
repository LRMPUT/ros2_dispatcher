import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyze_scaling import (
    aggregate_cell,
    expected_messages,
    load_jsonl,
    percentile,
)


def test_percentile_basic():
    samples = list(range(1, 101))  # 1..100
    assert percentile(samples, 0.5) == 50
    assert percentile(samples, 0.95) == 95
    assert percentile(samples, 0.99) == 99


def test_percentile_empty_returns_zero():
    assert percentile([], 0.95) == 0


def test_expected_messages():
    assert expected_messages(n=10, rate_hz=10, duration_s=60) == 6000


def test_load_jsonl_round_trip(tmp_path):
    p = tmp_path / "x.jsonl"
    p.write_text('{"latency_ns": 100}\n{"latency_ns": 200}\n')
    rows = load_jsonl(str(p))
    assert [r["latency_ns"] for r in rows] == [100, 200]


def test_aggregate_cell_basic(tmp_path):
    cell_dir = tmp_path / "N=5_broker=kafka_run=1"
    cell_dir.mkdir()
    rows = [{"latency_ns": v} for v in [100, 200, 300, 400, 500]]
    with open(cell_dir / "consumer.jsonl", "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    summary = aggregate_cell(str(cell_dir), n=5, rate_hz=10, duration_s=10)
    assert summary["received"] == 5
    assert summary["latency_avg_ns"] == 300
    # drop_rate = 1 - 5/(5*10*10) = 1 - 5/500 = 0.99
    assert abs(summary["drop_rate"] - 0.99) < 1e-9
