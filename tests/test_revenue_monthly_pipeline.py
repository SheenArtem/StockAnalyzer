from pathlib import Path

import pandas as pd
import pytest

from tools import compute_revenue_scores_monthly as revenue_producer


REPO = Path(__file__).resolve().parents[1]


def _raw_revenue_through_june_2026() -> pd.DataFrame:
    periods = pd.period_range("2024-12", "2026-06", freq="M")
    dates = (periods + 1).to_timestamp()
    revenue = pd.Series(100.0, index=periods)
    revenue.loc[pd.Period("2026-03", freq="M")] = 120.0
    revenue.loc[pd.Period("2026-06", freq="M")] = 80.0
    return pd.DataFrame({
        "stock_id": "2330",
        "date": dates,
        "revenue_year": periods.year,
        "revenue_month": periods.month,
        "revenue": revenue.to_numpy(),
    })


def test_june_revenue_is_available_july_10(tmp_path, monkeypatch):
    raw_path = tmp_path / "financials_revenue.parquet"
    score_path = tmp_path / "revenue_scores_monthly.parquet"
    _raw_revenue_through_june_2026().to_parquet(raw_path)

    monkeypatch.setattr(revenue_producer, "IN_PATH", raw_path)
    monkeypatch.setattr(revenue_producer, "OUT_PATH", score_path)
    revenue_producer.main()

    produced = pd.read_parquet(score_path)
    june_score = produced.loc[
        produced["date"] == pd.Timestamp("2026-07-10"), "revenue_score"
    ]
    assert june_score.tolist() == [30.0]


def test_raw_revenue_date_must_match_year_month_contract():
    raw = _raw_revenue_through_june_2026()
    raw.loc[raw.index[-1], "date"] = pd.Timestamp("2026-06-01")

    with pytest.raises(RuntimeError, match="raw date must equal"):
        revenue_producer._prepare_revenue_input(raw)


def test_latest_period_coverage_rejects_partial_market():
    frames = []
    for index in range(10):
        frame = _raw_revenue_through_june_2026()
        frame["stock_id"] = str(1100 + index)
        if index >= 7:
            frame = frame.iloc[:-1]
        frames.append(frame)
    prepared = revenue_producer._prepare_revenue_input(
        pd.concat(frames, ignore_index=True))
    scored = revenue_producer._score_revenue_rows(prepared)

    with pytest.raises(RuntimeError, match="raw latest-period coverage collapsed"):
        revenue_producer._validate_latest_period_coverage(prepared, scored)


def test_calendar_period_matching_does_not_shift_across_missing_month():
    raw = _raw_revenue_through_june_2026()
    raw.loc[
        (raw["revenue_year"] == 2025) & (raw["revenue_month"] == 5),
        "revenue",
    ] = 50.0
    raw = raw.loc[
        ~((raw["revenue_year"] == 2026) & (raw["revenue_month"] == 1))
    ].copy()

    prepared = revenue_producer._prepare_revenue_input(raw)
    scored = revenue_producer._score_revenue_rows(prepared)
    june = scored.loc[scored["_period"] == pd.Period("2026-06", freq="M")]

    assert june["revenue_score"].tolist() == [30.0]


def test_missing_exact_year_ago_month_produces_no_shifted_score():
    raw = _raw_revenue_through_june_2026()
    raw = raw.loc[
        ~((raw["revenue_year"] == 2025) & (raw["revenue_month"] == 6))
    ].copy()

    prepared = revenue_producer._prepare_revenue_input(raw)
    scored = revenue_producer._score_revenue_rows(prepared)

    assert pd.Period("2026-06", freq="M") not in set(scored["_period"])


def test_bulk_revenue_batch_rebuilds_scores_and_propagates_failures():
    batch_bytes = (REPO / "run_bulk_revenue_monthly.bat").read_bytes()
    assert all(byte < 128 for byte in batch_bytes)
    assert b"\n" not in batch_bytes.replace(b"\r\n", b"")

    batch = batch_bytes.decode("ascii")
    raw_command = "python tools\\vfvc_backfill_monthly_rev.py --bulk-update"
    score_command = "python tools\\compute_revenue_scores_monthly.py"
    raw_index = batch.index(raw_command)
    score_index = batch.index(score_command)
    failure_label_index = batch.index(":failed")
    guard = 'if not "%EC%"=="0" goto failed'

    assert raw_index < score_index
    assert batch.count("set EC=%ERRORLEVEL%") == 2
    assert batch.count(guard) == 2
    assert batch.index(guard, raw_index) < score_index
    assert score_index < batch.index(guard, score_index) < failure_label_index
    assert "ERROR: %STEP% failed (exit=%EC%)." in batch
    assert batch.rstrip().endswith("exit /b %EC%")
