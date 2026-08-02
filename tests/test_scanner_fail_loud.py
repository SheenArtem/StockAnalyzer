from pathlib import Path

import pytest

from tools import verify_scan_stages as verifier


ROOT = Path(__file__).resolve().parent.parent
BATCH_PATH = ROOT / 'run_scanner.bat'

SUCCESS_MARKERS = (
    'Scanner started',
    'YT sync done',
    'RF-1 consistency check done',
    'Market regime logger done',
    'Universe price refresh done (exit=0)',
    'Refresh backtest panels done (exit=0)',
    'Chip history resume done',
    'News flow anomaly done',
    'Theme momentum done',
    'ATM PUT premium archive done',
    'Minifutures ratio archive done',
    'Options institutional archive done',
    'Earnings calendar fetch done',
    'Scanner finished (exit=0)',
)

MARKET_PANEL_SUCCESS_MARKERS = SUCCESS_MARKERS[4:6]


def _run_verifier(monkeypatch, tmp_path, markers):
    log_path = tmp_path / 'scanner.log'
    log_path.write_text(
        ''.join(f'[2026-07-14T00:00:00] {marker}\n' for marker in markers),
        encoding='utf-8',
    )
    monkeypatch.setattr(verifier, 'LOG_PATH', log_path)
    return verifier.main()


def test_verifier_accepts_complete_scanner_pipeline(monkeypatch, tmp_path):
    assert _run_verifier(monkeypatch, tmp_path, SUCCESS_MARKERS) == 0


@pytest.mark.parametrize('missing_marker', MARKET_PANEL_SUCCESS_MARKERS)
def test_verifier_rejects_each_missing_market_panel_stage(
    monkeypatch, tmp_path, capsys, missing_marker
):
    markers = [marker for marker in SUCCESS_MARKERS if marker != missing_marker]

    assert _run_verifier(monkeypatch, tmp_path, markers) == 1
    assert missing_marker.removesuffix(' (exit=0)') in capsys.readouterr().out


@pytest.mark.parametrize(
    ('command', 'exit_var', 'success_marker'),
    (
        (
            'python tools\\refresh_universe_prices.py',
            'PRICE_REFRESH_EXIT',
            'Universe price refresh done (exit=0)',
        ),
        (
            'python tools\\refresh_backtest_panels.py',
            'BACKTEST_PANELS_EXIT',
            'Refresh backtest panels done (exit=0)',
        ),
    ),
)
def test_critical_market_stage_failure_skips_downstream(
    command, exit_var, success_marker
):
    batch = BATCH_PATH.read_text(encoding='ascii')
    command_offset = batch.index(command)
    success_offset = batch.index(success_marker, command_offset)
    failure_block = batch[command_offset:success_offset]

    assert f'set {exit_var}=%ERRORLEVEL%' in failure_block
    assert f'if not "%{exit_var}%"=="0" (' in failure_block
    assert '[FAIL]' in failure_block
    assert 'goto skip_market_panels' in failure_block

    label_offset = batch.index('\n:skip_market_panels')
    assert label_offset > batch.index('python tools\\refresh_backtest_panels.py')


def test_run_scanner_batch_is_ascii_crlf_only():
    raw = BATCH_PATH.read_bytes()

    assert all(byte < 0x80 for byte in raw)
    without_crlf = raw.replace(b'\r\n', b'')
    assert b'\n' not in without_crlf
    assert b'\r' not in without_crlf
