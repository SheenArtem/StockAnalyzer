import re
from pathlib import Path

import pytest

from tools import verify_scan_stages as verifier


ROOT = Path(__file__).resolve().parent.parent
BATCH_PATH = ROOT / 'run_scanner.bat'

# 從 verifier 直接推導，**不再手抄**。
# 2026-08-02 code review：原本這裡是 REQUIRED_STAGES 的手抄副本，驗的其實是
# 「verifier 對自己的清單一致」。手抄的失效方式是雙向的：verifier 新增 stage 而這裡
# 沒跟上（此測試會失敗，還算安全），或 verifier 要求一個 bat 根本不會印的 marker
# （production 天天報失敗，測試卻綠燈，因為 log 是用同一份清單合成的）。
SUCCESS_MARKERS = tuple(
    re.sub(r'\\(.)', r'\1', pattern.removeprefix(r'\]')).strip()
    for _label, pattern in verifier.REQUIRED_STAGES
)

# 關鍵市場面板 stage（失敗會讓 goto skip_market_panels 跳掉整段）
MARKET_PANEL_SUCCESS_MARKERS = tuple(
    m for m in SUCCESS_MARKERS
    if m.startswith(('Universe price refresh', 'Refresh backtest panels'))
)


def _bat_log_markers():
    """bat 裡所有 `call :log "..."` 的字串，%VAR% 以 0 代入（成功路徑的樣子）。"""
    raw = BATCH_PATH.read_text(encoding='ascii')
    return [re.sub(r'%[A-Za-z0-9_]+%', '0', m)
            for m in re.findall(r'call :log "([^"]*)"', raw)]


def _run_verifier(monkeypatch, tmp_path, markers):
    log_path = tmp_path / 'scanner.log'
    log_path.write_text(
        ''.join(f'[2026-07-14T00:00:00] {marker}\n' for marker in markers),
        encoding='utf-8',
    )
    monkeypatch.setattr(verifier, 'LOG_PATH', log_path)
    return verifier.main()


def test_success_markers_were_derived_not_hand_copied():
    assert len(SUCCESS_MARKERS) == len(verifier.REQUIRED_STAGES)
    assert 'Scanner started' in SUCCESS_MARKERS
    assert 'Scanner finished (exit=0)' in SUCCESS_MARKERS
    assert len(MARKET_PANEL_SUCCESS_MARKERS) == 2


def test_verifier_accepts_complete_scanner_pipeline(monkeypatch, tmp_path):
    assert _run_verifier(monkeypatch, tmp_path, SUCCESS_MARKERS) == 0


@pytest.mark.parametrize('missing_marker', MARKET_PANEL_SUCCESS_MARKERS)
def test_verifier_rejects_each_missing_market_panel_stage(
    monkeypatch, tmp_path, capsys, missing_marker
):
    markers = [marker for marker in SUCCESS_MARKERS if marker != missing_marker]

    assert _run_verifier(monkeypatch, tmp_path, markers) == 1
    assert missing_marker.removesuffix(' (exit=0)') in capsys.readouterr().out


@pytest.mark.parametrize(('label', 'pattern'), verifier.REQUIRED_STAGES)
def test_every_required_stage_is_actually_emitted_by_the_bat(label, pattern):
    """verifier 不可要求 bat 根本不會印的 marker。

    這是手抄清單擋不住的那個方向：若 marker 打錯字或 stage 從 bat 移除，production
    會天天報缺 stage，而合成 log 的測試照樣綠燈。
    """
    candidates = [f'[2026-07-14T00:00:00] {m}' for m in _bat_log_markers()]
    rx = re.compile(pattern)
    assert any(rx.search(c) for c in candidates), (
        f'REQUIRED_STAGES 的 {label!r} (pattern={pattern!r}) 在 run_scanner.bat 的 '
        f'call :log 裡找不到對應字串')


def test_breadth_stage_is_covered_by_the_verifier():
    """TW breadth 失敗時 bat 只印 [WARN]，所以更需要後檢查盯著。

    2026-08-02 之前 REQUIRED_STAGES 沒有這個 marker：breadth 持續失敗不會被任何人
    發現，macro_dashboard 的 Market Breadth 會一直顯示舊資料。
    """
    assert any('TW breadth panel done' in label
               for label, _ in verifier.REQUIRED_STAGES)
    assert 'TW breadth panel done (exit=0)' in SUCCESS_MARKERS


def test_verifier_rejects_missing_breadth_stage(monkeypatch, tmp_path, capsys):
    markers = [m for m in SUCCESS_MARKERS if 'TW breadth' not in m]

    assert _run_verifier(monkeypatch, tmp_path, markers) == 1
    assert 'TW breadth panel done' in capsys.readouterr().out


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
