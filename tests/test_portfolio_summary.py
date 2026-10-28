"""投資組合彙總卡片與側邊欄預設模式測試。

1. `_render_summary` 必須在「總市值」旁邊顯示「總成本」，且數值對齊 `summarize()['cost']`。
2. `app.py` 側邊欄功能模式以 `portfolio` 為第一個與預設選項，且不含已下架的 `notes` / `curation`。
"""
import ast
from pathlib import Path

import portfolio_store as ps
import portfolio_view as pv


class _FakeCol:
    def __init__(self, metrics, captions):
        self._metrics = metrics
        self._captions = captions

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def markdown(self, text):
        pass

    def metric(self, label, value, delta=None, delta_color="normal"):
        self._metrics.append((label, value, delta, delta_color))

    def caption(self, text):
        self._captions.append(text)


def test_render_summary_shows_total_cost_next_to_market_value(monkeypatch):
    """總成本緊鄰總市值顯示，且金額等於未平倉 cost_basis 總和（含買進手續費）。"""
    txns = [
        dict(id='a', ticker='2330', market='tw', action='buy', date='2026-06-10',
             shares=1000, price=900.0, fee=1282.0, tax=0.0, created_at='t1'),
        dict(id='b', ticker='2317', market='tw', action='buy', date='2026-06-11',
             shares=2000, price=200.0, fee=570.0, tax=0.0, created_at='t2'),
    ]
    holdings = ps.derive_holdings(txns)
    by_market, _ = ps.summarize(holdings, {'2330': 950.0, '2317': 210.0})

    recorded_metrics = []
    recorded_captions = []

    def fake_columns(n):
        count = n if isinstance(n, int) else len(n)
        return [_FakeCol(recorded_metrics, recorded_captions) for _ in range(count)]

    monkeypatch.setattr(pv.st, 'columns', fake_columns)

    pv._render_summary(by_market, {'tw': 12_000.0})

    labels = [m[0] for m in recorded_metrics]
    values = dict((m[0], m[1]) for m in recorded_metrics)

    assert labels[:3] == ['總市值', '總成本', '未實現損益'], f'首列指標順序非預期：{labels}'
    assert labels[3:] == ['當日損益', '已實現損益'], f'次列指標順序非預期：{labels}'
    # 市值 = 1000*950 + 2000*210 = 1,370,000；成本 = 901,282 + 400,570 = 1,301,852；未實現 = +68,148
    assert values['總市值'] == '1,370,000.00'
    assert values['總成本'] == '1,301,852.00'
    assert values['未實現損益'] == '+68,148.00'


def test_app_sidebar_portfolio_is_first_and_default_without_notes_or_curation():
    """靜態檢查 app.py 側邊欄選項順序、預設模式與已移除項目。"""
    src = (Path(__file__).resolve().parent.parent / 'app.py').read_text(encoding='utf-8')
    tree = ast.parse(src)

    assigns = {}
    default_mode = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in ('_mode_options', '_mode_idx_map'):
                assigns[name] = ast.literal_eval(node.value)
            elif name == '_current_mode' and isinstance(node.value, ast.Call):
                if len(node.value.args) == 2 and ast.literal_eval(node.value.args[0]) == 'app_mode':
                    default_mode = ast.literal_eval(node.value.args[1])

    opts = assigns['_mode_options']
    idx_map = assigns['_mode_idx_map']

    assert opts[0] == 'portfolio', f'投資組合必須在最前面：{opts}'
    assert 'notes' not in opts and 'curation' not in opts, f'知識庫與題材策展必須從 UI 下架：{opts}'
    assert default_mode == 'portfolio', f'預設 app_mode 必須為 portfolio：{default_mode}'
    assert idx_map.get('portfolio') == 0
    assert idx_map.get('analysis') == opts.index('individual')
