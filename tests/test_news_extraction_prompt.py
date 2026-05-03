"""Unit tests for build_extraction_prompt — schema-bump regression防呆.

歷史教訓 (2026-05-02 → 5/3 silent fail 兩晚):
- commit 58c4eab (#3 target_prices) 在 prompt template 內放 literal
  `{"broker": str, "price": float}` 沒 escape 成 `{{...}}`，f-string parser
  把它當 format spec 解析直接 ValueError，cron 連兩晚 exit=1
- 修在 commit 71be8b7

這份 test 只要 import + 跑空 batch + 1 篇 article batch，就會 catch 任何
未來 prompt schema bump 引入的 f-string 語法錯誤 (任何 unescaped `{...}`
literal、未綁定變數、type spec 衝突等)。
"""
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / 'tools'))

from news_theme_extract import (  # noqa: E402
    build_extraction_prompt, normalize_theme_key, pick_canonical_theme,
)


class TestNormalizeThemeKey:
    """Phase 1+ theme variant normalization."""

    def test_strips_internal_whitespace(self):
        assert (normalize_theme_key('ABF 載板') ==
                normalize_theme_key('ABF載板') == 'abf載板')

    def test_lowercase_english(self):
        assert (normalize_theme_key('CoWoS 先進封裝') ==
                normalize_theme_key('cowos 先進封裝') ==
                normalize_theme_key('CoWoS先進封裝') == 'cowos先進封裝')

    def test_multiple_spaces_collapsed(self):
        assert normalize_theme_key('AI    雲端') == 'ai雲端'

    def test_empty_returns_empty(self):
        assert normalize_theme_key('') == ''
        assert normalize_theme_key(None) == ''


class TestFetchUdnHtmlBody:
    """Phase 1+ I: UDN HTML body fetcher (網路依賴, 用 mock)."""

    def test_non_udn_url_returns_summary(self):
        from news_theme_extract import _fetch_udn_html_body
        body, status = _fetch_udn_html_body('https://example.com/x')
        assert body == '' and status == 'summary_only'

    def test_empty_url_returns_summary(self):
        from news_theme_extract import _fetch_udn_html_body
        body, status = _fetch_udn_html_body('')
        assert body == '' and status == 'summary_only'

    def test_selector_priority(self, monkeypatch):
        """Mock UDN HTML 含 article-body__editor 應優先取."""
        import news_theme_extract as m

        class MockResp:
            status_code = 200
            text = (
                '<html><body>'
                '<section class="article-body__editor">'
                + ('文章內容很長很長很長很長。' * 20) +  # > 200 chars
                '</section>'
                '<div class="article-body">should not match</div>'
                '</body></html>'
            )
            def raise_for_status(self): pass

        monkeypatch.setattr(m.requests, 'get', lambda *a, **kw: MockResp())
        body, status = m._fetch_udn_html_body('https://money.udn.com/x')
        assert status == 'udn_html'
        assert '文章內容' in body
        assert 'should not match' not in body

    def test_no_selector_match_falls_back(self, monkeypatch):
        import news_theme_extract as m

        class MockResp:
            status_code = 200
            text = '<html><body><div>tiny content</div></body></html>'
            def raise_for_status(self): pass

        monkeypatch.setattr(m.requests, 'get', lambda *a, **kw: MockResp())
        body, status = m._fetch_udn_html_body('https://money.udn.com/x')
        # tiny content < UDN_HTML_MIN_BODY_FROM_RSS=200 → fallback
        assert status == 'summary_only'

    def test_get_failure_falls_back(self, monkeypatch):
        import news_theme_extract as m

        def raise_err(*a, **kw):
            raise m.requests.exceptions.Timeout('mock timeout')

        monkeypatch.setattr(m.requests, 'get', raise_err)
        body, status = m._fetch_udn_html_body('https://money.udn.com/x')
        assert body == '' and status == 'summary_only'


class TestPickCanonicalTheme:
    def test_most_common_wins(self):
        # ABF 載板 出現 3 次, ABF載板 1 次 → 前者勝
        assert pick_canonical_theme(['ABF 載板', 'ABF 載板', 'ABF 載板',
                                     'ABF載板']) == 'ABF 載板'

    def test_tie_keeps_first(self):
        assert pick_canonical_theme(['A', 'B', 'A', 'B']) == 'A'

    def test_empty_returns_empty(self):
        assert pick_canonical_theme([]) == ''
        assert pick_canonical_theme(['', None, '']) == ''


class TestBuildExtractionPromptSyntax:
    """f-string template 不能爆炸 — 任何 input shape 都要回 str。"""

    def test_empty_batch_returns_str(self):
        """空 batch 不能 ValueError；回空-articles prompt 字串。"""
        result = build_extraction_prompt([])
        assert isinstance(result, str)
        assert len(result) > 100

    def test_single_article_batch(self):
        """1 篇 article batch 應 render 出 article block + system 規範."""
        article = {
            'query': '台積電',
            'date': '2026-05-03',
            'source': 'udn',
            'title': '台積電 Q1 EPS 上修法人喊買進目標價 1500',
            'summary': '法人會後升評，瑞銀目標價上看 1500。' * 5,
        }
        result = build_extraction_prompt([article])
        assert isinstance(result, str)
        assert '台積電' in result
        assert 'Article 1' in result

    def test_multi_article_batch(self):
        """多篇 batch 應全部 render，順序 1..N。"""
        articles = [
            {
                'query': f'q{i}',
                'date': '2026-05-03',
                'source': 'cnyes',
                'title': f'測試新聞 {i}',
                'summary': '內容' * 30,
            }
            for i in range(1, 6)
        ]
        result = build_extraction_prompt(articles)
        for i in range(1, 6):
            assert f'Article {i}' in result

    def test_no_unescaped_braces_in_fstring(self):
        """Regression for 71be8b7: prompt 應包含 literal {{...}} 範例（escape 後）.

        若有人未來改 prompt 又忘記 escape `{...}` JSON 範例，會在
        build_extraction_prompt() invocation 直接 ValueError。本測試
        catch invocation 是否成功 — 若 raise 直接 fail。
        """
        # 多種邊界 input 都不能爆
        inputs = [
            [],
            [{'query': '', 'date': '', 'source': '', 'title': '', 'summary': ''}],
            [{'query': 'x', 'date': '2026-01-01', 'source': 's', 'title': 't', 'summary': ''}] * 20,
        ]
        for batch in inputs:
            try:
                result = build_extraction_prompt(batch)
                assert isinstance(result, str)
            except ValueError as e:
                pytest.fail(
                    f"build_extraction_prompt() raised ValueError on batch "
                    f"size={len(batch)}: {e}. 可能是 prompt template 有未 "
                    f"escape 的 {{...}} literal — 範例 dict 須寫成 {{{{...}}}}."
                )

    def test_target_prices_example_is_escaped(self):
        """Regression for 5/2 bug: target_prices JSON 範例必須是 escaped 雙括號."""
        result = build_extraction_prompt([])
        # escape 後的 {{}} 在 f-string output 變成 single {}，所以最終 prompt
        # 應該看到 literal {"broker"...} 字串
        assert '{"broker"' in result, (
            "target_prices dict 範例消失了 — 可能是 #3 schema 被人移除。"
            "若刻意移除請更新此 test。"
        )

    def test_material_event_type_categories(self):
        """Regression for #6: 6 類 material_event_type 必須在 prompt 描述中."""
        result = build_extraction_prompt([])
        for cat in ('merger', 'buyback', 'lawsuit', 'capital_reduction',
                    'penalty', 'major_contract'):
            assert cat in result, (
                f"material_event_type '{cat}' 從 prompt 消失 — "
                f"#6 schema 被改了？若刻意請同步 rebuild_material_events 白名單"
            )
