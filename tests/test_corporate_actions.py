"""tools/build_corporate_actions.py 的 regression gate（不觸網）。

釘住四件事，它們是 Path B（係數用算的、不用存的）的核心：

1. `derive_factor` 的 fallback 選擇 —— FinMind 會把 `reference_price` 留成跟
   `before_price` 一樣（係數變 1.0 ＝ 完全沒調整），實測壞掉比率上市 2.3%／上櫃 7.5%。
   漏掉 fallback 不會報錯，只會讓那些股票的還原**靜默失效**。
2. `cumulative_factor` 的邊界 —— 係數只套用在事件日**之前**的日期。
   差一天就會讓事件當天的假跳空沒被消掉（或反而多消一次）。
   外加：整張表（多檔股票）餵進來必須直接擋掉，不能靜默把所有股票乘在一起。
3. `merge_events` 的持久化不變量 —— 既有列永遠獲勝、範圍過濾只碰新抓的列。
   這些是 contract 列名的保證，沒有測試釘住的話一次 refactor 就能靜默退化。
4. 官方減資表的解析 —— 欄名定位、已公告未生效列要丟掉、端點沒吃日期區間要吵。
"""
import argparse
import sys

import numpy as np
import pandas as pd
import pytest

from tools import build_corporate_actions as bca
from tools.build_corporate_actions import (
    COLS,
    FACTOR_MAX,
    FACTOR_MIN,
    SRC_FALLBACK,
    SRC_PAR_VALUE,
    SRC_REDUCTION,
    SRC_REFERENCE,
    _reduction_rows_to_events,
    _roc_to_iso,
    _TPEX_RED_FIELDS,
    _TWSE_RED_FIELDS,
    cumulative_factor,
    derive_factor,
    fetch_events,
    fetch_par_value_changes,
    fetch_reductions_tpex,
    fetch_reductions_twse,
    in_factor_range,
    merge_events,
)


class TestDeriveFactor:
    def test_uses_reference_price_when_populated(self):
        # 2330 2026-06-11 實際事件：1) 2255 -> 2248.99
        f, src = derive_factor(2255.0, 2248.99, 6.01)
        assert src == 'reference'
        assert f == pytest.approx(2248.99 / 2255.0)

    def test_falls_back_when_reference_equals_before(self):
        """6541 2018-07-10 的真實壞資料：參考價沒填，但股利欄有值。

        沒有 fallback 的話 factor 會是 1.0 —— 還原完全失效而且不會報錯。
        """
        f, src = derive_factor(95.20, 95.20, 1.053754)
        assert src == 'fallback'
        assert f == pytest.approx((95.20 - 1.053754) / 95.20)
        assert f < 1.0

    def test_no_fallback_when_nothing_was_distributed(self):
        """參考價等於前價、且沒有配發 -> 就是「係數 1」的正常事件，不可硬套 fallback。"""
        f, src = derive_factor(50.0, 50.0, 0.0)
        assert src == 'reference'
        assert f == pytest.approx(1.0)

    def test_fallback_when_reference_missing_entirely(self):
        f, src = derive_factor(100.0, np.nan, 2.5)
        assert src == 'fallback'
        assert f == pytest.approx(0.975)

    def test_rights_issue_uses_same_fallback(self):
        """現金增資也走同一條 fallback —— FinMind 把增資的價值調整編碼在同一欄。

        8054 2026-07-30 實測：官方公式 (79.0 + 0.02363562*77.00)/(1+0.02363562)
        = 78.953820，隱含扣減 0.046180，而 FinMind 給的正是 0.04618。
        """
        f, src = derive_factor(79.0, 79.0, 0.04618)
        official_ref = (79.0 + 0.02363562 * 77.00) / (1 + 0.02363562)
        assert src == 'fallback'
        assert f * 79.0 == pytest.approx(official_ref, abs=1e-3)

    def test_capital_reduction_gives_a_factor_above_one(self):
        """減資：參考價**高於**前價，同一條式子就得到 factor > 1。

        2327 國巨 2022-10-31（官方 TWTAUU）：268.00 -> 334.15。
        """
        f, src = derive_factor(268.00, 334.15, np.nan)
        assert src == 'reference'
        assert f == pytest.approx(334.15 / 268.00)
        assert f > 1.0

    # 官方 TWSE `TWT49U` 逐筆抄下來的三筆「權值+息值 < 0」事件（2026-08-10 實查）。
    # 認購價**高於**市價的現金增資，官方參考價比前一日收盤**高**，係數 > 1。
    NEGATIVE_VALUE_CUT = [
        ('1312', 13.20, 13.40, -0.209613),
        ('1735', 12.60, 12.73, -0.135448),
        ('1464', 9.47, 9.63, -0.164495),
    ]

    @pytest.mark.parametrize('sid,before,official_ref,value_cut', NEGATIVE_VALUE_CUT)
    def test_negative_value_cut_still_uses_the_fallback(self, sid, before,
                                                        official_ref, value_cut):
        """`value_cut` 是**可以為負**的，判準必須是 `!= 0` 不是 `> 0`（審查 F1）。

        用 `vc > 0` 的話這三筆會靜默落回 `reference` / factor 1.0：
        既沒還原（除權日留著 +1.0~1.7% 的機械跳空），又假稱用了真實參考價，
        連事後拿 `source` 欄稽核都找不到。實測這三筆確實是這樣被寫進表裡的。
        """
        f, src = derive_factor(before, before, value_cut)   # FinMind 沒填參考價
        assert src == 'fallback'
        assert f > 1.0
        assert f == pytest.approx((before - value_cut) / before)
        # 還原出來的參考價要對上官方，容差一個檔位
        assert before * f == pytest.approx(official_ref, abs=0.011)

    def test_negative_value_cut_without_any_reference_price_also_falls_back(self):
        """第二條 fallback 分支（參考價整個缺）同樣不可以用 `> 0` 卡掉負值。"""
        f, src = derive_factor(13.20, np.nan, -0.209613)
        assert src == 'fallback'
        assert f == pytest.approx((13.20 + 0.209613) / 13.20)
        assert f > 1.0

    @pytest.mark.parametrize('bp', [0, -1, np.nan])
    def test_unusable_without_a_valid_before_price(self, bp):
        f, src = derive_factor(bp, 10.0, 1.0)
        assert src == 'unusable'
        assert np.isnan(f)

    def test_unusable_when_reference_and_value_both_missing(self):
        f, src = derive_factor(100.0, np.nan, np.nan)
        assert src == 'unusable'
        assert np.isnan(f)


def _events(rows):
    return pd.DataFrame(rows, columns=['stock_id', 'date', 'factor'])


class TestCumulativeFactor:
    dates = pd.to_datetime(['2026-01-05', '2026-01-06', '2026-01-07', '2026-01-08'])

    def test_factor_applies_strictly_before_the_ex_date(self):
        """事件當天**不**套自己的係數 —— 當天價格已經是除權／減資後的。"""
        ev = _events([('X', pd.Timestamp('2026-01-07'), 0.95)])
        f = cumulative_factor(self.dates, ev)
        assert f.loc[pd.Timestamp('2026-01-05')] == pytest.approx(0.95)
        assert f.loc[pd.Timestamp('2026-01-06')] == pytest.approx(0.95)
        assert f.loc[pd.Timestamp('2026-01-07')] == pytest.approx(1.0)
        assert f.loc[pd.Timestamp('2026-01-08')] == pytest.approx(1.0)

    def test_multiple_events_compound(self):
        ev = _events([('X', pd.Timestamp('2026-01-06'), 0.9),
                      ('X', pd.Timestamp('2026-01-08'), 0.8)])
        f = cumulative_factor(self.dates, ev)
        assert f.loc[pd.Timestamp('2026-01-05')] == pytest.approx(0.9 * 0.8)
        assert f.loc[pd.Timestamp('2026-01-07')] == pytest.approx(0.8)
        assert f.loc[pd.Timestamp('2026-01-08')] == pytest.approx(1.0)

    def test_dividend_only_series_ends_at_one_and_is_non_decreasing(self):
        """**只有除權息事件時**的定義性質：往回推只會變小，最後一天永遠是 1.0。

        這正是 2026-08-10 用來判定既有 `Adj Close` 壞掉的判據
        （772 檔的係數中途往下掉）。
        ⚠️ 單調性**只在 factor <= 1 的事件下成立** —— 減資是 factor > 1，
        見 `test_capital_reduction_makes_the_series_decrease`。
        """
        ev = _events([('X', pd.Timestamp('2026-01-06'), 0.9),
                      ('X', pd.Timestamp('2026-01-08'), 0.8)])
        f = cumulative_factor(self.dates, ev)
        assert f.iloc[-1] == pytest.approx(1.0)
        assert (f.diff().dropna() >= -1e-12).all()

    def test_capital_reduction_makes_the_series_decrease(self):
        """減資（factor > 1）讓係數序列**往回遞增**，也就是隨時間遞減。

        contract 那條「factor 序列 non-decreasing」只對純除權息成立；
        合法的減資事件存在時它必然不成立，只有「最後一天 = 1.0」是恆真的。
        """
        ev = _events([('X', pd.Timestamp('2026-01-07'), 334.15 / 268.00)])
        f = cumulative_factor(self.dates, ev)
        assert f.iloc[-1] == pytest.approx(1.0)
        assert f.loc[pd.Timestamp('2026-01-05')] > 1.0
        assert (f.diff().dropna() <= 1e-12).all()      # 遞減，不是遞增

    def test_removes_the_ex_date_return_artifact(self):
        """端到端：原始價在除權息當天有假跌，還原後應該被消掉。

        對照 2026-08-10 的實測（37 檔／500 個觀測）：
        原始價當天平均 -4.61%，還原後 +0.46%。
        """
        raw = pd.Series([100.0, 100.0, 95.0, 95.0], index=self.dates)
        ev = _events([('X', pd.Timestamp('2026-01-07'), 0.95)])
        adj = raw * cumulative_factor(self.dates, ev)
        raw_ret = raw.pct_change().loc[pd.Timestamp('2026-01-07')]
        adj_ret = adj.pct_change().loc[pd.Timestamp('2026-01-07')]
        assert raw_ret == pytest.approx(-0.05)
        assert adj_ret == pytest.approx(0.0, abs=1e-12)

    def test_removes_the_capital_reduction_gap(self):
        """端到端（F1）：減資日的假**漲**也要被消掉。

        全部是 2327 國巨的真實數字 —— 價格取自 FinMind 官方原始價，
        事件取自官方 TWTAUU（268.00 -> 334.15，減資停止買賣 10/20~10/28）：

            原始價 10/19 收 268.0 -> 10/31 收 367.0 ＝ **+36.94%**
            其中 +24.68% 純粹是減資造成的機械跳空

        還原後那 24.68% 必須消失，剩下的 +9.83% 是停牌 8 天期間的真實漲幅。
        """
        idx = pd.to_datetime(['2022-10-14', '2022-10-17', '2022-10-18',
                              '2022-10-19', '2022-10-31', '2022-11-01'])
        raw = pd.Series([277.5, 278.5, 278.5, 268.0, 367.0, 379.0], index=idx)
        ev = _events([('2327', pd.Timestamp('2022-10-31'), 334.15 / 268.00)])
        fac = cumulative_factor(idx, ev)
        adj = raw * fac

        # 減資前的價格被抬到「減資後股數」的計價基準上
        assert adj.loc[pd.Timestamp('2022-10-19')] == pytest.approx(334.15)
        assert fac.loc[pd.Timestamp('2022-10-31')] == pytest.approx(1.0)

        red_day = pd.Timestamp('2022-10-31')
        raw_ret = raw.pct_change().loc[red_day]
        adj_ret = adj.pct_change().loc[red_day]
        assert raw_ret == pytest.approx(367.0 / 268.0 - 1)       # +36.94%
        assert raw_ret > 0.35
        assert adj_ret == pytest.approx(367.0 / 334.15 - 1)      # +9.83%
        assert adj_ret < 0.11
        # 還原序列在減資日跨得過去：不再有 20% 以上的機械跳空
        assert (adj.pct_change().dropna().abs() < 0.11).all()

    def test_out_of_range_factors_are_ignored(self):
        """一筆爛事件不可以毀掉整段歷史的累乘結果。"""
        ev = _events([('X', pd.Timestamp('2026-01-07'), FACTOR_MAX * 10),
                      ('X', pd.Timestamp('2026-01-07'), FACTOR_MIN / 10),
                      ('X', pd.Timestamp('2026-01-07'), np.nan)])
        f = cumulative_factor(self.dates, ev)
        assert (f == 1.0).all()

    def test_boundary_factors_agree_between_the_writer_and_the_reader(self):
        """寫入端與讀取端必須共用同一個範圍判準（審查 F8）。

        原本寫入端用 `between()`（含端點）、讀取端用 `MIN < f < MAX`（不含），
        於是係數**正好**等於邊界的事件會被存進表裡、卻在算累積係數時被靜默略過。
        「存了但不生效」是最難事後察覺的一種不一致。
        """
        for bound in (FACTOR_MIN, FACTOR_MAX):
            assert in_factor_range(bound)
            written, _ = merge_events(_frame([]),
                                      _frame([_row('X', '2026-01-07', bound)]))
            applied = cumulative_factor(
                self.dates, _events([('X', pd.Timestamp('2026-01-07'), bound)])
            ).loc[pd.Timestamp('2026-01-05')]
            assert len(written) == 1, '寫入端收了 %s' % bound
            assert applied == pytest.approx(bound), '讀取端卻沒套用 %s' % bound

    def test_no_events_is_identity(self):
        f = cumulative_factor(self.dates, pd.DataFrame(columns=['stock_id', 'date', 'factor']))
        assert (f == 1.0).all()
        assert cumulative_factor(self.dates, None).eq(1.0).all()

    def test_rejects_events_from_more_than_one_stock(self):
        """整張事件表餵進來 = 把所有股票的係數乘在一起，而且不會有任何跡象。

        最容易誤用的輸入正是本工具自己的輸出檔，所以這裡要 raise 而不是回一個
        看起來很正常的序列。
        """
        ev = _events([('X', pd.Timestamp('2026-01-06'), 0.9),
                      ('Y', pd.Timestamp('2026-01-07'), 0.8)])
        with pytest.raises(ValueError, match='ONE stock'):
            cumulative_factor(self.dates, ev)

    def test_single_stock_frame_is_accepted(self):
        """選擇性：同一檔股票的多筆事件不受 guard 影響。"""
        ev = _events([('X', pd.Timestamp('2026-01-06'), 0.9),
                      ('X', pd.Timestamp('2026-01-07'), 0.8)])
        assert cumulative_factor(self.dates, ev).iloc[-1] == pytest.approx(1.0)


def _row(stock_id, date, factor, source='reference', before=100.0, reference=95.0,
         value_cut=np.nan):
    return {'stock_id': stock_id, 'date': pd.Timestamp(date), 'before_price': before,
            'reference_price': reference, 'value_cut': value_cut, 'factor': factor,
            'source': source}


def _frame(rows):
    return pd.DataFrame(rows, columns=COLS)


class TestMergeEvents:
    """持久化不變量：既有列永遠獲勝、範圍過濾只碰新抓的列。

    負向對照（2026-08-10 實跑）：把 `keep='first'` 改成 `keep='last'`，
    `test_duplicate_key_keeps_the_existing_row` 如預期變紅，其餘維持綠。
    """

    def test_duplicate_key_keeps_the_existing_row(self):
        existing = _frame([_row('2330', '2026-06-11', 0.95, before=2255.0)])
        fetched = _frame([_row('2330', '2026-06-11', 0.80, before=999.0,
                               source='fallback')])
        merged, stats = merge_events(existing, fetched)
        assert len(merged) == 1
        assert merged.iloc[0]['factor'] == pytest.approx(0.95)
        assert merged.iloc[0]['before_price'] == pytest.approx(2255.0)
        assert merged.iloc[0]['source'] == 'reference'
        assert stats['added'] == 0

    def test_new_keys_are_appended(self):
        existing = _frame([_row('2330', '2026-06-11', 0.95)])
        fetched = _frame([_row('2330', '2025-06-12', 0.97),        # 同股票新日期
                          _row('2327', '2022-10-31', 1.2468,
                               source=SRC_REDUCTION)])            # 新股票
        merged, stats = merge_events(existing, fetched)
        assert len(merged) == 3
        assert stats['added'] == 2
        assert set(merged['stock_id']) == {'2327', '2330'}
        # 排序保證：stock_id 再 date
        assert list(merged['date'].dt.strftime('%Y-%m-%d')) == [
            '2022-10-31', '2025-06-12', '2026-06-11']

    def test_out_of_range_factor_only_filters_fetched_rows(self):
        """既有列就算超出範圍也不動它 —— 這個表是 append-only 的。"""
        existing = _frame([_row('1111', '2020-01-02', FACTOR_MAX * 100)])
        fetched = _frame([_row('2222', '2020-01-02', FACTOR_MAX * 10),
                          _row('3333', '2020-01-02', FACTOR_MIN / 10),
                          _row('4444', '2020-01-02', 1.2468, source=SRC_REDUCTION),
                          _row('5555', '2020-01-02', 0.1, source=SRC_PAR_VALUE)])
        merged, stats = merge_events(existing, fetched)
        assert stats['dropped_out_of_range'] == 2
        assert set(merged['stock_id']) == {'1111', '4444', '5555'}
        assert merged.loc[merged.stock_id == '1111', 'factor'].iloc[0] > FACTOR_MAX

    def test_source_collision_is_reported_not_silently_resolved(self):
        existing = _frame([_row('2323', '2022-10-24', 0.9847)])
        fetched = _frame([_row('2323', '2022-10-24', 0.9495, source=SRC_REDUCTION)])
        merged, stats = merge_events(existing, fetched)
        assert stats['collisions'] == 1
        assert stats['collision_keys'] and '2323' in stats['collision_keys'][0]
        assert merged.iloc[0]['source'] == 'reference'    # 既有列仍然獲勝

    def test_factor_bounds_admit_the_real_extremes(self):
        """範圍邊界不是憑直覺挑的 —— 實測到的合法極值必須進得來。

        0.100 = 面額變更 10 元改 1 元（`6548` 2019-09-09，312 -> 31.2）
        40.03 = 彌補虧損減資（`5364` 2013-12-10，1.15 -> 46.04）
        原本 dividends-only 的 0.2~5.0 會把這兩類全部剔除掉。
        """
        assert FACTOR_MIN < 31.2 / 312.0 < FACTOR_MAX
        assert FACTOR_MIN < 46.04 / 1.15 < FACTOR_MAX

    def test_nan_factor_existing_row_loses_to_a_usable_fetched_factor(self):
        """NaN 係數不帶資訊，卻會用 `keep='first'` **永久**佔住那個 key（審查 F4）。

        `--refetch-all` 依設計無法覆寫既有列 ＝ 一旦寫進去就沒有任何修復路徑。
        所以這是「既有列永遠獲勝」唯一的例外。
        """
        existing = _frame([_row('2330', '2026-06-11', np.nan, source='unusable',
                                before=np.nan)])
        fetched = _frame([_row('2330', '2026-06-11', 0.95, before=2255.0)])
        merged, stats = merge_events(existing, fetched)
        assert len(merged) == 1
        assert merged.iloc[0]['factor'] == pytest.approx(0.95)
        assert merged.iloc[0]['source'] == 'reference'
        assert stats['repaired_nan'] == 1
        assert stats['added'] == 0
        # 修復不是碰撞，不該混進碰撞警告
        assert stats['collisions'] == 0

    def test_a_usable_existing_factor_still_wins(self):
        """選擇性：讓位規則**只**適用於 NaN，不可以順手把 append-only 破壞掉。"""
        existing = _frame([_row('2330', '2026-06-11', 0.95, before=2255.0)])
        fetched = _frame([_row('2330', '2026-06-11', 0.80, before=999.0)])
        merged, stats = merge_events(existing, fetched)
        assert merged.iloc[0]['factor'] == pytest.approx(0.95)
        assert merged.iloc[0]['before_price'] == pytest.approx(2255.0)
        assert stats['repaired_nan'] == 0

    def test_both_nan_keeps_the_existing_row(self):
        existing = _frame([_row('X', '2020-01-02', np.nan, source='unusable')])
        fetched = _frame([_row('X', '2020-01-02', np.nan, source='fallback')])
        merged, stats = merge_events(existing, fetched)
        assert len(merged) == 1
        assert merged.iloc[0]['source'] == 'unusable'
        assert stats['repaired_nan'] == 0

    def test_same_source_duplicate_with_a_different_factor_is_reported(self):
        """同 source 的數值分歧也要出聲（審查 F3）。

        原本碰撞偵測只比 `source`，所以官方減資表自己重複、兩列價格不同時
        （實測 `3536` 104/03/20）完全靜默 —— 由 payload 順序決定答案。
        """
        existing = _frame([_row('3536', '2015-03-20', 2.02584, source=SRC_REDUCTION)])
        fetched = _frame([_row('3536', '2015-03-20', 1.98480, source=SRC_REDUCTION)])
        merged, stats = merge_events(existing, fetched)
        assert stats['collisions'] == 1
        assert '3536' in stats['collision_keys'][0]
        assert merged.iloc[0]['factor'] == pytest.approx(2.02584)   # 既有列仍獲勝

    def test_identical_same_source_duplicates_are_not_reported(self):
        """選擇性：真的一模一樣就不要吵，否則每次重跑市場層都會刷警告。"""
        existing = _frame([_row('2327', '2022-10-31', 1.2468, source=SRC_REDUCTION)])
        fetched = _frame([_row('2327', '2022-10-31', 1.2468, source=SRC_REDUCTION)])
        merged, stats = merge_events(existing, fetched)
        assert stats['collisions'] == 0
        assert len(merged) == 1

    def test_empty_inputs_are_safe(self):
        assert len(merge_events(_frame([]), _frame([]))[0]) == 0
        merged, stats = merge_events(_frame([]), _frame([_row('X', '2020-01-02', 0.9)]))
        assert len(merged) == 1 and stats['added'] == 1
        merged, stats = merge_events(_frame([_row('X', '2020-01-02', 0.9)]), _frame([]))
        assert len(merged) == 1 and stats['added'] == 0


class TestRocDate:
    def test_twse_slash_format(self):
        assert _roc_to_iso('111/10/31') == pd.Timestamp('2022-10-31')

    def test_tpex_compact_format(self):
        assert _roc_to_iso('1150630') == pd.Timestamp('2026-06-30')
        assert _roc_to_iso('1020116') == pd.Timestamp('2013-01-16')

    def test_twse_exright_cjk_format(self):
        """`TWT49U` 用的是第三種寫法，跟同機構的減資表**不一樣**。

        沒有這一支的話，`115年08月03日` 會掉進緊湊分支被切成
        `115年08`/`月0`/`3日` 而拋 ValueError —— 增量模式整個抓不到東西。
        """
        assert _roc_to_iso('115年08月03日') == pd.Timestamp('2026-08-03')
        assert _roc_to_iso('115年8月3日') == pd.Timestamp('2026-08-03')
        assert _roc_to_iso('99年12月31日') == pd.Timestamp('2010-12-31')

    def test_all_three_formats_agree_on_the_same_day(self):
        """三種寫法指同一天時必須解出同一個 Timestamp（格式差異不可影響結果）。"""
        assert (_roc_to_iso('115/08/03') == _roc_to_iso('1150803')
                == _roc_to_iso('115年08月03日') == pd.Timestamp('2026-08-03'))


# 官方 payload 的真實形狀（欄位順序照抄實際回應）
TWSE_FIELDS = ['恢復買賣日期', '股票代號', '名稱', '停止買賣前收盤價格', '恢復買賣參考價',
               '漲停價格', '跌停價格', '開盤競價基準', '除權參考價', '減資原因', '詳細資料']
TPEX_FIELDS = ['恢復買賣日期', '股票代號', '名稱', '最後交易日之收盤價格',
               '減資恢復買賣開始日參考價格', '漲停價格', '跌停價格', '開始交易基準價',
               '除權參考價', '減資原因', '詳細資料']
# 2327 國巨 2022-10-31，逐格照抄 TWTAUU
TWSE_2327 = ['111/10/31', '2327', '國巨', '268.00', '334.15', '367.50', '301.00',
             '334.00', '--', '退還股款', '2327  ,20221019']
# 1563 巧新：已公告、還沒生效（價格欄全是 "-"）
TWSE_ANNOUNCED = ['115/09/07', '1563', '巧新', '-', '-', '-', '-', '-', '--',
                  '退還股款', '1563  ,20260807']
# 3312 弘憶股 2018-04-10：減資後同日再現金增資，除權參考價 8.34 才是真的起跳價
TWSE_EXRIGHT = ['107/04/10', '3312', '弘憶股', '6.54', '8.48', '9.32', '7.64',
                '8.48', '8.34', '彌補虧損', '3312  ,20180329']
# 3152 璟德 2026-06-30，逐格照抄 TPEX bulletin/revivt
TPEX_3152 = ['1150630', '3152', '璟德', '208.00', '360.24', '396.00', '324.50',
             '360.00', '0.00', '現金減資', '<table>...</table>']
# 3536 誠創 104/03/20：官方表**自己重複了三次**，而且第一列的「除權參考價」是 `--`、
# 另外兩列是 13.06（2026-08-10 直接向 TWTAUU 查證，逐格照抄）。
# 交給 payload 順序決定就會取到 13.33/6.58 = 2.02584，正確答案是 13.06/6.58 = 1.98480。
TWSE_3536_DUP = [
    ['104/03/20', '3536', '誠創', '6.58', '13.33', '14.25', '12.15', '13.35', '--',
     '彌補虧損', '3536  ,20150107'],
    ['104/03/20', '3536', '誠創', '6.58', '13.33', '14.25', '12.15', '13.35', '13.06',
     '彌補虧損', '3536  ,20150113'],
    ['104/03/20', '3536', '誠創', '6.58', '13.33', '14.25', '12.15', '13.35', '13.06',
     '彌補虧損', '3536  ,20150331'],
]
# 2408 南亞科 103/09/09：同樣重複，但只差在公司名稱與詳細資料欄，
# 本工具讀的 5 欄完全一致 ＝ 挑哪一列都對（無害的那種重複）。
TWSE_2408_DUP = [
    ['103/09/09', '2408', '南亞科', '8.10', '80.93', '86.50', '75.30', '80.90', '--',
     '彌補虧損', '2408  ,20140922'],
    ['103/09/09', '2408', '南科', '8.10', '80.93', '86.50', '75.30', '80.90', '--',
     '彌補虧損', '2408  ,20140820'],
]


class TestOfficialReductionParsing:
    def test_twse_row_becomes_a_factor_above_one(self):
        df = _reduction_rows_to_events(TWSE_FIELDS, [TWSE_2327], _TWSE_RED_FIELDS)
        assert len(df) == 1
        r = df.iloc[0]
        assert r['stock_id'] == '2327'
        assert r['date'] == pd.Timestamp('2022-10-31')
        assert r['before_price'] == pytest.approx(268.00)
        assert r['reference_price'] == pytest.approx(334.15)
        assert r['factor'] == pytest.approx(334.15 / 268.00)
        assert r['factor'] > 1.0
        assert r['source'] == SRC_REDUCTION

    def test_tpex_row_uses_its_own_field_names(self):
        """兩個市場欄名不同（`最後交易日之收盤價格` vs `停止買賣前收盤價格`），
        所以定位一定要用名字；用位置的話上櫃那張表會靜默錯位。"""
        df = _reduction_rows_to_events(TPEX_FIELDS, [TPEX_3152], _TPEX_RED_FIELDS)
        assert len(df) == 1
        assert df.iloc[0]['stock_id'] == '3152'
        assert df.iloc[0]['date'] == pd.Timestamp('2026-06-30')
        assert df.iloc[0]['factor'] == pytest.approx(360.24 / 208.00)

    def test_announced_but_not_effective_rows_are_dropped(self):
        """已公告未生效的列**不可入表** —— 它會用 keep='first' 把日後抓到的
        正確價格永遠擋在門外。"""
        df = _reduction_rows_to_events(TWSE_FIELDS, [TWSE_2327, TWSE_ANNOUNCED],
                                       _TWSE_RED_FIELDS)
        assert list(df['stock_id']) == ['2327']

    def test_exright_reference_price_wins_when_present(self):
        """減資後同日再現金增資：當天真正的起跳價是除權參考價。"""
        df = _reduction_rows_to_events(TWSE_FIELDS, [TWSE_EXRIGHT], _TWSE_RED_FIELDS)
        assert df.iloc[0]['reference_price'] == pytest.approx(8.34)
        assert df.iloc[0]['factor'] == pytest.approx(8.34 / 6.54)

    def test_reordered_fields_are_still_parsed_correctly(self):
        """欄位換順序仍要對 —— 這是「用名字定位而不是用位置」唯一的理由。

        兩個市場今天剛好共用同一個欄位順序，所以只比對上市／上櫃**驗不到**這件事：
        位置寫死的版本照樣全綠（2026-08-10 突變測試實測），要靠這個案例才擋得住。
        """
        order = [0, 3, 4, 1, 2, 5, 6, 7, 8, 9, 10]
        fields = [TWSE_FIELDS[i] for i in order]
        row = [TWSE_2327[i] for i in order]
        df = _reduction_rows_to_events(fields, [row], _TWSE_RED_FIELDS)
        assert df.iloc[0]['stock_id'] == '2327'
        assert df.iloc[0]['date'] == pd.Timestamp('2022-10-31')
        assert df.iloc[0]['before_price'] == pytest.approx(268.00)
        assert df.iloc[0]['factor'] == pytest.approx(334.15 / 268.00)

    def test_duplicate_official_rows_prefer_the_one_carrying_an_exright_price(self):
        """官方表自己重複、而且不一致時，不可以讓 payload 順序決定答案（審查 F3）。

        `3536` 104/03/20 出現三次，第一列沒有除權參考價、另兩列有 13.06。
        `drop_duplicates(keep='first')` 會取 13.33/6.58 = 2.02584 ——
        比正確值高 2.07%，而且表是 append-only 的，**寫進去就修不掉**。
        """
        df = _reduction_rows_to_events(TWSE_FIELDS, TWSE_3536_DUP, _TWSE_RED_FIELDS)
        assert len(df) == 1
        assert df.iloc[0]['reference_price'] == pytest.approx(13.06)
        assert df.iloc[0]['factor'] == pytest.approx(13.06 / 6.58)
        assert df.iloc[0]['factor'] != pytest.approx(13.33 / 6.58)

    def test_duplicate_official_rows_survive_in_any_payload_order(self):
        """答案不可以隨官方列的順序改變 —— 把有除權參考價的那列排到最後也一樣。"""
        for rows in (TWSE_3536_DUP, list(reversed(TWSE_3536_DUP))):
            df = _reduction_rows_to_events(TWSE_FIELDS, rows, _TWSE_RED_FIELDS)
            assert len(df) == 1
            assert df.iloc[0]['factor'] == pytest.approx(13.06 / 6.58)

    def test_harmless_duplicate_official_rows_collapse_to_one(self):
        """選擇性：`2408` 兩列只差公司名稱，讀的欄位一致，收斂成一列即可。"""
        df = _reduction_rows_to_events(TWSE_FIELDS, TWSE_2408_DUP, _TWSE_RED_FIELDS)
        assert len(df) == 1
        assert df.iloc[0]['factor'] == pytest.approx(80.93 / 8.10)

    def test_distinct_keys_are_not_collapsed(self):
        """選擇性：去重只能碰同一個 (代號, 日期)，不可以順手吃掉別的事件。"""
        df = _reduction_rows_to_events(TWSE_FIELDS,
                                       TWSE_3536_DUP + [TWSE_2327, TWSE_EXRIGHT],
                                       _TWSE_RED_FIELDS)
        assert sorted(df['stock_id']) == ['2327', '3312', '3536']

    def test_missing_field_raises_instead_of_misaligning(self):
        broken = [f for f in TWSE_FIELDS if f != '恢復買賣參考價']
        with pytest.raises(RuntimeError, match='欄位不見了'):
            _reduction_rows_to_events(broken, [TWSE_2327[:4] + TWSE_2327[5:]],
                                      _TWSE_RED_FIELDS)


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return _FakeResp(self.payload)


class TestOfficialFetchGuards:
    """端點必須自報它真的用了我們給的區間。

    這不是假想風險：TPEX 的 `date=` 參數**完全被忽略**（不論給什麼都回今天的窗口），
    而 repo 早有同型事故（`stk_quote_result.php` 拿請求日期當資料日期，
    差點把「上一場」的 OHLCV 寫進 1,900+ 支 CSV）。
    """

    def test_twse_accepts_a_correctly_echoed_window(self):
        sess = _FakeSession({'stat': 'OK', 'strDate': '20110101', 'endDate': '20261231',
                             'fields': TWSE_FIELDS, 'data': [TWSE_2327]})
        df = fetch_reductions_twse(sess, '2011-01-01', '2026-12-31')
        assert len(df) == 1 and df.iloc[0]['stock_id'] == '2327'
        assert sess.calls[0][1]['startDate'] == '20110101'

    def test_twse_refuses_when_the_window_is_not_echoed(self):
        """實測到的失敗形狀：端點整個無視請求區間，兩個欄位都回今天。"""
        sess = _FakeSession({'stat': 'OK', 'strDate': '20260810', 'endDate': '20260811',
                             'fields': TWSE_FIELDS, 'data': [TWSE_2327]})
        with pytest.raises(RuntimeError, match='沒有採用請求的區間'):
            fetch_reductions_twse(sess, '2011-01-01', '2026-12-31')

    def test_twse_refuses_when_only_the_start_date_is_not_echoed(self):
        """兩個欄位要**各自**釘住（審查 F7）。

        上面那個案例兩邊都不符，所以只留其中一半的比對它照樣會過 ——
        實測把 `endDate` 那半停掉，39 個測試仍然全綠。真實的部分採用
        （端點把過早的起始日夾到它自己的下限）正是只有一邊不符。
        """
        sess = _FakeSession({'stat': 'OK', 'strDate': '20110125', 'endDate': '20261231',
                             'fields': TWSE_FIELDS, 'data': [TWSE_2327]})
        with pytest.raises(RuntimeError, match='沒有採用請求的區間'):
            fetch_reductions_twse(sess, '2011-01-01', '2026-12-31')

    def test_twse_refuses_when_only_the_end_date_is_not_echoed(self):
        sess = _FakeSession({'stat': 'OK', 'strDate': '20110101', 'endDate': '20260810',
                             'fields': TWSE_FIELDS, 'data': [TWSE_2327]})
        with pytest.raises(RuntimeError, match='沒有採用請求的區間'):
            fetch_reductions_twse(sess, '2011-01-01', '2026-12-31')

    def test_twse_refuses_when_stat_is_not_ok(self):
        sess = _FakeSession({'stat': '查詢開始日期小於100年1月1日，請重新查詢!'})
        with pytest.raises(RuntimeError, match='拒絕查詢'):
            fetch_reductions_twse(sess, '2011-01-01', '2026-12-31')

    def test_tpex_accepts_a_correctly_echoed_window(self):
        sess = _FakeSession({'stat': 'ok', 'date': '20130101~20261231',
                             'tables': [{'fields': TPEX_FIELDS, 'data': [TPEX_3152]}]})
        df = fetch_reductions_tpex(sess, '2013-01-01', '2026-12-31')
        assert len(df) == 1 and df.iloc[0]['stock_id'] == '3152'
        # 這個端點只認斜線格式
        assert sess.calls[0][1]['startDate'] == '2013/01/01'

    def test_tpex_refuses_the_today_only_window(self):
        """實測到的真實失敗模式：不論 `date` 給什麼，都回 `20260810~20260811`。"""
        sess = _FakeSession({'stat': 'ok', 'date': '20260810~20260811',
                             'tables': [{'fields': TPEX_FIELDS, 'data': []}]})
        with pytest.raises(RuntimeError, match='沒有採用請求的區間'):
            fetch_reductions_tpex(sess, '2013-01-01', '2026-12-31')


# --------------------------------------------------------------------------- #
#  main() 層：續跑語義、市場層涵蓋、exit code、既有表的處置
#
#  ⚠️ 這一段是 2026-08-10 round-2 審查 F6 補上的。在那之前 39 個測試**全部**只碰
#  四個純函式，main() 一行都沒被執行過 —— 而審查在 main() 裡找到的缺陷
#  （F2 死掉的 fail-loud 閘門、F5 市場層餓死逐檔層）正好就躺在那個盲區裡。
#  審查實測：把續跑判定改成計入所有 source、把市場層改用待辦清單過濾、
#  把 fail-loud 閘門改成恆真、把面額變更層整層改成回空 frame ——
#  這四種突變當時**全都留下 39 個綠燈**。
# --------------------------------------------------------------------------- #
class FinMindQuotaBlockedError(Exception):
    """本工具是用**例外名稱**分類額度錯誤的（`'Quota' in type(e).__name__`），
    所以測試用的替身必須叫這個名字才會走到那條分支。
    真品在 `cache_manager`，同名。"""


def _finmind_dividends(rows):
    """FinMind `taiwan_stock_dividend_result` 的形狀（只留本工具會讀的欄位）。"""
    return pd.DataFrame(rows, columns=['date', 'before_price', 'reference_price',
                                       'stock_and_cache_dividend'])


def _finmind_par_value(rows):
    """FinMind `taiwan_stock_split_price` 的形狀。"""
    return pd.DataFrame(rows, columns=['stock_id', 'date', 'before_price', 'after_price'])


class _FakeLoader:
    """假的 FinMind loader（只實作本工具用到的兩個 dataset），並記錄呼叫順序。"""

    def __init__(self, dividends=None, par_value=None, dividend_error=None,
                 error_from=1, par_value_error=None):
        self._dividends = dividends or {}
        self._par_value = par_value
        self._dividend_error = dividend_error
        self._error_from = error_from
        self._par_value_error = par_value_error
        self.asked = []
        self.sequence = []

    def taiwan_stock_dividend_result(self, stock_id, start_date):
        self.asked.append(stock_id)
        self.sequence.append('dividends:%s' % stock_id)
        if self._dividend_error is not None and len(self.asked) >= self._error_from:
            raise self._dividend_error
        return self._dividends.get(stock_id, pd.DataFrame())

    def taiwan_stock_split_price(self, start_date):
        self.sequence.append('par_value')
        if self._par_value_error is not None:
            raise self._par_value_error
        return self._par_value if self._par_value is not None else pd.DataFrame()


class _FakeOfficialSession:
    """兩個官方減資端點的替身；會把請求的區間照實回音，讓真正的守衛過關。"""

    def __init__(self, twse_rows=(), tpex_rows=()):
        self.twse_rows, self.tpex_rows = list(twse_rows), list(tpex_rows)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if 'twse.com.tw' in url:
            return _FakeResp({'stat': 'OK', 'strDate': params['startDate'],
                              'endDate': params['endDate'],
                              'fields': TWSE_FIELDS, 'data': self.twse_rows})
        return _FakeResp({'stat': 'ok',
                          'date': '%s~%s' % (params['startDate'].replace('/', ''),
                                             params['endDate'].replace('/', '')),
                          'tables': [{'fields': TPEX_FIELDS, 'data': self.tpex_rows}]})


class _Rig:
    """把 `main()` 接到暫存目錄 + 替身資料源上（不觸網、不碰真的 data_cache）。

    宇宙是照 `data_cache/*_price.csv` 長出來的，所以這裡真的建檔，
    讓 `_tw_cache_stems` 的真實邏輯照跑。
    """

    def __init__(self, tmp_path, monkeypatch):
        self.cache = tmp_path / 'data_cache'
        self.cache.mkdir()
        self.out = self.cache / 'backtest' / 'corporate_actions_tw.parquet'
        self.mp = monkeypatch
        monkeypatch.setattr(bca, 'CACHE_DIR', self.cache)
        monkeypatch.setattr(bca, 'OUT_PATH', self.out)

    def universe(self, *stems):
        for s in stems:
            (self.cache / ('%s_price.csv' % s)).write_text('Date,Close\n',
                                                           encoding='utf-8')

    def seed(self, rows):
        self.out.parent.mkdir(parents=True, exist_ok=True)
        _frame(rows).to_parquet(self.out, index=False)

    def run(self, loader, twse_rows=(), tpex_rows=(), argv=(), session=None):
        self.session = session or _FakeOfficialSession(twse_rows, tpex_rows)
        self.mp.setattr(bca, 'get_finmind_loader', lambda: loader)
        self.mp.setattr(bca, 'official_session', lambda: self.session)
        self.mp.setattr(sys, 'argv', ['build_corporate_actions.py'] + list(argv))
        return bca.main()

    def table(self):
        return pd.read_parquet(self.out)


@pytest.fixture
def rig(tmp_path, monkeypatch):
    return _Rig(tmp_path, monkeypatch)


_DIV_1101 = _finmind_dividends([('2021-07-01', 50.0, 48.0, 2.0)])
_DIV_1102 = _finmind_dividends([('2021-07-02', 30.0, 29.0, 1.0)])
_DIV_1103 = _finmind_dividends([('2021-07-03', 20.0, 19.5, 0.5)])


class TestMainResumeSemantics:
    """contract 不變量 4 —— round-1 唯一那個 P1 的修法，原本沒有任何測試釘住。"""

    def test_resume_skips_only_dividend_sourced_stocks(self, rig):
        """完成判定只能看除權息來源的列（`reference`/`fallback`/`unusable`）。

        `1102` 在表裡只有一筆**減資**列 —— 那是全市場層放進去的，不代表它的
        除權息抓過了。用「股票在不在表裡」當判據的話，它的除權息**永遠補不到**。
        """
        rig.universe('1101', '1102', '1103')
        rig.seed([_row('1101', '2020-06-01', 0.97, source=SRC_REFERENCE),
                  _row('1102', '2015-03-20', 1.9848, source=SRC_REDUCTION)])
        loader = _FakeLoader(dividends={'1102': _DIV_1102, '1103': _DIV_1103})
        assert rig.run(loader) == 0
        assert loader.asked == ['1102', '1103']

    def test_market_wide_layers_land_for_stocks_already_fetched(self, rig):
        """全市場層要用**完整 universe** 過濾，不是續跑後的待辦清單。

        否則已經抓過除權息的股票就永遠補不到減資 —— 這正是 round-1 的 P1。
        """
        rig.universe('1103', '2327')
        rig.seed([_row('2327', '2020-06-01', 0.97, source=SRC_REFERENCE)])
        loader = _FakeLoader(dividends={'1103': _DIV_1103})
        assert rig.run(loader, twse_rows=[TWSE_2327]) == 0
        assert loader.asked == ['1103']                     # 2327 的除權息被跳過
        t = rig.table()
        red = t[(t['stock_id'] == '2327') & (t['source'] == SRC_REDUCTION)]
        assert len(red) == 1                                # 但減資照樣補進去了
        assert red.iloc[0]['factor'] == pytest.approx(334.15 / 268.00)

    def test_official_rows_outside_our_universe_are_filtered_out(self, rig):
        """選擇性：官方表是全市場的，沒在追蹤的股票不可以混進來。"""
        rig.universe('1103')
        loader = _FakeLoader(dividends={'1103': _DIV_1103})
        assert rig.run(loader, twse_rows=[TWSE_2327], tpex_rows=[TPEX_3152]) == 0
        assert set(rig.table()['stock_id']) == {'1103'}

    def test_refetch_all_overrides_the_resume_filter(self, rig):
        rig.universe('1101')
        rig.seed([_row('1101', '2020-06-01', 0.97, source=SRC_REFERENCE)])
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        assert rig.run(loader, argv=['--refetch-all']) == 0
        assert loader.asked == ['1101']


class TestMainFailureHandling:
    """contract 不變量 7 + 審查 F2 / F5。"""

    def test_quota_stop_returns_3_and_keeps_what_was_fetched(self, rig):
        rig.universe('1101', '1102', '1103')
        loader = _FakeLoader(dividends={'1101': _DIV_1101},
                             dividend_error=FinMindQuotaBlockedError('quota'),
                             error_from=2)
        assert rig.run(loader) == 3
        assert loader.asked == ['1101', '1102']        # 第 2 檔就停，沒有硬跑完
        assert list(rig.table()['stock_id']) == ['1101']   # 已抓到的有寫進去

    def test_a_dead_dividend_source_refuses_to_write(self, rig):
        """審查 F2：原本的條件 `stats['ok'] > 20 and len(dividends) == 0`
        **邏輯上不可能成立**（`ok` 只在收到非空 frame 時 +1，所以
        `len(dividends) == 0` 等價於 `ok == 0`），`refusing to write` 從沒執行過。
        來源整個死掉時工具會回 exit 0 ＝ 排程當成成功。
        """
        stems = ['%04d' % (1100 + i) for i in range(25)] + ['2327']
        rig.universe(*stems)
        loader = _FakeLoader()                       # 每一檔都回空 frame
        assert rig.run(loader, twse_rows=[TWSE_2327]) == 1
        assert len(loader.asked) == 26
        # 拒絕寫入，不是寫完才抱怨 —— 而且這裡本來有一筆合法的減資可寫
        assert not rig.out.exists()

    def test_a_routine_rerun_of_event_free_stocks_still_succeeds(self, rig):
        """選擇性，也是 F2 最容易改壞的地方。

        全市場抓完之後，續跑剩下的**正好就是**真的沒有除權息事件的那批
        （docstring 記為約 4%，實測 `--limit 200` 剩 4 檔且全部合法零事件）。
        天真地改成「`no_events` 超過門檻就報錯」會讓每次例行重跑都誤報 ——
        這裡故意讓待辦有 21 檔（超過門檻 20）但只佔宇宙 21%。
        """
        stems = ['%04d' % (1100 + i) for i in range(100)]
        rig.universe(*stems)
        rig.seed([_row(s, '2020-06-01', 0.97, source=SRC_REFERENCE) for s in stems[:79]])
        loader = _FakeLoader()
        rc = rig.run(loader)
        assert len(loader.asked) == 21
        assert rc == 0

    def test_a_source_that_raises_on_every_stock_refuses_to_write(self, rig):
        """審查 R3-1：閘門要數「沒收成」，不能只數「回空」。

        非額度例外走的是 `stats['error']`，**不是** `stats['no_events']` ——
        所以只看 `no_events` 的話，「整批股票每一檔都拋例外」（SDK 方法改名、
        斷網、例外型別改名讓 `'Quota' in name` 失效）仍然回 exit 0，
        排程只看 exit code 就會當成成功。這是 AGENTS.md 列為 SERIOUS 的吞 exit code。
        """
        stems = ['%04d' % (1100 + i) for i in range(25)] + ['2327']
        rig.universe(*stems)
        loader = _FakeLoader(dividend_error=ValueError('SDK method renamed'))
        assert rig.run(loader, twse_rows=[TWSE_2327]) == 1
        assert len(loader.asked) == 26                 # 每一檔都問過、每一檔都炸
        # 跟「回空」那條一樣：拒絕寫入，即使這裡本來有一筆合法的減資可寫
        assert not rig.out.exists()

    def test_a_routine_rerun_with_scattered_errors_still_succeeds(self, rig):
        """選擇性，釘住 R3-1 修法裡的 `bulk_run` 條件。

        例行續跑本來就會有零星錯誤（單檔逾時、上游偶發 5xx）。少了 `bulk_run`，
        把 `error` 加進閘門之後這些偶發失敗就會被誤報成「來源死亡」。
        這裡待辦 21 檔（超過門檻 20）但只佔宇宙 21%，全部拋例外也必須維持 exit 0。
        """
        stems = ['%04d' % (1100 + i) for i in range(100)]
        rig.universe(*stems)
        rig.seed([_row(s, '2020-06-01', 0.97, source=SRC_REFERENCE) for s in stems[:79]])
        loader = _FakeLoader(dividend_error=ValueError('transient upstream 502'))
        rc = rig.run(loader)
        assert len(loader.asked) == 21
        assert rc == 0
        assert len(rig.table()) == 79                  # 既有列原封不動留著

    def test_a_bulk_backfill_tolerates_genuinely_event_free_stocks(self, rig):
        """選擇性，釘住 R3-1 修法裡的 `stats['ok'] == 0` 條件。

        全市場首建**本來就**會有一批股票真的沒有除權息（docstring 記為約 4%），
        再加上零星錯誤，`fruitless` 輕鬆超過門檻 20 —— 但那是正常的，
        因為絕大多數股票有收成。少了 `ok == 0`，一次跑完約 1,900 檔的回補
        會在最後一刻 exit 1 拒絕寫入，3.3 小時的 FinMind 額度整批丟掉。

        2026-08-11 負向對照抓到的：把 `ok == 0` 拿掉時，原本 6 個突變裡
        只有這一種**存活**（既有測試全綠），所以補這一條。
        """
        ok_stems = ['%04d' % (1100 + i) for i in range(40)]
        dry_stems = ['%04d' % (1200 + i) for i in range(20)]
        rig.universe(*(ok_stems + dry_stems))
        divs = {s: _finmind_dividends([('2021-07-01', 50.0, 48.0, 2.0)])
                for s in ok_stems}
        loader = _FakeLoader(dividends=divs)
        assert rig.run(loader) == 0                    # 首建、bulk、20 檔零事件 = 正常
        assert len(loader.asked) == 60
        assert len(rig.table()) == 40                  # 有收成的那批照樣寫進去

    def test_par_value_failure_does_not_starve_the_dividend_loop(self, rig):
        """審查 F5：面額變更層（FinMind、全市場）必須排在逐檔層**之後**。

        FinMind 免費層對「不帶 `data_id` 的全市場查詢」回 HTTP 400
        `Your level is register`，SDK 把它變成 `KeyError('data')`，
        `cache_manager` 再一律當成伺服器端額度爆掉。原本這個誤判會設一個
        **全域**旗標把整個逐檔迴圈關掉，並回報「被額度打斷，再跑一次即可續跑」——
        但等多久再跑都不會好，每一輪都是零進度。

        ⚠️ 只把旗標改成區域變數不夠：`cache_manager` 的 negative cache 是
        **行程層級**的，被誤判一次之後 300 秒內每個 FinMind 呼叫都直接 fail-fast，
        逐檔迴圈照樣死在第 1 檔。真正的解法是順序。
        """
        rig.universe('1101', '1102')
        loader = _FakeLoader(
            dividends={'1101': _DIV_1101, '1102': _DIV_1102},
            par_value_error=FinMindQuotaBlockedError('Your level is register'))
        assert rig.run(loader) == 0                  # 市場層失敗不改 exit code
        assert loader.asked == ['1101', '1102']      # 逐檔層照跑完
        assert len(rig.table()) == 2
        # 順序本身就是修法：便宜、每次重抓的市場層不可以卡在昂貴、可續跑的那層前面
        assert loader.sequence[-1] == 'par_value'

    def test_an_official_endpoint_failure_does_not_abort_the_run(self, rig):
        """選擇性：官方端點掛掉時，除權息照跑（既有設計，一併釘住）。"""
        rig.universe('1101')
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        bad = _FakeOfficialSession()
        bad.get = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('endpoint down'))
        self_session = bad
        rig.mp.setattr(bca, 'get_finmind_loader', lambda: loader)
        rig.mp.setattr(bca, 'official_session', lambda: self_session)
        rig.mp.setattr(sys, 'argv', ['build_corporate_actions.py'])
        assert bca.main() == 0
        assert len(rig.table()) == 1


class TestMainExistingTableHandling:
    """contract 不變量 7 的後半 + 審查 F10。"""

    def test_a_corrupt_table_is_moved_aside_before_writing(self, rig):
        rig.universe('1101')
        rig.out.parent.mkdir(parents=True, exist_ok=True)
        rig.out.write_bytes(b'this is definitely not a parquet file')
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        assert rig.run(loader) == 0
        aside = list(rig.out.parent.glob('*.corrupt-*'))
        assert len(aside) == 1
        assert aside[0].read_bytes() == b'this is definitely not a parquet file'
        assert len(rig.table()) == 1

    def test_a_corrupt_table_that_cannot_be_moved_refuses_to_write(self, rig):
        rig.universe('1101')
        rig.out.parent.mkdir(parents=True, exist_ok=True)
        rig.out.write_bytes(b'not parquet')

        def _no_replace(*_a, **_k):
            raise OSError('cannot move')

        rig.mp.setattr(bca.os, 'replace', _no_replace)
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        assert rig.run(loader) == 1
        assert rig.out.read_bytes() == b'not parquet'      # 原始位元組保住

    def test_a_table_that_cannot_be_opened_is_not_quarantined(self, rig):
        """審查 F10：讀不到 ≠ 內容壞掉。

        檔案鎖、防毒掃描、另一個同時在跑的實例都會讓 `read_parquet` 拋例外，
        但那些都不是毀損。原本任何例外都改名 ＝ 丟掉整份續跑狀態
        （下一輪要重燒約 3.3 小時的 FinMind 額度，還要人工把檔名改回來）。

        這裡讓路徑其實是個**目錄**來製造貨真價實的 OSError，不用任何 mock。
        """
        rig.universe('1101')
        rig.out.mkdir(parents=True)
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        assert rig.run(loader) == 1
        assert rig.out.is_dir()                             # 原封不動
        assert not list(rig.out.parent.glob('*.corrupt-*'))
        assert loader.asked == []                           # 連額度都沒開始燒

    def test_dry_run_reports_a_corrupt_table_without_quarantining_it(self, rig):
        """審查 R3-2：`--dry-run` 宣稱不寫檔，就不可以動檔案系統。

        改名發生在**讀取**階段，比 `args.dry_run` 那個判斷更早，所以原本
        「只想看看會發生什麼」的冒煙測試會把既有表改成 `.corrupt-<ts>`，
        還要人工改回檔名。位元組不會掉（F10 那條仍成立），但那是意外的副作用。

        對照組是同一節第一個測試：**真跑**時照樣要改名（那條不可以跟著變綠）。
        """
        rig.universe('1101')
        rig.out.parent.mkdir(parents=True, exist_ok=True)
        rig.out.write_bytes(b'this is definitely not a parquet file')
        loader = _FakeLoader(dividends={'1101': _DIV_1101})
        assert rig.run(loader, argv=['--dry-run']) == 0
        assert not list(rig.out.parent.glob('*.corrupt-*'))     # 沒有隔離檔
        # 原檔還在原地、位元組一個沒動
        assert rig.out.read_bytes() == b'this is definitely not a parquet file'


class TestUnusableRowsAreDropped:
    """審查 F4：只有減資層會丟掉算不出係數的列，另外兩層沒有。"""

    def test_dividend_rows_with_an_underivable_factor_are_dropped(self):
        """NaN 係數不帶資訊，卻會用 `keep='first'` 永久佔住 key 且修不掉。"""
        dl = _FakeLoader(dividends={'1101': _finmind_dividends([
            ('2021-07-01', 50.0, 48.0, 2.0),        # 正常
            ('2021-07-02', 0.0, np.nan, np.nan),    # before_price 壞掉
            ('2021-07-03', np.nan, np.nan, np.nan),  # 欄位改名會長這樣
        ])})
        df = fetch_events(dl, '1101', '2000-01-01')
        assert list(df['date'].dt.strftime('%Y-%m-%d')) == ['2021-07-01']
        assert df.iloc[0]['source'] == SRC_REFERENCE

    def test_the_par_value_layer_derives_the_expected_factor(self):
        """面額變更層原本**連一個測試都沒有** —— 審查把它整層改成回空 frame，
        39 個測試全綠。10 元改 1 元 ＝ 係數 0.1，也是 `FACTOR_MIN` 壓到 0.05 的原因。
        """
        dl = _FakeLoader(par_value=_finmind_par_value([
            ('6548', '2019-09-09', '312.0', '31.2')]))
        df = fetch_par_value_changes(dl, '2000-01-01')
        assert list(df['stock_id']) == ['6548']
        assert df.iloc[0]['factor'] == pytest.approx(0.1)
        assert df.iloc[0]['source'] == SRC_PAR_VALUE

    def test_par_value_rows_with_an_underivable_factor_are_dropped(self):
        dl = _FakeLoader(par_value=_finmind_par_value([
            ('6548', '2019-09-09', '312.0', '31.2'),
            ('9999', '2019-09-10', '-', '-'),
        ]))
        df = fetch_par_value_changes(dl, '2000-01-01')
        assert list(df['stock_id']) == ['6548']

    def test_a_fallback_row_is_still_kept(self):
        """選擇性：fallback 算得出係數，不可以被當成 unusable 一起丟掉。"""
        dl = _FakeLoader(dividends={'6541': _finmind_dividends([
            ('2018-07-10', 95.20, 95.20, 1.053754)])})
        df = fetch_events(dl, '6541', '2000-01-01')
        assert len(df) == 1
        assert df.iloc[0]['source'] == SRC_FALLBACK


# --------------------------------------------------------------------------- #
#  增量模式（`--sync-recent`）：官方除權息行事曆
#
#  這一段釘的是續跑語義的結構性缺口：逐檔層的完成判定是 per-STOCK 不是 per-DATE，
#  所以一檔股票只要有過任何除權息列就永遠被跳過，它**之後**新發生的事件再也補不到
#  （FinMind 當天入庫落後，夜間路徑必然錯過）。2026-08-11 實證：官方當天 5 筆除權息
#  全部不在表內，而那幾檔全都已在表中 ＝ 全部被跳過。
# --------------------------------------------------------------------------- #

# 逐格照抄 2026-08-11 實際回應（欄位順序照 payload）
TWSE_EXRIGHT_FIELDS = [
    '資料日期', '股票代號', '股票名稱', '除權息前收盤價', '除權息參考價', '權值+息值',
    '權/息', '漲停價格', '跌停價格', '開盤競價基準', '減除股利參考價', '詳細資料',
    '最近一次申報資料 季別/日期', '最近一次申報每股 (單位)淨值',
    '最近一次申報每股 (單位)盈餘']
TWSE_EXRIGHT_1465 = ['115年08月03日', '1465', '偉全', '12.70', '12.40', '0.300000',
                     '息', '13.60', '11.20', '12.40', '12.40', '1465,20260803',
                     '115年第1季', '15.15', '0.27']
TWSE_EXRIGHT_2892 = ['115年08月11日', '2892', '第一金', '29.40', '28.30', '1.100000',
                     '息', '31.10', '25.50', '28.30', '28.30', '2892,20260811',
                     '115年第1季', '20.00', '1.00']


def _twse_exright_payload(rows, start='20260804', end='20260811', stat='OK',
                          fields=None):
    return {'stat': stat, 'strDate': start, 'endDate': end,
            'fields': TWSE_EXRIGHT_FIELDS if fields is None else fields,
            'data': list(rows)}


def _tpex_cal_row(date, stock_id, cash='1.00000000'):
    """OpenAPI `tpex_exright_prepost` 的一列（本工具會讀的鍵 ＋ 幾個鄰居）。"""
    return {'ExRrightsExDividendDate': date, 'SecuritiesCompanyCode': stock_id,
            'CompanyName': 'X', 'ExRrightsExDividend': '除息',
            'StockDividendRatio': '0.00000000', 'CashDividend': cash}


class _FakeCalendarSession:
    """增量模式兩個端點的替身。TWSE 照實回音；TPEX 那支**不吃參數**（真品如此）。"""

    def __init__(self, twse_payload=None, tpex_rows=None,
                 twse_error=None, tpex_error=None):
        self._twse_payload = twse_payload
        self._tpex_rows = tpex_rows if tpex_rows is not None else []
        self._twse_error = twse_error
        self._tpex_error = tpex_error
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        if 'exRight' in url:
            if self._twse_error:
                raise self._twse_error
            payload = self._twse_payload
            if payload is None:
                payload = _twse_exright_payload([], params['startDate'],
                                                params['endDate'])
            return _FakeResp(payload)
        if self._tpex_error:
            raise self._tpex_error
        return _FakeResp(self._tpex_rows)


class TestExRightCalendarParsing:
    def test_twse_rows_become_events_with_a_derived_factor(self):
        sess = _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_1465]))
        df = bca.fetch_exright_twse(sess, '2026-08-04', '2026-08-11')

        assert len(df) == 1
        row = df.iloc[0]
        assert row['stock_id'] == '1465'
        assert row['date'] == pd.Timestamp('2026-08-03')
        assert row['before_price'] == pytest.approx(12.70)
        assert row['reference_price'] == pytest.approx(12.40)
        assert row['factor'] == pytest.approx(12.40 / 12.70)

    def test_twse_rows_are_tagged_with_their_own_source(self):
        """必須是 `twse_exright`，不能混進 `reference`/`fallback`。

        那兩個值屬於 `DIVIDEND_SOURCES` ＝ 續跑的完成判據。官方表只涵蓋查詢的
        那幾天、不是全歷史，標成 `reference` 會讓一檔從沒被逐檔抓過、卻剛好在
        增量窗口內配息的股票被永久標記完成，它的歷史除權息就再也抓不到 ——
        那正是這個模式要修的病，只是換個方向重現一次。
        """
        sess = _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_1465]))
        df = bca.fetch_exright_twse(sess, '2026-08-04', '2026-08-11')

        assert df.iloc[0]['source'] == bca.SRC_TWSE_EXRIGHT
        assert bca.SRC_TWSE_EXRIGHT not in bca.DIVIDEND_SOURCES

    def test_twse_rejects_a_window_it_did_not_echo(self):
        sess = _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_1465],
                                                          start='20260101',
                                                          end='20260102'))
        with pytest.raises(RuntimeError, match='沒有採用請求的區間'):
            bca.fetch_exright_twse(sess, '2026-08-04', '2026-08-11')

    def test_an_empty_window_is_success_not_failure(self):
        """「這段期間沒有除權息」不是故障。

        ⚠️ 實測（2026-08-11）空窗口的回應是 `很抱歉，沒有符合條件的資料!`，而且
        **fields / data / strDate / endDate 一個鍵都沒有**（所以回音檢查與欄位檢查
        也一定會炸）。把它當失敗的話，只要遇上一段真的沒有除權息的日子
        （農曆年長假最典型），排程就會**每天**報一次假警報 —— 誤報久了沒人看，
        真的故障也會被一起忽略。
        """
        sess = _FakeCalendarSession({'stat': '很抱歉，沒有符合條件的資料!'})
        df = bca.fetch_exright_twse(sess, '2026-12-01', '2026-12-05')

        assert len(df) == 0
        assert list(df.columns) == COLS

    def test_a_genuine_refusal_still_raises(self):
        """只有「沒有符合條件的資料」那一種算空窗口，其他非 OK 一律照舊 raise。

        對照組取自減資表真實會回的訊息 —— 那個必須繼續是錯誤，
        不能被「空窗口」這條新路徑一起吞掉。
        """
        sess = _FakeCalendarSession(
            {'stat': '查詢開始日期小於100年1月1日，請重新查詢!'})
        with pytest.raises(RuntimeError, match='拒絕查詢'):
            bca.fetch_exright_twse(sess, '2010-01-01', '2010-01-05')

    def test_twse_raises_when_a_field_disappears(self):
        """欄位用名字定位，改版就要吵 —— 靜默錯位會寫進錯的價格。"""
        broken = [f for f in TWSE_EXRIGHT_FIELDS if f != '除權息參考價']
        sess = _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_1465],
                                                          fields=broken))
        with pytest.raises(RuntimeError, match='欄位不見了'):
            bca.fetch_exright_twse(sess, '2026-08-04', '2026-08-11')

    def test_twse_survives_reordered_fields(self):
        """欄位順序倒過來但名字還在 -> 照樣正確（位置定位會在這裡壞掉）。

        這是 round-1 那個「11 種突變只有它全綠」的同型缺口：兩張表剛好共用欄位
        順序時，只比對兩個市場驗不到錯位。
        """
        order = list(reversed(range(len(TWSE_EXRIGHT_FIELDS))))
        fields = [TWSE_EXRIGHT_FIELDS[i] for i in order]
        row = [TWSE_EXRIGHT_1465[i] for i in order]
        sess = _FakeCalendarSession(_twse_exright_payload([row], fields=fields))

        df = bca.fetch_exright_twse(sess, '2026-08-04', '2026-08-11')
        assert df.iloc[0]['stock_id'] == '1465'
        assert df.iloc[0]['factor'] == pytest.approx(12.40 / 12.70)

    def test_tpex_calendar_drops_events_that_have_not_happened_yet(self):
        """那支端點是 pre+post。未來事件的價格還不存在，收進來只會是垃圾列。"""
        sess = _FakeCalendarSession(tpex_rows=[
            _tpex_cal_row('1150803', '3306'),     # 已發生
            _tpex_cal_row('1150811', '3390'),     # 當天
            _tpex_cal_row('1150831', '9999'),     # 還沒發生
        ])
        df = bca.fetch_exright_calendar_tpex(sess, '2026-08-11')

        assert sorted(df['stock_id']) == ['3306', '3390']

    def test_tpex_calendar_skips_unparseable_rows_instead_of_crashing(self):
        """實測 payload 混著 `ExRrightsExDividendDate == '0'` 的列。"""
        sess = _FakeCalendarSession(tpex_rows=[
            _tpex_cal_row('0', '1111'),
            _tpex_cal_row('', '2222'),
            {'SecuritiesCompanyCode': '3333'},        # 完全沒有日期鍵
            _tpex_cal_row('1150803', ''),             # 沒有代號
            _tpex_cal_row('1150803', '3306'),         # 唯一好的
        ])
        df = bca.fetch_exright_calendar_tpex(sess, '2026-08-11')

        assert list(df['stock_id']) == ['3306']

    def test_tpex_calendar_raises_when_the_payload_shape_changes(self):
        sess = _FakeCalendarSession(tpex_rows={'unexpected': 'dict'})
        with pytest.raises(RuntimeError, match='不是列表'):
            bca.fetch_exright_calendar_tpex(sess, '2026-08-11')


class TestSyncRecent:
    """增量模式的行為契約。"""

    def _args(self, days=7, dry_run=False, sleep=0.0, start='2000-01-01'):
        return argparse.Namespace(sync_recent=days, dry_run=dry_run, sleep=sleep,
                                  start_date=start)

    def _run(self, rig, args, universe, existing, session, loader):
        rig.mp.setattr(bca, 'official_session', lambda: session)
        return bca.sync_recent(args, list(universe), set(universe), existing,
                               '2026-08-11', dl_factory=lambda: loader)

    def test_adds_a_listed_event_without_spending_any_finmind_call(self, rig):
        """上市那條路必須零 FinMind —— 官方表自帶算 factor 的三個欄位。"""
        existing = _frame([_row('2892', '2025-08-07', 0.97)])
        loader = _FakeLoader()
        code, merged = self._run(
            rig, self._args(), ['2892'], existing,
            _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_2892])), loader)

        assert code == 0
        assert loader.asked == []          # 一次都沒問 FinMind
        added = merged[merged['date'] == pd.Timestamp('2026-08-11')]
        assert len(added) == 1
        assert added.iloc[0]['stock_id'] == '2892'
        assert added.iloc[0]['source'] == bca.SRC_TWSE_EXRIGHT
        assert added.iloc[0]['factor'] == pytest.approx(28.30 / 29.40)

    def test_does_not_resend_events_the_table_already_has(self, rig):
        """已有的不重送 —— 否則每天例行執行都刷出幾十筆 collision 警告，
        真正該注意的碰撞會被淹掉。"""
        existing = _frame([_row('2892', '2026-08-11', 0.9628)])
        loader = _FakeLoader()
        code, merged = self._run(
            rig, self._args(), ['2892'], existing,
            _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_2892])), loader)

        assert code == 0
        assert len(merged) == len(existing)
        assert merged.iloc[0]['source'] == SRC_REFERENCE   # 既有列原封不動

    def test_already_present_events_never_reach_the_merge(self, rig, monkeypatch):
        """已在表中的事件**不可以送進 merge**。

        ⚠️ 這一條不能用「合併結果對不對」來驗，突變測試證實過：把重複的送進去，
        merge 是 append-only、既有列本來就會勝，**結果一模一樣**
        （`fresh = mine` 這個突變讓當時 93 個測試全部照樣綠）。
        真正被破壞的是 collision 警告的用途 —— 窗口內幾十筆既有事件每天撞一次，
        真正該注意的碰撞就被淹在噪音裡。所以這裡直接攔 `merge_events`，
        驗的是「送進去的是什麼」而不是「出來的是什麼」。
        """
        seen = {}
        real_merge = bca.merge_events

        def spy(existing, fetched):
            seen['fetched'] = fetched.copy()
            return real_merge(existing, fetched)

        monkeypatch.setattr(bca, 'merge_events', spy)
        existing = _frame([_row('2892', '2026-08-11', 0.9628)])
        code, _merged = self._run(
            rig, self._args(), ['2892'], existing,
            _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_2892])),
            _FakeLoader())

        assert code == 0
        assert len(seen['fetched']) == 0

    def test_otc_gap_triggers_exactly_one_finmind_call_per_missing_stock(self, rig):
        """上櫃行事曆沒有價格，缺的那幾檔才去 FinMind 拿 —— 不是整批重抓。"""
        existing = _frame([_row('3390', '2025-07-01', 0.98),
                           _row('4580', '2025-07-01', 0.99)])
        session = _FakeCalendarSession(
            _twse_exright_payload([]),
            tpex_rows=[_tpex_cal_row('1150811', '3390'),   # 表裡沒有 -> 要補
                       _tpex_cal_row('1150701', '4580')],  # 不在窗口內
        )
        loader = _FakeLoader(dividends={'3390': _finmind_dividends(
            [('2026-08-11', 20.0, 19.5, 0.5)])})
        code, merged = self._run(rig, self._args(days=3), ['3390', '4580'],
                                 existing, session, loader)

        assert code == 0
        assert loader.asked == ['3390']
        assert len(merged[(merged['stock_id'] == '3390')
                          & (merged['date'] == pd.Timestamp('2026-08-11'))]) == 1

    def test_universe_filter_keeps_out_of_universe_symbols_out(self, rig):
        """官方表含 ETF／特別股（實測 `00940`、`2348A`、`00953B`）。宇宙是照
        price CSV 長出來的，沒有價格序列就沒有還原對象 —— 與既有減資層同一個過濾器。
        """
        etf_row = list(TWSE_EXRIGHT_2892)
        etf_row[1] = '00940'
        loader = _FakeLoader()
        code, merged = self._run(
            rig, self._args(), ['2892'], bca._empty_events(),
            _FakeCalendarSession(
                _twse_exright_payload([TWSE_EXRIGHT_2892, etf_row])), loader)

        assert code == 0
        assert set(merged['stock_id']) == {'2892'}

    def test_returns_nonzero_when_both_official_calendars_fail(self, rig):
        """兩個來源都掛掉時這個模式什麼都沒檢查到，回 0 就是吞 exit code
        （AGENTS.md 列為 SERIOUS）。"""
        code, _merged = self._run(
            rig, self._args(), ['2892'], bca._empty_events(),
            _FakeCalendarSession(twse_error=RuntimeError('twse down'),
                                 tpex_error=RuntimeError('tpex down')),
            _FakeLoader())

        assert code == 1

    def test_one_calendar_failing_still_lets_the_other_through(self, rig):
        """單邊掛掉不該讓另一邊白跑（兩個市場是獨立來源）。"""
        code, merged = self._run(
            rig, self._args(), ['2892'], bca._empty_events(),
            _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_2892]),
                                 tpex_error=RuntimeError('tpex down')),
            _FakeLoader())

        assert code == 0
        assert len(merged) == 1

    def test_dry_run_writes_nothing(self, rig):
        code, merged = self._run(
            rig, self._args(dry_run=True), ['2892'], bca._empty_events(),
            _FakeCalendarSession(_twse_exright_payload([TWSE_EXRIGHT_2892])),
            _FakeLoader())

        assert code == 0
        assert len(merged) == 1          # 算出來了
        assert not rig.out.exists()      # 但沒落地
