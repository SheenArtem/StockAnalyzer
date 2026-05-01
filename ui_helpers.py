"""UI helper 函式集中地（Phase A 從 app.py 抽出）

收錄：
- 籌碼快取 wrapper（@st.cache_data）
- 歷史紀錄 dropdown callback
- 分析主流程封裝（run_analysis）
- AI 報告背景 worker + lock
- ticker validation
- picks 表 column 短字串 helper（週榜 / 題材 / 共振）

設計原則：保留原 app.py 的 lazy import 風格（避免 streamlit 啟動慢）。
"""

import logging
import re
import threading

import streamlit as st

from technical_analysis import (
    calculate_all_indicators,
    load_and_resample,
    plot_dual_timeframe,
    plot_interactive_chart,
)

logger = logging.getLogger(__name__)


# ====================================================================
# 籌碼快取（H5 2026-04-23 改用 ChipAnalyzer.fetch_chip 乾淨 API）
# ====================================================================
@st.cache_data(ttl=3600)
def get_chip_data_cached(ticker, force):
    """取得籌碼快取。

    Returns:
        dict | None: chip data dict on success, None on fetch failure.
        Caller 直接 `if chip_data is not None: ...`，不需 unpack。
    """
    from chip_analysis import ChipAnalyzer, ChipFetchError
    try:
        return ChipAnalyzer().fetch_chip(ticker, force_update=force)
    except ChipFetchError as e:
        logger.warning("Chip fetch failed for %s: %s", ticker, e)
        return None


# ====================================================================
# 歷史紀錄 dropdown callback（sidebar 用）
# ====================================================================
def on_history_change():
    """side bar 歷史紀錄 selectbox 的 on_change callback。"""
    selected = st.session_state.get('history_selected', '')
    if selected:
        selected = selected.strip()
        # Basic character check: only allow alphanumeric, dot, hyphen
        if not re.match(r'^[A-Za-z0-9.\-]{1,20}$', selected):
            logger.error(f"Invalid ticker from history dropdown: {selected!r}")
            return  # Do not activate analysis for invalid ticker
    st.session_state['ticker_input'] = selected
    st.session_state['analysis_active'] = True
    st.session_state['force_run'] = False


# ====================================================================
# 個股分析主流程封裝
# ====================================================================
def run_analysis(source_data, force_update=False):
    """個股分析主流程：股票代號或 CSV 都能餵。

    Returns:
        (figures, errors, df_week, df_day, stock_meta) 5-tuple
    """
    # 1. 股票代號情況
    if isinstance(source_data, str):
        return plot_dual_timeframe(source_data, force_update=force_update)

    # 2. CSV 資料情況 (DataFrame 無法直接 hash，需注意 cache 機制，這裡簡化處理)
    # Streamlit 對 DataFrame 有支援 hashing，所以通常可以直接傳
    ticker_name, df_day, df_week, stock_meta = load_and_resample(source_data)  # CSV no force update

    figures = {}
    errors = {}

    # 手動計算
    if not df_week.empty:
        try:
            df_week = calculate_all_indicators(df_week)
            fig_week = plot_interactive_chart(ticker_name, df_week, "Trend (Long)", "Weekly")
            figures['Weekly'] = fig_week
        except Exception as e:
            errors['Weekly'] = str(e)

    if not df_day.empty:
        try:
            df_day = calculate_all_indicators(df_day)
            fig_day = plot_interactive_chart(ticker_name, df_day, "Action (Short)", "Daily")
            figures['Daily'] = fig_day
        except Exception as e:
            errors['Daily'] = str(e)

    return figures, errors, df_week, df_day, stock_meta


# ====================================================================
# AI 報告背景執行緒 worker
# H3 (2026-04-23): 重構後與 tools/auto_ai_reports 共用 ai_report_pipeline
# H4 (2026-04-23): _ai_report_job_lock 保護 job dict 多步 state transition
# ====================================================================
_ai_report_job_lock = threading.Lock()


def _ai_report_worker(job, ticker, report_format='md', include_songfen=True):
    """在背景 thread 跑完整 AI 報告流程。

    job 是 session_state 裡的 dict 參照，thread 透過 _ai_report_job_lock 安全 mutate。
    禁止呼叫任何 st.* UI 函式（會觸發 ScriptRunContext 警告）。

    Args:
        report_format: 'md' = 傳統 Markdown 報告；'html' = 互動儀表板
        include_songfen: bool，md 格式時在最末尾附加「宋分視角補充分析」區塊。html 忽略。
    """
    from ai_report_pipeline import generate_one_report

    def _progress(msg):
        with _ai_report_job_lock:
            job['progress'].append(msg)

    try:
        result = generate_one_report(ticker, fmt=report_format,
                                     progress_cb=_progress,
                                     include_songfen=include_songfen)
        with _ai_report_job_lock:
            if result['ok']:
                job['result'] = {
                    'rid': result['rid'],
                    'content': result['content'],
                    'format': result['format'],
                }
                job['status'] = 'done'
            else:
                err = result.get('error') or 'unknown error'
                if result.get('traceback'):
                    err = f"{err}\n\n{result['traceback']}"
                job['result'] = err
                job['status'] = 'error'
    except Exception as _e:
        # Defensive: 通常 generate_one_report 自己會 catch，這層是 last-resort
        import traceback
        logger.error(f"[AI worker] uncaught exception: {_e}", exc_info=True)
        with _ai_report_job_lock:
            job['result'] = f"{type(_e).__name__}: {_e}\n\n{traceback.format_exc()}"
            job['status'] = 'error'


# ====================================================================
# Ticker validation
# ====================================================================
def validate_ticker(ticker):
    """驗證股票代號格式 (只允許英數字、點號、連字號)"""
    if not ticker:
        return False, "請輸入股票代號"
    # 只允許英數字、點號、連字號，長度 1-20
    pattern = r'^[A-Za-z0-9.\-]{1,20}$'
    if not re.match(pattern, ticker):
        return False, "股票代號格式不正確 (只允許英數字、點號)"
    return True, ""


# ====================================================================
# Picks 表 column 短字串 helper
# ====================================================================
def _wc_tags_short(stock_id):
    """取個股本週上榜 tags 並 join 成短字串給表格 column (BL-4 Phase C)。Empty -> ''."""
    try:
        from weekly_chip_loader import get_stock_tags as _wc_get
        tags = _wc_get(stock_id)
        return '; '.join(tags) if tags else ''
    except Exception:
        return ''


# --- 4-layer 題材融合 (2026-05-01)：manual → News (30d) → YT (180d) → TV industry 中文 ---
# 原僅 manual.json 140 ticker (~15-40% picks 命中率) → 三層 → 四層後 ~95%+ non-empty。
# 新加 Layer 2: News themes from data/news_themes.parquet (Sonnet 萃取，30d TTL)
# Why News before YT: news 是 current focus，YT 法說會節奏較慢 (週/月)。

_DYN_TAGS_TTL_DAYS = 180  # YT 動態題材：只用近 180 天提及
_DYN_TAGS_RELOAD_SEC = 3600  # parquet 1h reload (檔案每日由 scanner 更新)
_DYN_TAGS_CACHE = {'data': None, 'ts': 0.0}

_NEWS_TAGS_TTL_DAYS = 30  # News themes：只用近 30 天 (parquet 已 trim，這裡 belt+suspenders)
_NEWS_TAGS_RELOAD_SEC = 3600
_NEWS_TAGS_CACHE = {'data': None, 'ts': 0.0}

# TV industry 英文 → 中文 mapping (cover 全部 112 個 TW 市場 industries)
_TV_INDUSTRY_ZH = {
    # 半導體 / 電子
    'Semiconductors': '半導體',
    'Electronic Components': '電子零組件',
    'Electronic Production Equipment': '電子設備製造',
    'Electronic Equipment/Instruments': '電子儀器',
    'Electronics Distributors': '電子通路',
    'Electronics/Appliances': '消費電子',
    'Electronics/Appliance Stores': '消費電子零售',
    'Computer Peripherals': '電腦週邊',
    'Computer Processing Hardware': '電腦處理硬體',
    'Computer Communications': '電腦通訊',
    'Telecommunications Equipment': '通訊設備',
    # 工業機械 / 製造
    'Industrial Machinery': '工業機械',
    'Industrial Specialties': '工業特用',
    'Industrial Conglomerates': '工業集團',
    'Miscellaneous Manufacturing': '一般製造',
    'Metal Fabrication': '金屬加工',
    'Tools & Hardware': '工具五金',
    'Trucks/Construction/Farm Machinery': '工程/農用車輛',
    'Building Products': '建材製品',
    'Construction Materials': '建材',
    'Engineering & Construction': '工程營建',
    'Aerospace & Defense': '航太國防',
    # 化工 / 材料
    'Chemicals: Specialty': '特用化學',
    'Chemicals: Major Diversified': '綜合化工',
    'Chemicals: Agricultural': '農化',
    'Pulp & Paper': '紙漿/紙業',
    'Containers/Packaging': '包裝材料',
    'Forest Products': '林產',
    # 鋼鐵 / 金屬
    'Steel': '鋼鐵',
    'Aluminum': '鋁',
    'Other Metals/Minerals': '其他金屬礦產',
    'Precious Metals': '貴金屬',
    'Mining/Quarrying': '採礦',
    'Coal': '煤炭',
    # 紡織 / 民生
    'Textiles': '紡織',
    'Apparel/Footwear': '成衣/鞋業',
    'Apparel/Footwear Retail': '成衣零售',
    'Home Furnishings': '家具',
    'Household/Personal Care': '家居/個人護理',
    'Consumer Sundries': '日用品',
    'Recreational Products': '休閒娛樂用品',
    'Other Consumer Specialties': '其他消費商品',
    # 食品 / 飲料
    'Food: Major Diversified': '綜合食品',
    'Food: Specialty/Candy': '特用食品',
    'Food: Meat/Fish/Dairy': '肉品/水產/乳品',
    'Beverages: Non-Alcoholic': '飲料',
    'Beverages: Alcoholic': '酒類',
    'Tobacco': '菸草',
    'Agricultural Commodities/Milling': '農產加工',
    'Food Retail': '食品零售',
    'Food Distributors': '食品通路',
    # 零售 / 通路
    'Specialty Stores': '專賣店',
    'Department Stores': '百貨',
    'Discount Stores': '量販',
    'Internet Retail': '電商',
    'Wholesale Distributors': '批發通路',
    'Catalog/Specialty Distribution': '型錄/專業通路',
    'Drugstore Chains': '連鎖藥局',
    'Home Improvement Chains': '居家修繕',
    'Medical Distributors': '醫療通路',
    # 醫療 / 生技
    'Pharmaceuticals: Major': '主要藥廠',
    'Pharmaceuticals: Generic': '學名藥',
    'Pharmaceuticals: Other': '其他製藥',
    'Biotechnology': '生技',
    'Medical Specialties': '醫療器材',
    'Medical/Nursing Services': '醫療看護服務',
    'Hospital/Nursing Management': '醫院管理',
    # 服務 / 觀光
    'Restaurants': '餐飲',
    'Hotels/Resorts/Cruise lines': '觀光酒店',
    'Hotels/Resorts/Cruiselines': '觀光酒店',
    'Movies/Entertainment': '電影娛樂',
    'Casinos/Gaming': '博弈',
    'Broadcasting': '廣播',
    'Cable/Satellite TV': '有線電視',
    'Publishing: Books/Magazines': '出版',
    'Publishing: Newspapers': '報業',
    'Advertising/Marketing Services': '廣告行銷',
    'Personnel Services': '人力服務',
    'Commercial Printing/Forms': '印刷',
    'Office Equipment/Supplies': '辦公用品',
    'Other Consumer Services': '其他消費服務',
    'Miscellaneous Commercial Services': '商業服務',
    'Environmental Services': '環保服務',
    # 運輸 / 物流
    'Marine Shipping': '航運',
    'Air Freight/Couriers': '空運/快遞',
    'Trucking': '陸運',
    'Airlines': '航空',
    'Other Transportation': '其他運輸',
    # 汽車
    'Auto Parts: OEM': '汽車零組件',
    'Motor Vehicles': '汽車整車',
    'Automotive Aftermarket': '汽車後市場',
    # 建材 / 不動產
    'Real Estate Development': '不動產開發',
    'Real Estate Investment Trusts': 'REITs',
    'Homebuilding': '建設',
    # 金融 / 保險
    'Major Banks': '大型銀行',
    'Regional Banks': '區域銀行',
    'Investment Banks/Brokers': '證券',
    'Investment Managers': '投信投顧',
    'Investment Trusts/Mutual Funds': 'ETF/基金',
    'Life/Health Insurance': '人壽/健康險',
    'Property/Casualty Insurance': '產險',
    'Multi-Line Insurance': '綜合保險',
    'Specialty Insurance': '特別保險',
    'Insurance Brokers/Services': '保險經紀',
    'Finance/Rental/Leasing': '租賃融資',
    'Financial Conglomerates': '金融集團',
    # 軟體 / IT
    'Packaged Software': '軟體',
    'Information Technology Services': 'IT 服務',
    'Internet Software/Services': '網路服務',
    'Data Processing Services': '資料處理服務',
    # 電力 / 公用事業
    'Electrical Products': '電氣設備',
    'Electric Utilities': '電力',
    'Water Utilities': '自來水',
    'Gas Distributors': '燃氣',
    'Alternative Power Generation': '再生能源',
    'Oil & Gas Production': '油氣生產',
    'Oil Refining/Marketing': '煉油',
    'Integrated Oil': '綜合油氣',
    # 電信
    'Major Telecommunications': '電信',
    'Wireless Telecommunications': '無線通訊',
    'Specialty Telecommunications': '特用電信',
}


# 同題材常見變體 → canonical 顯示名 (處理 manual.json vs YT 分頭命名差異)
# Key 是 strip-whitespace+lowercase 後的 normalize 形；value 是顯示名（或 '' 黑名單）
# 多個 key 可指向相同 value 達成跨變體去重 (e.g. 'cowos' 和 'cowos先進封裝' → 'CoWoS 先進封裝')
_THEME_ALIAS = {
    # 蘋果供應鏈 (manual.json 用「Apple 蘋果供應鏈」作 canonical)
    'apple供應鏈': 'Apple 蘋果供應鏈',
    'apple蘋果供應鏈': 'Apple 蘋果供應鏈',
    # AI 伺服器 (有 ODM / 電源 / 一般三層)
    'ai伺服器': 'AI 伺服器',
    'ai伺服器odm': 'AI 伺服器 ODM',
    'ai伺服器電源': 'AI 伺服器電源',
    'aipc': 'AI PC',
    'aipcsoc': 'AI PC SoC',
    # 先進封裝 / CoWoS (合併 CoWoS / 先進封裝 → manual canonical)
    'cowos': 'CoWoS 先進封裝',
    'cowos先進封裝': 'CoWoS 先進封裝',
    '先進封裝': 'CoWoS 先進封裝',
    '先進封測': '先進封測',
    # 載板 / PCB
    'pcb硬板': 'PCB 硬板',
    'abf載板': 'ABF 載板',
    # 半導體
    '矽晶圓': '矽晶圓',
    '半導體': '半導體',
    'asic': 'ASIC',
    'asic設計服務': 'ASIC 設計服務',
    'cpo': 'CPO',
    'hbm': 'HBM',
    # EV
    'ev': 'EV',
    'evev供應鏈': 'EV 供應鏈',
    # 黑名單
    'ai': '',
    '其他': '',
    '無': '',
    '電子': '',
    '半導體龍頭': '',
    '護國神山': '',
}


def _clean_yt_tag(tag):
    """處理 YT LLM 萃出常見的「主題（解釋）」格式：

    '先進封測（CoWoS 後段 / AI 測試）' → '先進封測'
    'AI 散熱（液冷）' → 'AI 散熱'
    截到第一個全形/半形括號前。如果整段都是 wrapped，回原字串。
    """
    if not tag or not isinstance(tag, str):
        return ''
    s = tag.strip()
    # 去 leading 括號描述
    for bracket in ('（', '('):
        if bracket in s:
            head = s.split(bracket, 1)[0].strip()
            if head:  # 確保截掉後不是空字串
                s = head
                break
    return s


def _normalize_theme_key(s):
    """Dedup key: 去空白 + lowercase + 共用 alias。

    'AI 伺服器 ODM' / 'AI伺服器ODM' → 同 key。
    'Apple 供應鏈' / 'Apple 蘋果供應鏈' → 走 _THEME_ALIAS 映射到同 key。
    """
    if not s or not isinstance(s, str):
        return ''
    base = s.replace(' ', '').replace('　', '').lower()
    # alias 表把已知變體 normalize 成同一個 canonical key
    canonical = _THEME_ALIAS.get(base)
    if canonical == '':
        return ''  # 顯式黑名單 (其他 / AI 太泛)
    if canonical:
        return canonical.replace(' ', '').replace('　', '').lower()
    return base


def _is_junk_yt_tag(tag):
    """過濾 LLM 萃出的 catch-all junk tag。

    YT extraction 經常產出 '其他 (晶圓代工)' / '其他(AI晶片代工)' / '其他 大盤指標'
    這類前綴 '其他' 的 placeholder，全部不要。
    """
    if not tag or not isinstance(tag, str):
        return True
    t = tag.strip()
    if t.startswith('其他'):
        return True
    if t.lower() == 'ai' or t == '無' or t == '':
        return True
    return False


def _build_dyn_tags_index():
    """從 sector_tags_dynamic.parquet 建 ticker → ordered list[theme] 索引。

    Filters:
      - confidence >= 70
      - ticker_suspicious == False
      - date 在 _DYN_TAGS_TTL_DAYS 天內
      - tag 不是 '其他*' / 'AI' / '' 等 junk (見 _is_junk_yt_tag)

    排序：date 降序，新鮮 mention 優先；同 ticker 內以 alias 去重；
    每 ticker 最多保留 5 個 (避免 +N 失控)。
    """
    import pandas as pd
    from pathlib import Path

    path = Path(__file__).resolve().parent / 'data' / 'sector_tags_dynamic.parquet'
    if not path.exists():
        return {}
    df = pd.read_parquet(path)
    if df.empty:
        return {}
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=_DYN_TAGS_TTL_DAYS)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    mask = (
        (df['confidence'] >= 70)
        & (df['ticker_suspicious'] == False)
        & (df['date'] >= cutoff)
    )
    clean = df[mask].sort_values('date', ascending=False)

    MAX_PER_TICKER = 5
    idx = {}
    for _, row in clean.iterrows():
        ticker = row.get('ticker')
        if not ticker:
            continue
        tags = row.get('tags')
        if tags is None:
            continue
        bucket = idx.setdefault(ticker, {'order': [], 'seen': set()})
        if len(bucket['order']) >= MAX_PER_TICKER:
            continue
        for tag in tags:
            if _is_junk_yt_tag(tag):
                continue
            cleaned = _clean_yt_tag(tag)
            if _is_junk_yt_tag(cleaned):
                continue
            k = _normalize_theme_key(cleaned)
            if not k or k in bucket['seen']:
                continue
            base_lower = cleaned.replace(' ', '').replace('　', '').lower()
            display = _THEME_ALIAS.get(base_lower, cleaned)
            if not display:
                continue
            bucket['order'].append(display)
            bucket['seen'].add(k)
            if len(bucket['order']) >= MAX_PER_TICKER:
                break
    return {tk: v['order'] for tk, v in idx.items()}


def _get_dyn_tags(stock_id):
    import time as _t
    now = _t.time()
    if (
        _DYN_TAGS_CACHE['data'] is None
        or now - _DYN_TAGS_CACHE['ts'] > _DYN_TAGS_RELOAD_SEC
    ):
        try:
            _DYN_TAGS_CACHE['data'] = _build_dyn_tags_index()
        except Exception as e:
            logger.warning("YT dynamic tags index build failed: %s", e)
            _DYN_TAGS_CACHE['data'] = {}
        _DYN_TAGS_CACHE['ts'] = now
    return _DYN_TAGS_CACHE['data'].get(stock_id, [])


def _build_news_tags_index():
    """從 data/news_themes.parquet 建 ticker → ordered list[theme] 索引。

    Filters:
      - confidence >= 70
      - date 在 _NEWS_TAGS_TTL_DAYS 天內
      - ticker 非空 (theme-only 條目跳過)
    Tags 去重：先 alias normalize，按 (date desc, confidence desc) 排序，
    每 ticker 最多 5 個 (避免 +N 失控)。
    """
    import pandas as pd
    from pathlib import Path as _P

    path = _P(__file__).resolve().parent / 'data' / 'news_themes.parquet'
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logger.warning("讀 news_themes.parquet 失敗: %s", e)
        return {}
    if df.empty:
        return {}

    df = df[df['ticker'].astype(str).str.strip() != ''].copy()
    if df.empty:
        return {}

    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=_NEWS_TAGS_TTL_DAYS)
    df = df[(df['date'] >= cutoff) & (df['confidence'] >= 70)].copy()
    if df.empty:
        return {}

    # 排序：新鮮 + 高信心優先
    df = df.sort_values(['date', 'confidence'], ascending=[False, False])

    MAX_PER_TICKER = 5
    idx: dict[str, dict] = {}
    for _, row in df.iterrows():
        ticker = str(row['ticker'])
        theme = str(row['theme'])
        if _is_junk_yt_tag(theme):
            continue
        cleaned = _clean_yt_tag(theme)
        if _is_junk_yt_tag(cleaned):
            continue
        k = _normalize_theme_key(cleaned)
        if not k:
            continue
        bucket = idx.setdefault(ticker, {'order': [], 'seen': set()})
        if len(bucket['order']) >= MAX_PER_TICKER:
            continue
        if k in bucket['seen']:
            continue
        base_lower = cleaned.replace(' ', '').replace('　', '').lower()
        display = _THEME_ALIAS.get(base_lower, cleaned)
        if not display:
            continue
        bucket['order'].append(display)
        bucket['seen'].add(k)
    return {tk: v['order'] for tk, v in idx.items()}


def _get_news_tags(stock_id):
    import time as _t
    now = _t.time()
    if (
        _NEWS_TAGS_CACHE['data'] is None
        or now - _NEWS_TAGS_CACHE['ts'] > _NEWS_TAGS_RELOAD_SEC
    ):
        try:
            _NEWS_TAGS_CACHE['data'] = _build_news_tags_index()
        except Exception as e:
            logger.warning("News tags index build failed: %s", e)
            _NEWS_TAGS_CACHE['data'] = {}
        _NEWS_TAGS_CACHE['ts'] = now
    return _NEWS_TAGS_CACHE['data'].get(stock_id, [])


def _get_tv_industry_zh(stock_id):
    """TV industry 英文 → 中文 fallback。沒命中 mapping 則退回英文。"""
    try:
        from peer_comparison import _fetch_tv_industry_map
        tv_map = _fetch_tv_industry_map()
        if tv_map is None or stock_id not in tv_map.index:
            return ''
        ind = tv_map.loc[stock_id, 'industry']
        if not isinstance(ind, str):
            return ''
        return _TV_INDUSTRY_ZH.get(ind, ind)
    except Exception:
        return ''


def _theme_tags_short(stock_id):
    """4 層融合：manual.json → News (30d) → YT dynamic (180d) → TV industry 中文。

    最多顯示 2 個 + 餘數。各層去重 (whitespace-insensitive + alias)。Empty -> ''.
    """
    themes = []
    seen = set()

    # Layer 1: sector_tags_manual.json (高精度 curated catalyst)
    try:
        from peer_comparison import get_ticker_themes as _gtt
        for t in _gtt(stock_id) or []:
            zh_raw = t.get('zh', t.get('id', '')) if isinstance(t, dict) else ''
            zh = _clean_yt_tag(zh_raw)  # 截掉「（解釋）」尾巴與 YT/News 一致
            k = _normalize_theme_key(zh)
            if zh and k and k not in seen:
                base_lower = zh.replace(' ', '').replace('　', '').lower()
                display = _THEME_ALIAS.get(base_lower, zh)
                if display:
                    themes.append(display)
                    seen.add(k)
    except Exception:
        pass

    # Layer 2: news_themes.parquet (News RSS LLM 萃取，30d 新鮮)
    try:
        for tag in _get_news_tags(stock_id):
            k = _normalize_theme_key(tag)
            if k and k not in seen:
                themes.append(tag)
                seen.add(k)
    except Exception:
        pass

    # Layer 3: sector_tags_dynamic (YT 法說會萃取，180d 內)
    try:
        for tag in _get_dyn_tags(stock_id):
            k = _normalize_theme_key(tag)
            if k and k not in seen:
                themes.append(tag)
                seen.add(k)
    except Exception:
        pass

    # Layer 4: TV industry 中文 (前 3 層皆空時 fallback，避免 catalyst 被 biz segment 稀釋)
    if not themes:
        ind_zh = _get_tv_industry_zh(stock_id)
        if ind_zh:
            themes.append(ind_zh)

    if not themes:
        return ''
    head = ' / '.join(themes[:2])
    if len(themes) > 2:
        head += f' +{len(themes) - 2}'
    return head


def _convergence_label(stock_id, conv_map):
    """產生共振標記文字（QM/Value/Swing/MeanRev 多選股交集）"""
    c = conv_map.get(stock_id)
    if not c:
        return ''
    modes = c['modes']
    tier = c['tier']
    has_val = 'value' in modes
    has_mom = bool(set(modes) & {'momentum', 'swing', 'qm'})
    if has_val and has_mom:
        return f'T{tier} 動能+價值'
    return f'T{tier} {"+".join(modes)}'
