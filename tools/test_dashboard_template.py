"""
測試儀表板 HTML 模板渲染（不走 Claude CLI，直接注入假資料）
用法: python tools/test_dashboard_template.py
產出: tools/_test_dashboard_output.html → 雙擊在瀏覽器開啟驗證
"""
import json
import os
import sys
from datetime import datetime

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_TEMPLATE_PATH = os.path.join(_ROOT, "prompts", "report_dashboard_template.html")
_OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_test_dashboard_output.html")


def sample_data():
    """祥碩 5269.TW 仿真資料"""
    return {
        "meta": {
            "ticker": "5269.TW",
            "name": "祥碩科技",
            "market": "TW",
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "last_price": 2155.0,
            "change_pct": 1.42,
        },
        "summary": {
            "verdict": "買進",
            "confidence": "高",
            "trigger_score": 5.8,
            "trend_score": 7.0,
            "percentile": 82,
            "regime": "trending",
            "position_adjustment": 1.0,
            "one_liner": "AMD 平台擴張 + PCIe Gen5 FinFET 2027 ASP 跳升雙驅動，分析師系統性低估長期 EPS 上檔。",
            "key_points": [
                {"text": "月營收 3 月 YoY +39.9%，高基期下仍加速", "direction": "bull"},
                {"text": "PCIe Gen5 FinFET 2027 量產，ASP 可望 3-4 倍跳升", "direction": "bull"},
                {"text": "Techpoint 車用 ISP 2028 貢獻未被計入", "direction": "bull"},
                {"text": "台幣若升值到 28 以下，毛利壓力大", "direction": "bear"},
                {"text": "Astera Labs 若切入 Gen5 邊緣市場，ASP 保護存疑", "direction": "bear"},
            ],
            "fundamentals": {
                "pe": "29.6x", "eps": "72.7",
                "yield": "3.01%", "pb": "6.3x", "roe": "21.2%",
            },
            "monthly_revenue": [
                {"month": "2025-10", "rev": 12.21, "yoy": 67.1},
                {"month": "2025-11", "rev": 11.86, "yoy": 85.2},
                {"month": "2025-12", "rev": 11.20, "yoy": 50.0},
                {"month": "2026-01", "rev": 13.46, "yoy": 47.5},
                {"month": "2026-02", "rev": 8.93, "yoy": 17.65},
                {"month": "2026-03", "rev": 12.58, "yoy": 39.9},
            ],
        },
        "technical": {
            "triggers": [
                {"type": "bull", "text": "MACD 週線黃金交叉確認", "weight": 1.5},
                {"type": "bull", "text": "RVOL = 2.3x（大量突破 60 日高）", "weight": 1.2},
                {"type": "bull", "text": "Supertrend 週線翻多", "weight": 1.0},
                {"type": "bull", "text": "外資近 5 日淨買超 8,500 張", "weight": 0.8},
                {"type": "bear", "text": "RSI(14) = 72，短期超買", "weight": -0.5},
                {"type": "neutral", "text": "融資餘額增加 5.2%（中性偏警戒）", "weight": -0.3},
            ],
            "signals": [
                {"category": "趨勢", "indicator": "MA20/60/200", "value": "多頭排列", "signal": "多", "note": "週線 MA60 上揚"},
                {"category": "趨勢", "indicator": "Supertrend", "value": "週線 BUY", "signal": "多", "note": "2026-03-15 翻多"},
                {"category": "動能", "indicator": "MACD", "value": "DIF>DEA", "signal": "多", "note": "柱狀體放大"},
                {"category": "動能", "indicator": "KD(9)", "value": "K=78, D=72", "signal": "多", "note": "高檔鈍化"},
                {"category": "動能", "indicator": "RSI(14)", "value": "72", "signal": "中", "note": "接近超買"},
                {"category": "量能", "indicator": "RVOL", "value": "2.3x", "signal": "多", "note": "大量突破"},
                {"category": "波動", "indicator": "BB 寬度", "value": "12.5%", "signal": "中", "note": "擴張中"},
                {"category": "型態", "indicator": "K 線", "value": "多頭吞噬", "signal": "多", "note": "3 月初"},
            ],
        },
        "chip": {
            "rows": [
                {"category": "外資", "data": "近 5 日淨買 8,523 張", "direction": "正", "impact": "外資回補中"},
                {"category": "投信", "data": "近 5 日淨買 1,200 張", "direction": "正", "impact": "投信作帳進場"},
                {"category": "自營商", "data": "近 5 日淨賣 -320 張", "direction": "負", "impact": "自營避險"},
                {"category": "融資", "data": "增加 5.2%", "direction": "中", "impact": "散戶追價警戒"},
                {"category": "融券", "data": "減少 12%", "direction": "正", "impact": "軋空動能"},
                {"category": "借券賣出", "data": "餘額下降 8%", "direction": "正", "impact": "空頭撤退"},
            ],
            "foreign_flow": [
                {"date": "04-07", "net": 1523},
                {"date": "04-08", "net": -420},
                {"date": "04-09", "net": 2100},
                {"date": "04-10", "net": 3850},
                {"date": "04-11", "net": 1470},
            ],
        },
        "valuation": {
            "current_price": 2155,
            "scenarios": [
                {
                    "scenario": "bear", "eps_assumption": "2026E 85 元",
                    "pe_assumption": "18x (歷史低)",
                    "target": 1530, "trigger": "AMD 平台失速 + Gen5 延遲",
                },
                {
                    "scenario": "base", "eps_assumption": "2026E 100 元",
                    "pe_assumption": "25x (歷史中位)",
                    "target": 2500, "trigger": "AMD 持續增長 + Gen4 穩定",
                },
                {
                    "scenario": "bull", "eps_assumption": "2026E 120 元",
                    "pe_assumption": "32x (AI 題材加乘)",
                    "target": 3840, "trigger": "Gen5 提前 + Techpoint ISP 貢獻",
                },
            ],
            "pe_history": {
                "current": 29.6, "low": 15.2, "median": 23.8, "high": 42.5,
            },
            "peer_comparison": [
                {"ticker": "5269.TW", "name": "祥碩", "pe": 29.6, "pb": 6.3, "yield": 3.01},
                {"ticker": "3583.TW", "name": "辛耘", "pe": 18.2, "pb": 2.8, "yield": 4.50},
                {"ticker": "6515.TW", "name": "穎崴", "pe": 32.1, "pb": 5.1, "yield": 1.80},
                {"ticker": "2449.TW", "name": "京元電子", "pe": 15.8, "pb": 2.2, "yield": 5.20},
            ],
            "eps_forecast": [
                {"year": "2024A", "bear": 51.57, "base": 51.57, "bull": 51.57},
                {"year": "2025A", "bear": 72.7, "base": 72.7, "bull": 72.7},
                {"year": "2026E", "bear": 85, "base": 100, "bull": 120},
                {"year": "2027E", "bear": 100, "base": 132, "bull": 165},
                {"year": "2028E", "bear": 112, "base": 158, "bull": 205},
            ],
        },
        "bull_bear": {
            "bull_points": [
                {"text": "AMD AM5 桌面平台市佔 2026 破 45%，每塊主機板 2-3 顆 IC", "weight": "高"},
                {"text": "PCIe Gen5 FinFET 2027 量產，單價 3-4 倍跳升", "weight": "高"},
                {"text": "Techpoint 車用 ISP 新品 2028 test chip，ADAS 邊緣市場", "weight": "中"},
                {"text": "CXL 2.0 邊緣伺服器市場 2028 爆發，祥碩定位成本敏感", "weight": "中"},
                {"text": "Win10 停止支援換機潮驅動 USB4 需求", "weight": "中"},
            ],
            "bear_points": [
                {"text": "台幣升值每 1 元影響 0.5-1 億 NTD 稅後淨利", "weight": "中"},
                {"text": "Astera Labs / Broadcom 若切入邊緣 Gen5 市場", "weight": "中"},
                {"text": "Techpoint 車用認證 3-5 年，ISP 產品跳票風險", "weight": "低"},
                {"text": "AMD 因關稅/供應鏈丟市佔的尾部風險", "weight": "低"},
            ],
            "risks": [
                {"risk": "AMD 平台失速", "severity": "中", "horizon": "短期",
                 "description": "若 AMD 因競爭丟失市佔，對祥碩 EPS 直接衝擊 10-15 元"},
                {"risk": "台幣升值", "severity": "中", "horizon": "短期",
                 "description": "60%+ 營收美元計價，升值壓毛利"},
                {"risk": "Gen5 競爭進入", "severity": "中", "horizon": "長期",
                 "description": "2027 年若 Astera Labs 切入邊緣市場，ASP 保護存疑"},
                {"risk": "Techpoint 整合延遲", "severity": "低", "horizon": "長期",
                 "description": "車用認證週期 3-5 年，ISP 產品跳票"},
            ],
            "recommendation": {
                "entry_zone": "1,980 - 2,100 元（回測週線 MA20）",
                "stop_loss": "1,850 元（跌破 ATR 2x 支撐）",
                "position_size": "標準倉位（Regime = trending，無需減碼）",
                "strategy": "分批佈局 2026E 目標 2,500 元；若 Gen5 news 催化衝 3,000 元加碼",
            },
        },
    }


def main():
    with open(_TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    data = sample_data()
    json_str = json.dumps(data, ensure_ascii=False, indent=2)

    html = template.replace("__TITLE__", f"{data['meta']['ticker']} {data['meta']['name']} 研究報告")
    html = html.replace("__REPORT_JSON__", json_str)

    with open(_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"[OK] Test dashboard saved to:\n  {_OUTPUT_PATH}")
    print(f"\n雙擊 _test_dashboard_output.html 在瀏覽器開啟驗證")


if __name__ == "__main__":
    main()
