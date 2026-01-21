import pandas as pd
import datetime

def generate_analysis_markdown(ticker, report_data, df_day, chip_data=None):
    """
    Generate a formatted Markdown report for the analysis.
    """
    if not report_data:
        return "無法生成報告：無分析數據"

    # Unpack Data
    scenario = report_data.get('scenario', {})
    action_plan = report_data.get('action_plan', {})
    checklist = report_data.get('checklist', {})
    trend_details = report_data.get('trend_details', [])
    trigger_details = report_data.get('trigger_details', [])
    
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    last_close = df_day['Close'].iloc[-1] if not df_day.empty else 0
    
    md = []
    
    # Header
    md.append(f"# 📊 股票分析報告: {ticker}")
    md.append(f"**日期**: {current_date} | **收盤價**: {last_close:.2f}")
    md.append("---")
    
    # 1. AI 智能診斷 (Running Logic from common sense)
    s_title = scenario.get('title', 'N/A')
    s_desc = scenario.get('desc', 'N/A')
    md.append(f"## 🤖 AI 智能診斷")
    md.append(f"### {s_title}")
    md.append(f"> {s_desc}")
    md.append("")
    
    # 2. 核心操作策略
    if action_plan:
        strategy = action_plan.get('strategy', 'N/A')
        md.append("## 💡 核心操作策略")
        md.append(f"{strategy}")
        md.append("")
        
        # Table of Recommendations
        entry_desc = action_plan.get('rec_entry_desc', 'N/A')
        entry_range = f"{action_plan.get('rec_entry_low',0):.2f} ~ {action_plan.get('rec_entry_high',0):.2f}"
        tp_price = action_plan.get('rec_tp_price', 0)
        sl_price = action_plan.get('rec_sl_price', 0)
        rr = action_plan.get('rr_ratio', 0)
        
        md.append("| 項目 | 建議數值 | 說明 |")
        md.append("|---|---|---|")
        md.append(f"| **進場** | {entry_range} | {entry_desc} |")
        md.append(f"| **停利** | {tp_price:.2f} | 目標價 |")
        md.append(f"| **停損** | {sl_price:.2f} | {action_plan.get('rec_sl_method')} |")
        md.append(f"| **風報比** | 1 : {rr:.1f} | (獲利/風險) |")
        md.append("")

    # 3. 技術面詳情
    md.append("## 📈 技術面分析 (Technical)")
    
    md.append("### 📅 週線趨勢 (Long Term)")
    for item in trend_details:
        md.append(f"- {item}")
        
    md.append("")
    md.append("### ⚡ 日線訊號 (Short Term)")
    for item in trigger_details:
        md.append(f"- {item}")
    md.append("")

    # 4. 籌碼面 (若有)
    if chip_data:
        md.append("## 💰 籌碼面分析 (Chips)")
        # 簡易摘要籌碼狀況 (這裡只能根據已知變數生成，若無詳細 analysis text 則略過或簡單描述)
        # 嘗試從 trigger details 裡找籌碼相關的 (因為 analysis_engine 已經把籌碼因子加入 details 了)
        chip_related = [d for d in trend_details + trigger_details if "法人" in d or "融資" in d or "當沖" in d or "OBV" in d]
        if chip_related:
            for item in chip_related:
                md.append(f"- {item}")
        else:
            md.append("- (詳見圖表)")
        md.append("")

    # 5. 監控看板
    if checklist:
        md.append("## 🔔 盤中監控看板")
        if checklist.get('risk'):
            md.append("**🛑 風險預警 (停損/調節)**")
            for i in checklist['risk']: md.append(f"- {i}")
        
        if checklist.get('active'):
            md.append("\n**🚀 積極訊號 (追價/加碼)**")
            for i in checklist['active']: md.append(f"- {i}")
            
        if checklist.get('future'):
            md.append("\n**🔭 未來展望 (觀察)**")
            for i in checklist['future']: md.append(f"- {i}")
    
    md.append("\n---\n*本報告由 AI 自動生成，僅供參考，不代表投資建議。*")
    
    return "\n".join(md)
