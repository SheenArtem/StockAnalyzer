
import google.generativeai as genai
import pandas as pd

def generate_deep_analysis(api_key, stock_symbol, stock_name, data_summary):
    """
    Generates a deep investment analysis report using Google Gemini Pro.
    
    Args:
        api_key (str): User's Google AI Studio API Key.
        stock_symbol (str): e.g. "2330".
        stock_name (str): e.g. "TSMC".
        data_summary (dict): Dictionary containing Technical, Chip, and Fundamental data summaries.
                             Expected keys: 'technical', 'chips', 'fundamentals', 'price'.
    
    Yields:
        str: Streaming chunks of the analysis report.
    """
    if not api_key:
        yield "⚠️ 請先在側邊欄輸入 Google AI Studio API Key。"
        return

    try:
        genai.configure(api_key=api_key)
        
        # Define Model Hierarchy (High to Low)
        MODELS_TO_TRY = [
            'gemma-3-27b-it', # Highest Tier
            'gemma-3-12b-it',
            'gemma-3-4b-it',
            'gemma-3-1b-it'   # Fastest / Lowest Tier
        ]
        
        # Load Custom Prompt
        try:
            with open('ResearchPrompt.txt', 'r', encoding='utf-8') as f:
                custom_system_prompt = f.read()
        except FileNotFoundError:
            custom_system_prompt = "Role: Financial Analyst. Task: Analyze the following data."
        
        # Construct Prompt (Shared for all models)
        full_prompt = f"""
{custom_system_prompt}

# DATA CONTEXT (Provided by System)

[Price Action]
{data_summary.get('price', 'N/A')}

[Technical Analysis]
{data_summary.get('technical', 'N/A')}

[Chip Analysis (Institutional & Retail)]
{data_summary.get('chips', 'N/A')}

[Fundamentals (Valuation & Growth)]
{data_summary.get('fundamentals', 'N/A')}

Target: {stock_name} ({stock_symbol})

Note: You do not have autonomous web browsing tool. Please perform "Deep Research" simulation using your internal knowledge and the provided Data Context to the best of your ability. Prioritize the Data Context for recent numbers.

CRITICAL INSTRUCTION: The entire report MUST be written in Traditional Chinese (繁體中文). Do not output English unless it is a proper noun.

Begin Research Report:
"""

        # Fallback Loop
        last_error = None
        for model_name in MODELS_TO_TRY:
            try:
                # Inform user which model is being used (optional but helpful for debug)
                yield f"🔄 正在連線模型: **{model_name}** ...\n\n"
                
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(full_prompt, stream=True)
                
                # Check if stream works by trying to get first chunk
                stream_active = False
                for chunk in response:
                    stream_active = True
                    if chunk.text:
                        yield chunk.text
                
                if stream_active:
                    return # Success, exit function
                
            except Exception as e:
                last_error = e
                yield f"\n⚠️ 模型 {model_name} 連線失敗 ({str(e)})，嘗試降級...\n"
                continue
        
        # If loop finishes without success
        yield f"\n❌ 所有模型均嘗試失敗。最後錯誤: {str(last_error)}"

    except Exception as e:
        yield f"❌ System Error: {str(e)}\n\nPlease checks your API Key or Network connection."

def prepare_data_summary(df_day, df_week, chip_data, fund_data, tech_indicators):
    """
    Helper to serialize dataframes/dicts into a string summary for the Prompt.
    """
    summary = {}
    
    # 1. Price
    if not df_day.empty:
        last_day = df_day.iloc[-1]
        summary['price'] = f"""
        Date: {last_day.name.strftime('%Y-%m-%d')}
        Close: {last_day['Close']}
        Volume: {last_day['Volume']}
        Change: {last_day.get('Check_Trend_Vol', 'N/A')}
        """
    
    # 2. Technical
    # Extract key signals from tech_indicators text or raw data
    # Assuming tech_indicators is a dict or we just summarize df columns
    # Let's use the raw indicators from last row
    if not df_day.empty:
        last = df_day.iloc[-1]
        summary['technical'] = f"""
        MA5: {last.get('MA5', 0):.2f}, MA20: {last.get('MA20', 0):.2f}, MA60: {last.get('MA60', 0):.2f}
        RSI: {last.get('RSI', 0):.2f}
        KD: K={last.get('K', 0):.2f}, D={last.get('D', 0):.2f}
        MACD: Dif={last.get('MACD_dif', 0):.2f}, Dem={last.get('MACD_dem', 0):.2f}, Osc={last.get('MACD_osc', 0):.2f}
        Bollinger: Upper={last.get('BB_Upper', 0):.2f}, Lower={last.get('BB_Lower', 0):.2f}
        """
        
    # 3. Chips
    # Serialize chip dict
    chip_text = ""
    if chip_data:
        inst = chip_data.get('institutional')
        if inst is not None and not inst.empty:
            last_inst = inst.iloc[-1]
            chip_text += f"Institutional (Last Day): Foreign={last_inst.get('外資',0)}, Trust={last_inst.get('投信',0)}, Dealer={last_inst.get('自營商',0)}\n"
        
        margin = chip_data.get('margin')
        if margin is not None and not margin.empty:
            last_marg = margin.iloc[-1]
            chip_text += f"Margin: Balance={last_marg.get('融資餘額',0)}, Utilization={last_marg.get('MarginUtilization',0)}%\n"
            
        sh = chip_data.get('shareholding')
        if sh is not None and not sh.empty:
            last_sh = sh.iloc[-1]
            chip_text += f"Foreign Holding Ratio: {last_sh.get('ForeignHoldingRatio', 0)}%\n"
            
    summary['chips'] = chip_text
    
    # 4. Fundamentals
    # Serialize fund dict
    fund_text = ""
    if fund_data:
        # fund_data is 'fund_cache' dict + we might want to check the history df if available?
        # For now just use the summary dict
        for k, v in fund_data.items():
            if k not in ['Business Summary', 'Website']: # Skip long text
                fund_text += f"{k}: {v}\n"
    summary['fundamentals'] = fund_text
    
    return summary
