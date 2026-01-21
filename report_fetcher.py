
import requests
import re
from datetime import datetime
import streamlit as st

# GitHub Repo Config
GITHUB_REPO = "SheenArtem/stock-research-reports"
BRANCH = "main"

def get_latest_report(ticker):
    """
    Fetches the content of the latest report for the given ticker.
    
    Args:
        ticker (str): The stock ticker (e.g., '2330.TW', 'TSM', 'AAPL').
        
    Returns:
        tuple: (markdown_content, report_date_str, report_url)
        - If no report found: (None, None, None)
    """
    # 1. Normalize Ticker (Match Folder Name)
    # 規則: 純數字 (台股) 去掉 .TW，英文 (美股) 保持原樣
    if ticker.replace('.', '').isdigit(): # Handle '2330' strictly
         stock_id = ticker
    elif ticker[0].isdigit(): # Handle '2330.TW' or '2330.TWO'
        stock_id = ticker.split('.')[0]
    else:
        # 美股 (e.g., TSM, AAPL)
        stock_id = ticker

    # 2. List files in the directory via GitHub API
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/reports/{stock_id}"
    
    try:
        # Use a short timeout to prevent hanging the UI
        response = requests.get(api_url, timeout=5)
        
        if response.status_code == 404:
            return None, None, None
        
        if response.status_code != 200:
            print(f"GitHub API Error: {response.status_code}")
            return None, None, None
            
        files = response.json()
        
        # 3. Filter and Parse filenames
        # Pattern: {stock_id}_{date}_report.md
        # Regex to capture date: \d{4}-\d{2}-\d{2}
        report_files = []
        
        for file in files:
            filename = file['name']
            if filename.endswith('.md'):
                # Extract date
                match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                if match:
                    date_str = match.group(1)
                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                        report_files.append({
                            'filename': filename,
                            'date': date_obj,
                            'download_url': file['download_url'],
                            'html_url': file['html_url']
                        })
                    except ValueError:
                        continue # Invalid date format
        
        if not report_files:
            return None, None, None
            
        # 4. Sort by date (descending) and get the latest
        report_files.sort(key=lambda x: x['date'], reverse=True)
        latest_report = report_files[0]
        
        # 5. Fetch Content
        content_resp = requests.get(latest_report['download_url'], timeout=10)
        if content_resp.status_code == 200:
            # Decode content (utf-8)
            content = content_resp.text
            return content, latest_report['date'].strftime('%Y-%m-%d'), latest_report['html_url']
        
    except Exception as e:
        print(f"Error fetching report: {e}")
        return None, None, None

    return None, None, None
