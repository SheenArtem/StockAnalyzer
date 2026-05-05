import requests
from datetime import datetime, timedelta

def get_all_strikes():
    today = datetime.now()
    for delta in range(5):
        d = today - timedelta(days=delta)
        date_str = d.strftime('%Y/%m/%d')
        url = 'https://www.taifex.com.tw/cht/3/dlOptDataDown'
        payload = {
            'down_type': '1',
            'commodity_id': 'TXO',
            'queryStartDate': date_str,
            'queryEndDate': date_str,
        }
        try:
            resp = requests.post(url, data=payload, timeout=10)
            lines = resp.text.strip().split('\n')
            if len(lines) < 10: continue
            
            strikes_with_price = []
            months = []
            for line in lines[1:]:
                f = line.split(',')
                if len(f) > 4 and f[4].strip() == '賣權':
                    months.append(f[2].strip())
            
            if not months: continue
            near_month = sorted(list(set(months)))[0]
            
            for line in lines[1:]:
                f = line.split(',')
                if len(f) > 11 and f[4].strip() == '賣權' and f[2].strip() == near_month:
                    strike = f[3].strip()
                    close = f[6].strip()
                    settle = f[10].strip()
                    if close != '-' or settle != '-':
                        strikes_with_price.append((strike, close, settle))
            
            if strikes_with_price:
                print(f"Date: {date_str}, Near Month: {near_month}")
                for s, c, st in sorted(strikes_with_price, key=lambda x: float(x[0])):
                    if abs(float(s) - 39000) < 2000:
                        print(f"Strike: {s}, Close: {c}, Settle: {st}")
                return
        except Exception as e:
            print(e)

get_all_strikes()
