import requests
import json

url = "https://api.finmindtrade.com/api/v4/data"
params = {
    "dataset": "TaiwanStockShareholding",
    "data_id": "2330",
    "start_date": "2024-01-01",
    "token": "" # Empty for now
}
try:
    r = requests.get(url, params=params)
    print(f"Status Code: {r.status_code}")
    data = r.json()
    print("Keys:", data.keys())
    if 'msg' in data:
        print("Msg:", data['msg'])
    if 'data' in data:
        print("Data Count:", len(data['data']))
        if len(data['data']) > 0:
            print("First item keys:", data['data'][0].keys())
            print("First item:", data['data'][0])
    else:
        print("No data key found.")
        print(data)
except Exception as e:
    print(e)
