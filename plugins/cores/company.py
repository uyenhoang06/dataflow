import requests
import pandas as pd
import numpy as np

def fetch_company_info(symbol):
    url = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{symbol}/overview'
    
    try:
        response = requests.get(url)
        data = response.json() if response.status_code == 200 else None
    except Exception as e:
        print(f"Lỗi khi lấy dữ liệu: {e}")
        data = None
            
    selected_columns = ['ticker', 'shortName', 'industry', 'exchange', 'establishedYear', 'foreignPercent']
    
    if not data:
        df = pd.DataFrame([[symbol, np.nan, np.nan, np.nan, 0, 0]], columns=selected_columns)
    else:
        df = pd.json_normalize(data)
        
    df = df[selected_columns]

    df = df.rename(columns={
        'ticker': 'ticker',
        'shortName': 'name',
        'industry': 'industry',
        'exchange': 'exchange',
        'establishedYear': 'established_year',
        'foreignPercent': 'foreign_percent'
    })
    
    return df