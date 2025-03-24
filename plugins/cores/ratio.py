from typing import Optional
from configs.const import tcbs_headers, _FINANCIAL_REPORT_PERIOD_MAP 
from pandas import json_normalize
import requests


def fetch_ratio(symbol, period: Optional[str] = 'quarter'):
    """
    Truy xuất thông tin chỉ số tài chính của một công ty theo mã chứng khoán từ nguồn dữ liệu TCBS.
    Tham số:
        - period (str): Chu kỳ báo cáo tài chính cần truy xuất. Mặc định là 'quarter'.
    """
    if period not in ['year', 'quarter']:
        raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
    
    effective_period = _FINANCIAL_REPORT_PERIOD_MAP.get(period, period) if period else period
    effective_get_all = True
    
    url = f'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/financialratio?yearly={effective_period}&isAll={str(effective_get_all).lower()}'

    try:
        response = requests.get(url, headers=tcbs_headers)
        response.raise_for_status()
        
    except requests.RequestException as e:
        raise ValueError(f"Lỗi khi truy xuất dữ liệu chỉ số tài chính cho {symbol}: {e}")

    df = json_normalize(response.json())

    if df.empty:
        raise ValueError(f"Không có dữ liệu chỉ số tài chính cho {symbol}")

    if 'ticker' in df.columns:
        df.drop(columns=['ticker'], inplace=True)

    df['year'] = df['year'].astype(str)

    if period == 'year':
        if 'quarter' in df.columns:
            df.drop(columns=['quarter'], inplace=True)
        df.rename(columns={'year': 'period'}, inplace=True)
    elif period == 'quarter':
        if 'quarter' in df.columns:
            df['period'] = df['year'] + '-Q' + df['quarter'].astype(str)
        else:
            df['period'] = df['year'] 

    # Sắp xếp lại thứ tự cột để 'period' là cột đầu tiên
    cols = ['period'] + [col for col in df.columns if col != 'period']
    df = df[cols]

    df['ticker'] = symbol
    # Đặt 'period' làm index
    
    if period == 'quarter':
        df.set_index('period', inplace=True)

    return df
