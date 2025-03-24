import requests
from pandas import json_normalize
from typing import Optional    
from configs.const import tcbs_headers, _FINANCIAL_REPORT_PERIOD_MAP, _BASE_URL, _ANALYSIS_URL, _FINANCIAL_REPORT_MAP

def _get_report (symbol, report_type:Optional[str]='balance_sheet', period:Optional[str]='quarter'):
    """
    Truy xuất thông tin báo cáo tài chính của một công ty theo mã chứng khoán từ nguồn dữ liệu TCBS.
    Tham số:
        - report_type (str): Loại báo cáo tài chính cần truy xuất. Mặc định là 'balance_sheet'.
        - period (str): Chu kỳ báo cáo tài chính cần truy xuất. Mặc định là 'quarterly'.
    """
    # Use instance attributes if parameters not provided
    if period not in ['year', 'quarter']:
        raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
    
    effective_report_type = _FINANCIAL_REPORT_MAP.get(report_type, report_type) if report_type else report_type
    effective_period = _FINANCIAL_REPORT_PERIOD_MAP.get(period, period) if period else period

    url = f'{_BASE_URL}/{_ANALYSIS_URL}/v1/finance/{symbol}/{effective_report_type}'
    params = {'yearly': effective_period}
            
    try:
        response = requests.get(url, params=params, headers=tcbs_headers)
        response.raise_for_status()  
    except requests.RequestException as e:
        raise ValueError(f"Lỗi khi truy xuất dữ liệu báo cáo tài chính cho {symbol}: {e}")

        
    df = json_normalize(response.json())

    df['year'] = df['year'].astype(str)
    df['quarter'] = df['quarter'].astype(str)

    df.dropna(axis=1, how='all', inplace=True)

    if effective_report_type != 'cash_flow':
        if period == 'year':
            df.drop(columns='quarter', inplace=True)
            df.rename(columns={'year':'period'}, inplace=True)
        elif period == 'quarter':
            df['period'] = df['year'] + '-Q' + df['quarter']
            
            cols = df.columns.tolist()
            cols = cols[-1:] + cols[:-1]
            df = df[cols]

    df.set_index('period', inplace=True)

    df.name = symbol
    return df

def balance_sheet (symbol, period:Optional[str]='quarter'):
    """
    Truy xuất thông tin bản cân đối kế toán (rút gọn) của một công ty theo mã chứng khoán từ nguồn dữ liệu TCBS.
    Tham số:
        - period (str): Chu kỳ báo cáo tài chính cần truy xuất. 
    """
    if period not in ['year', 'quarter']:
        raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
    
    df = _get_report(symbol, 'balance_sheet', period=period)
    return df
    
def income_statement (symbol, period:Optional[str]='quarter'):
    """
    Truy xuất thông tin báo cáo doanh thu của một công ty theo mã chứng khoán từ nguồn dữ liệu TCBS.

    Tham số:
        - period (str): Chu kỳ báo cáo tài chính cần truy xuất. 
    """
    if period not in ['year', 'quarter']:
        raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
    
    df = _get_report(symbol,'income_statement', period=period)
    
    return df
    
def cash_flow (symbol, period:Optional[str]='quarter'):
    """
    Truy xuất thông tin báo cáo lưu chuyển tiền tệ của một công ty theo mã chứng khoán từ nguồn dữ liệu TCBS.

    Tham số:
        - period (str): Chu kỳ báo cáo tài chính cần truy xuất. 
    """
    if period not in ['year', 'quarter']:
        raise ValueError("Kỳ báo cáo tài chính không hợp lệ. Chỉ chấp nhận 'year' hoặc 'quarter'.")
    
    df = _get_report(symbol, 'cash_flow', period=period)
    
    return df
