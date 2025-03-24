import requests
import pandas as pd
import time
from configs.const import tcbs_headers, vci_headers
from cores.company import fetch_company_info
from cores.stock_prices import fetch_history
from cores.ratio import fetch_ratio
from cores.financial_report import balance_sheet, income_statement, cash_flow
import numpy as np

# list ticker
def all_symbols():
    url = 'https://ai.vietcap.com.vn/api/get_all_tickers'
    
    try:
        response = requests.get(url, headers=vci_headers, timeout=10)
        response.raise_for_status()
        return pd.DataFrame(response.json().get('ticker_info', []))
    
    except requests.RequestException as e:
        raise ConnectionError(f"Lỗi khi lấy dữ liệu: {e}") from e
    

def fetch_tickers():
    t = all_symbols()
    
    tickers = t.loc[:, "ticker"]
    
    ticker_list = tickers.tolist()
    return ticker_list

# companies info
def fetch_companies():
    ticker_list = fetch_tickers()
    df_list = []
    for ticker in ticker_list:
        try:
            t = fetch_company_info(ticker)  
            df_list.append(t)
        except ValueError as e:
            pass  
    df = pd.concat(df_list, ignore_index=True)

    return df


# stock prices
def fetch_stock_prices(start: str, end:str):
    ticker_list = fetch_tickers()

    all_data = []
    
    for ticker in ticker_list:
        try:
            temp = fetch_history(ticker, start=start, end=end)

            if temp is None or temp.empty:
                continue

            all_data.append(temp)

        except Exception as e:
            print(f"Lỗi khi lấy dữ liệu {ticker}: {e}")

        time.sleep(8)

    if all_data:
        df = pd.concat(all_data, ignore_index=True)
        df = df.sort_values(by='date')    
    
    return df

# balance sheet
def fetch_year_financial_report_balance_sheet(): 
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = balance_sheet(ticker, period = 'year')
            df_list.append(t)
        except ValueError as e:
            pass  
    
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        
    return df

def fetch_year_financial_report_balance_sheet(): 
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = balance_sheet(ticker)
            df_list.append(t)
        except ValueError as e:
            pass  
    
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        
    return df

# cash flow
def fetch_year_financial_report_cash_flow(): 
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = cash_flow(ticker, period = 'year')
            df_list.append(t)
        except ValueError as e:
            pass  
    
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        
    return df

def fetch_quarter_financial_report_cash_flow(): 
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = cash_flow(ticker)
            df_list.append(t)
        except ValueError as e:
            pass  
    
    if df_list:
        df = pd.concat(df_list, ignore_index=True)

    return df

# income statement
def fetch_year_financial_report_income_state():
    ticker_list = fetch_tickers()

    
    df_list = []

    for ticker in ticker_list:
        try:
            t = income_statement(ticker, period = 'year')
            df_list.append(t)
        except ValueError as e:
            pass  
        
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
    return df

def fetch_quarter_financial_report_income_state():
    ticker_list = fetch_tickers()

    
    df_list = []

    for ticker in ticker_list:
        try:
            t = income_statement(ticker)
            df_list.append(t)
        except ValueError as e:
            pass  
        
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        
    return df


# ratio
def fetch_financial_ratio_quarter():
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = fetch_ratio(ticker) 
            
            selected_columns = ['ticker', 'quarter', 'year',
            "priceToEarning", "priceToBook", "roe", "roa", "earningPerShare",
            "currentPayment", "quickPayment", "debtOnEquity", "ebitOnInterest", "epsChange"
            ]
            
            tmp = t[[col for col in selected_columns if col in t.columns]]
            df_list.append(tmp)
        except ValueError as e:
            pass  
        
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
  
    return df

def fetch_financial_ratio_year():
    ticker_list = fetch_tickers()

    df_list = []

    for ticker in ticker_list:
        try:
            t = fetch_ratio(ticker, period='year')  
            selected_columns = [
            "priceToEarning", "priceToBook", "roe", "roa", "earningPerShare",
            "currentPayment", "quickPayment", "debtOnEquity", "ebitOnInterest", "epsChange"
            ]
            
            tmp = t[[col for col in selected_columns if col in t.columns]]
            df_list.append(tmp)
            
        except ValueError as e:
            pass  
        
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
  
    return df


