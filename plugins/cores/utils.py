import requests
import pandas as pd
import re

from configs.const import tcbs_headers

def all_symbols():
    url = 'https://ai.vietcap.com.vn/api/get_all_tickers'
    try:
        response = requests.get(url, headers=tcbs_headers, timeout=10)
        response.raise_for_status()
        
        df = pd.DataFrame(response.json().get('ticker_info', []))
        tickers = df.loc[:, "ticker"]
        
        ticker_list = tickers["ticker"].tolist()

        return ticker_list
    
    except requests.RequestException as e:
        raise ConnectionError(f"Lỗi khi lấy dữ liệu: {e}") from e
    

def get_asset_type(symbol: str) -> str:
    """
    Xác định loại tài sản dựa trên mã chứng khoán được cung cấp.
    Tham số: 
        - symbol (str): Mã chứng khoán hoặc mã chỉ số.
    Trả về:
        - 'index' nếu mã chứng khoán là mã chỉ số.
        - 'stock' nếu mã chứng khoán là mã cổ phiếu.
        - 'derivative' nếu mã chứng khoán là mã hợp đồng tương lai hoặc quyền chọn.
        - 'coveredWarr' nếu mã chứng khoán là mã chứng quyền.
    """
    symbol = symbol.upper()
    if symbol in ['VNINDEX', 'HNXINDEX', 'UPCOMINDEX', 'VN30', 'VN100', 'HNX30', 'VNSML', 'VNMID', 'VNALL', 'VNREAL', 'VNMAT', 'VNIT', 'VNHEAL', 'VNFINSELECT', 'VNFIN', 'VNENE', 'VNDIAMOND', 'VNCONS', 'VNCOND']:
        return 'index'
    elif len(symbol) == 3:
        return 'stock'
    elif len(symbol) in [7, 9]:
        fm_pattern = re.compile(r'VN30F\d{1,2}M')
        ym_pattern = re.compile(r'VN30F\d{4}')
        gb_pattern = re.compile(r'[A-Z]{3}\d{5}')
        bond_pattern = re.compile(r'[A-Z]{3}\d{6}')
        if bond_pattern.match(symbol) or gb_pattern.match(symbol):
            return 'bond'
        elif fm_pattern.match(symbol) or ym_pattern.match(symbol):
            return 'derivative'
        else:
            raise ValueError('Invalid derivative symbol. Symbol must be in format of VN30F1M, VN30F2024, GB10F2024')
    elif len(symbol) == 8:
        return 'coveredWarr'
    else:
        raise ValueError('Invalid symbol. Your symbol format is not recognized!')


def to_df(history_data: dict, floating: int = 2) -> pd.DataFrame:
    if not history_data or "data" not in history_data or not history_data['data']:
        return pd.DataFrame()  
    
    ticker = history_data.get("ticker", "Unknown")
    df = pd.DataFrame(history_data["data"])
    
    df["date"] = pd.to_datetime(df["tradingDate"], errors='coerce').dt.strftime('%Y-%m-%d')
    df.drop(columns=["tradingDate"], inplace=True)
    
    df["ticker"] = ticker
    
    df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].round(floating)
    
    df.rename(columns={"volume": "volumn"}, inplace=True)
    
    column_order = ['ticker', 'open', 'high', 'low', 'close', 'volumn', 'date']
    df = df[column_order]
    
    return df