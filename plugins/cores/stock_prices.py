import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Optional
from cores.utils import get_asset_type, to_df
from configs.const import _BASE_URL, _FUTURE_URL, _INTERVAL_MAP, _STOCKS_URL, tcbs_headers

def fetch_history(symbol: str, start: str, end: Optional[str] = None, interval: str = "1D") -> pd.DataFrame:
    """
    Truy xuất dữ liệu lịch sử từ TCBS. Nếu khoảng thời gian > 1 năm, chia nhỏ thành từng năm để tránh lỗi.

    Tham số:
        - symbol: Mã chứng khoán.
        - start: Ngày bắt đầu (YYYY-MM-DD).
        - end: Ngày kết thúc (YYYY-MM-DD).
        - interval: Khung thời gian (1m, 5m, 15m, 30m, 1H, 1D, 1W, 1M).

    Trả về:
        - DataFrame chứa dữ liệu lịch sử (chỉ bao gồm các ngày có dữ liệu).
    """

    if not start:
        raise ValueError("Thời gian bắt đầu (start) không được để trống.")

    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Định dạng ngày bắt đầu (start) không hợp lệ. Dùng YYYY-MM-DD.")

    if end:
        try:
            end_date = datetime.strptime(end, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Định dạng ngày kết thúc (end) không hợp lệ. Dùng YYYY-MM-DD.")
    else:
        end = datetime.today().strftime("%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")

    if end_date < start_date:
        raise ValueError("Thời gian kết thúc không thể sớm hơn thời gian bắt đầu.")

    asset_type = get_asset_type(symbol=symbol)

    def fetch_chunk(start_date, end_date):
        """Lấy dữ liệu cho một đoạn thời gian nhỏ."""
        end_stamp = int(end_date.timestamp())
        count_back = (end_date - start_date).days

        if interval in ["1D", "1W", "1M"]:
            end_point = "bars-long-term"
        elif interval in ["1m", "5m", "15m", "30m", "1H"]:
            end_point = "bars"

        interval_value = _INTERVAL_MAP[interval]

        if asset_type == "derivative":
            url = f"{_BASE_URL}/{_FUTURE_URL}/v2/stock/{end_point}?resolution={interval_value}&ticker={symbol}&type={asset_type}&to={end_stamp}&countBack={count_back}"
        else:
            url = f"{_BASE_URL}/{_STOCKS_URL}/v2/stock/{end_point}?resolution={interval_value}&ticker={symbol}&type={asset_type}&to={end_stamp}&countBack={count_back}"

        response = requests.get(url, headers=tcbs_headers)
        if response.status_code != 200:
            print(f"Lỗi tải dữ liệu {response.status_code}: {response.reason}")
            return pd.DataFrame()

        json_data = response.json()
        return to_df(json_data)
        # return json_data

    combined_data = []
    current_start = start_date

    while current_start <= end_date:
        next_year_date = datetime(current_start.year + 1, current_start.month, 1)
        chunk_end = min(next_year_date - timedelta(days=1), end_date)

        data_chunk = fetch_chunk(current_start, chunk_end)

        if not data_chunk.empty:
            combined_data.append(data_chunk)
        else:
            pass

        current_start = next_year_date

    if not combined_data:
        print(f"Không tìm thấy dữ liệu cho {symbol}. Mã này có thể chưa lên sàn trong khoảng thời gian yêu cầu.")
        return pd.DataFrame()

    df = pd.concat(combined_data, ignore_index=True)
    df = df[(df['date'] >= start) & (df['date'] <= end)]
    df['type'] = asset_type

    return df
