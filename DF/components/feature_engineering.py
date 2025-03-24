from haystack.nodes import BaseComponent
from config import ALLOWED_MODES, MAIN_FEATURE_COLS, INPUT_FOLDER
import pandas as pd


class FeatureEngineering(BaseComponent):
    """Component for preprocess the data and apply feature engineering"""

    outgoing_edges = 1

    def __init__(self):
        self.stock_df = self.read_stock_data()
        # self.export_folder = 'output'
        # os.makedirs(self.export_folder, exist_ok=True)

    def read_stock_data(self, file_path=f'{INPUT_FOLDER}/stock_price.csv'):
        """Reads stock data from a CSV file."""
        stock_df = pd.read_csv(file_path)
        stock_df['date'] = pd.to_datetime(stock_df['date'])
        stock_df.dropna(inplace=True)
        return stock_df

    def run(self, ticker, mode):
        # ticker = kwargs.get("ticker")
        # mode = kwargs.get("mode")

        if mode not in ALLOWED_MODES:
            raise ValueError(f"Invalid mode")

        list_windows_ma = ALLOWED_MODES[mode]["list_windows_ma"]
        list_lags = ALLOWED_MODES[mode]["list_lags"]

        data = self.get_data(ticker)
        if mode == '3d':
            data = self.__group_data(data=data, n_day=3)
        elif mode == '1w':
            data = self.__group_data(data=data, n_day=7)
        elif mode == '2w':
            data = self.__group_data(data=data, n_day=14)
        else:
            data = data

        fe_data = self.__add_ma_features(data, list_windows_ma)
        fe_data = self.__add_lag_features(data, list_lags)
        fe_data = self.__add_middle_features(data, list_lags)
        fe_data.fillna(0, inplace=True)


        return {"output_1": {
                            "data": fe_data, 
                            "ticker": ticker,
                            "mode": mode,
                            }
                }, "output_1"
    
    def run_batch(self, inputs: list):
        results = [self.run(ticker, mode) for ticker, mode in inputs]
        return [{"file_path": r[0]["file_path"]} for r in results], "output_1"
    

    def get_data(self, ticker):
        """
        Filters and processes for the given ticker.
        """

        data = self.stock_df[self.stock_df['ticker'] == ticker]
        data['date'] = pd.to_datetime(data['date']) 
        data.reset_index(drop=True, inplace=True)
        data = data.drop(columns=['ticker', 'type'])

        return data
    

    def __group_data(self, data, n_day):
        grouped_data = []
        for i in range(0, len(data), n_day):
            group = data.iloc[i:i+n_day]
            mean_values = group.mean(numeric_only=True)
            data_start = group['date'].iloc[0].date()
            data_end = group['date'].iloc[-1].date()
            mean_values['date_start'] = data_start
            mean_values['date_end'] = data_end
            grouped_data.append(mean_values)

        new_data = pd.DataFrame(grouped_data)
        new_data['date_start'] = pd.to_datetime(new_data['date_start'])
        new_data['date_end'] = pd.to_datetime(new_data['date_end'])

        return new_data

  
    def __add_ma_features(self, data, list_windows_ma):
        """
        Adds moving average features to the data.
        """

        for col in MAIN_FEATURE_COLS:
            for w in list_windows_ma:
                data[f'{col}_MA{w}'] = data[col].shift(1).rolling(window=w).mean()

        return data
    
    def __add_lag_features(self, data, list_lags):
        """
        Add lag features to the data
        """

        for col in MAIN_FEATURE_COLS:
            for lag in list_lags:
                data[f'{col}_Lag{lag}'] = data[col].shift(lag)
        
        return data
    
    def __add_middle_features(self, data, list_lags):
        data['middle_open_close'] = (data['open'] + data['close']) / 2
        data['middle_high_low'] = (data['high'] + data['low']) / 2

        for lag in list_lags:
            data[f'middle_open_close_Lag{lag}'] = data[f'middle_open_close'].shift(lag)
            data[f'middle_high_low_Lag{lag}'] = data[f'middle_high_low'].shift(lag)
        
        data.fillna(0, inplace=True)
        
        return data

    