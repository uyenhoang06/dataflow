from haystack import Pipeline
from components.feature_engineering import FeatureEngineering
from components.model_selection import ModelSelection
from components.prediction import Prediction
import pandas as pd
from config import INPUT_FOLDER
import warnings
import os

warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)


def create_pipeline(target):
    # Initialize components
    feature_engineering = FeatureEngineering()
    model_selection = ModelSelection(target=target)
    prediction = Prediction()

    # Create pipeline
    pipeline = Pipeline()
    pipeline.add_node(component=feature_engineering, name="FeatureEngineering", inputs=["Query"])
    pipeline.add_node(component=model_selection, name="ModelSelection", inputs=["FeatureEngineering.output_1"])
    pipeline.add_node(component=prediction, name="Prediction", inputs=["ModelSelection.output_2"])

    return pipeline

def run():
    file_path = f'{INPUT_FOLDER}/ticker_result.csv'
    tickers_data = pd.read_csv(file_path)
    ticker_list = tickers_data.iloc[:, 0].tolist()
    print(ticker_list)   

    folder_path = f'output'
    folder_names = [name for name in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, name))]
    print(folder_names)

    filtered_ticker_list = [ticker for ticker in ticker_list if ticker not in folder_names]
    print(filtered_ticker_list)

    for ticker in filtered_ticker_list:
        for mode in ['1d', '3d', '1w', '2w']:
            for target in ['middle_high_low', 'middle_open_close']:
                print(f"\nProcess {ticker} - {mode} - {target}")
                pipeline = create_pipeline(target)
                result = pipeline.run(params={"ticker": ticker, "mode": mode})
                result

if __name__ == "__main__":
    run()
