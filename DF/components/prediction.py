from haystack.nodes import BaseComponent
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
import os
from config import OUTPUT_FOLDER
import numpy as np
import json 
import matplotlib.pyplot as plt



class Prediction(BaseComponent):

    outgoing_edges = 1

    def __init__(self):
        self.model = None
        self.folder_file = None

    def run(self, output_2):
        ticker = output_2['ticker']
        mode = output_2['mode']
        target = output_2['target']
        self.model = output_2['model']
        X_test = output_2['X_test']
        y_test = output_2['y_test']
        test_data = output_2['test_data']
        feature_columns = output_2['feature_columns']
        params = output_2['params']
        model_name = output_2['model_name']
        
        self.folder_file = f'{OUTPUT_FOLDER}/{ticker}/{mode}/{target}'
        y_pred = self.predict(X_test)
        metrics = self.calculate_metric(y_test, y_pred)
        self.export_result(ticker, mode, target, metrics, params, model_name, y_pred)
        self.plot(test_data, y_test, y_pred, mode, target)
        self.feature_importance(feature_columns)

        
        return  {"output_3": {
                            "final": "final" 
                            }
                }, "output_3"
    
    
    def predict(self, X_test):
        y_pred = self.model.predict(X_test)
        return y_pred
    
    def calculate_metric(self, y_test, y_pred):
        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mape = mean_absolute_percentage_error(y_test, y_pred)
        risk = (np.sum(y_pred > y_test) / len(y_test) ) * 100

        return (r2, rmse, mape, risk)
    
    def export_result(self, ticker, mode, target, metric, params, model, y_pred):
        file_path = f'{OUTPUT_FOLDER}/result.json'

        if os.path.exists(file_path):
            with open(file_path, 'r') as json_file:
                all_results = json.load(json_file)
        else:
            all_results = {}

        if ticker not in all_results:
            all_results[ticker] = {}
        
        if mode not in all_results[ticker]:
            all_results[ticker][mode] = {}

        if target not in all_results[ticker][mode]:
            all_results[ticker][mode][target] = {}

        all_results[ticker][mode][target][model] = params
        all_results[ticker][mode][target]['r2'] = metric[0]
        all_results[ticker][mode][target]['rmse'] = metric[1]
        all_results[ticker][mode][target]['mape'] = metric[2]
        all_results[ticker][mode][target]['risk'] = metric[3]
        # all_results[ticker][mode][target]['y_pred'] =  y_pred.tolist()

        with open(file_path, 'w') as json_file:
            json.dump(all_results, json_file, indent=4)


    def plot(self, test_data, y_test, y_pred, mode, target):
        plt.figure(figsize=(10,6))
        if mode == '1d':
            plt.plot(test_data['date'], y_test, label='Actual Prices', color='blue')
            plt.plot(test_data['date'], y_pred, label='Predicted Prices', color='red')
        else:
            plt.plot(test_data['date_start'], y_test, label='Actual Prices', color='blue')
            plt.plot(test_data['date_start'], y_pred, label='Predicted Prices', color='red')
        plt.legend()
        plt.title(f'{target} Prediction')
        plt.xlabel('Date')
        plt.ylabel('Stock Price')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'{self.folder_file}/plot.png')

    
    def feature_importance(self, feature_columns):
        feature_importances = self.model.feature_importances_
        sorted_idx = np.argsort(feature_importances)[::-1]

        plt.figure(figsize=(20,30))
        plt.barh(np.array(feature_columns)[sorted_idx], feature_importances[sorted_idx], color='green')
        plt.xlabel('Feature Importance')
        plt.title('Feature Importances')
        plt.savefig(f'{self.folder_file}/feature_important.png')


        
    def run_batch(self, inputs: list) -> list:    
        results = [self.run(ticker, mode) for ticker, mode in inputs]
        return results