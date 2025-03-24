from haystack.nodes import BaseComponent
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import joblib
import os
from sklearn.ensemble import RandomForestRegressor
import xgboost as xgb
from config import PARAM_GRIDS, OUTPUT_FOLDER
import json 
import lightgbm as lgb


class ModelSelection(BaseComponent):

    outgoing_edges = 2

    def __init__(self, target):
        self.target = target
        self.output_folder = OUTPUT_FOLDER
        os.makedirs(self.output_folder, exist_ok=True)

        self.X_train = None
        self.X_test = None
        self.y_train = None 
        self.y_test = None 
        self.test_data = None

    
    def run_batch(self, inputs: list) -> list:    
        results = [self.run(ticker, mode) for ticker, mode in inputs]
        return results
    
    def __prepare_data(self, data, mode):
        if mode == '1d':
            col_to_drop = ['date', 'middle_open_close', 'middle_high_low', 'open', 'high', 'low', 'close', 'volumn']
        else:
            col_to_drop = ['date_start', 'date_end', 'middle_open_close', 'middle_high_low', 'open', 'high', 'low', 'close', 'volumn']

        feature_columns = [col for col in data.columns if col not in col_to_drop]

        if mode == '1d':
            test_data = data[data['date'].dt.year >= 2024]
            train_data = data[data['date'].dt.year < 2024]
        else:
            test_data = data[data['date_start'].dt.year >= 2024]
            train_data = data[data['date_start'].dt.year < 2024]
        
        self.test_data = test_data

        self.X_train = train_data[feature_columns]
        self.y_train = train_data[self.target]

        self.X_test = test_data[feature_columns]
        self.y_test = test_data[self.target]

    
    def compare_models(self, ticker, mode, target):
        models = {
            "LightGBM": lgb.LGBMRegressor(random_state=42, verbosity=-1),
            "XGBoost": xgb.XGBRegressor(random_state=42),
            "RandomForest": RandomForestRegressor(random_state=42),
        }

        best_models = {}
        final_best_model = None
        final_best_score = -float('inf')
        final_best_params = None
        final_best_name = None

        # Loop through models and perform GridSearchCV
        for model_name, model in models.items():
            print(f"Training {model_name} ...")
            grid_search = GridSearchCV(estimator=model, param_grid=PARAM_GRIDS[model_name], cv=5, scoring='r2')
            grid_search.fit(self.X_train, self.y_train)

            best_model = grid_search.best_estimator_
            best_params = grid_search.best_params_

            y_pred = best_model.predict(self.X_test)
            r2 = r2_score(self.y_test, y_pred)

            if r2 > final_best_score:
                final_best_score = r2
                final_best_model = best_model
                final_best_params = best_params
                final_best_name = model_name

            best_models[model_name] = {
                "params": best_params,
                "score": r2
            }

        models_file = f'{self.output_folder}/grid_search_result.json'
        if os.path.exists(models_file):
            with open(models_file, 'r') as json_file:
                all_models = json.load(json_file)
        else:
            all_models = {}

        if ticker not in all_models:
            all_models[ticker] = {}
        
        if mode not in all_models[ticker]:
            all_models[ticker][mode] = {}

        all_models[ticker][mode][target] = best_models
        # print(all_models)

        with open(models_file, 'w') as json_file:
            json.dump(all_models, json_file, indent=4)

        return final_best_model, final_best_params, final_best_name
    
    def __save_model(self, model, filename):
        joblib.dump(model, filename)


    def run(self, output_1):
        data = output_1['data']
        ticker = output_1['ticker']
        mode = output_1['mode']
        feature_columns = [col for col in data.columns if col not in ['date', 'date_start', 'date_end', 'middle_open_close', 'middle_high_low', 'open', 'high', 'low', 'close', 'volumn']]


        folder_path = f'{self.output_folder}/{ticker}/{mode}/{self.target}'
        os.makedirs(folder_path, exist_ok=True)
        
        self.__prepare_data(data, mode)

        best_model, params, model_name = self.compare_models(ticker=ticker, mode=mode, target=self.target)
        print(best_model)
        
        file_path = f"{folder_path}/model.pkl"
        self.__save_model(best_model, file_path)

        return  {"output_2": {
                            "model": best_model, 
                            "X_test": self.X_test,
                            "y_test": self.y_test,
                            "ticker": ticker,
                            "mode": mode,
                            "target": self.target,
                            'test_data': self.test_data,
                            'feature_columns': feature_columns,
                            'params': params,
                            "model_name": model_name
                            }
                }, "output_2"
