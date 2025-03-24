ALLOWED_MODES = {
    "1d": {
        "list_windows_ma": [3, 5, 7, 10, 14],
        "list_lags": [1, 2, 3, 5, 7, 10, 14]
    },
    "3d": {
        "list_windows_ma": [3, 5, 7, 10, 14],
        "list_lags": [1, 2, 3, 5, 7, 10, 14]
    },
    "1w": {
        "list_windows_ma": [2, 3, 5],
        "list_lags": [1, 2, 3, 5]
    },
    "2w": {
        "list_windows_ma": [2, 3, 5],
        "list_lags": [1, 2, 3, 5]
    }
}

MAIN_FEATURE_COLS = ['open', 'high', 'low', 'close', 'volumn']

PARAM_GRIDS = {
    "RandomForest": {
        'n_estimators': [100, 200, 500], 
        'max_depth': [5, 10],

    },

    "XGBoost": {
        'max_depth': [3, 6, 10],
        'n_estimators': [100, 200, 500]
    },
    "LightGBM": {
        'max_depth': [3, 6, 10],
        'num_leaves': [20, 30, 50]
    }
}

OUTPUT_FOLDER = 'output'
INPUT_FOLDER = 'input'
