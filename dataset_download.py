from kaggle.api.kaggle_api_extended import KaggleApi


api = KaggleApi()
api.authenticate()
api.dataset_download_files('mmmarchetti/tweets-dataset', path='.', unzip=True)
