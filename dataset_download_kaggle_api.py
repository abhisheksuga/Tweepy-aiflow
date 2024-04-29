from kaggle.api.kaggle_api_extended import KaggleApi

import * from config

# Function to download desired dataset from kaggle using kaggle api 
def download_kaggle_dataset(dataset_name ,download_path,unzip=False) :
    try:  
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset_name, data_files_path, unzip=unzip)
        print (f"dataset download success! find the files in {download_path} folder")
    except Exception as e:
        print (f"error while downloading: {e}")



download_kaggle_dataset('mmmarchetti/tweets-dataset', data_files_path, unzip=True) # calling the function , setting  unzip as True , as we want the files from kaggle to be unzipped . By defaut it is FALSE and it downloads the zip files. 
