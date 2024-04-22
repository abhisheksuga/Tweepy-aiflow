import json
import pandas as pd
from db import Database
from textblob import TextBlob

class DataProcessor:
    def __init__(self, config_file):
        self.config_file = config_file
        with open(self.config_file, 'r') as f:
            self.config = json.load(f)
        self.db_instance = Database(self.config_file)
        self.db_instance.connect_mongodb()

    def fetch_data_from_mongo(self):
        return self.db_instance.fetch_data_mongodb()

    def process_data(self, data):
        # Data processing steps here
        df = pd.DataFrame(data=data)
        print(df)
        df = df.drop(columns=['id', 'latitude', 'longitude', 'country'])
        df['num_words'] = df['content'].apply(self.count_of_words)
        df['sentiment_polarity'] = df['content'].apply(self.extract_sentiment)
        df['sentiment_category'] = df['sentiment_polarity'].apply(self.sentiment_category)
        df['date_time'] = pd.to_datetime(df['date_time'], format='%d/%m/%Y %H:%M')
        df['activity_hour'] = df['date_time'].dt.hour
        df['activity_day'] = df['date_time'].dt.dayofweek
        df['activity_month'] = df['date_time'].dt.month
        df['activity_year'] = df['date_time'].dt.year
        df['engagement'] = df['number_of_likes'] + df['number_of_shares']
        df['activity_day'] = df['activity_day'].map(self.day_mapping())
        return df

    def write_to_csv(self, dataframe):
        output_path = self.config.get('data_files_path')
        csv_file = f"{output_path}/processed_data.csv"
        dataframe.to_csv(csv_file, index=False)
        print(f"Processed data written to: {csv_file}")

    def count_of_words(self, content):
        return len(content.split())

    def extract_sentiment(self, content):
        emotions = TextBlob(content)
        return emotions.sentiment.polarity

    def sentiment_category(self, polarity):
        if polarity > 0:
            return 'positive'
        elif polarity < 0:
            return 'negative'
        else:
            return 'neutral'

    def day_mapping(self):
        return {
            0: 'Monday',
            1: 'Tuesday',
            2: 'Wednesday',
            3: 'Thursday',
            4: 'Friday',
            5: 'Saturday',
            6: 'Sunday'
        }

config_file_path = 'config.json'
processor = DataProcessor(config_file_path)


dataframe = processor.fetch_data_from_mongo()
if dataframe is not None:
    processed_dataframe = processor.process_data(dataframe)
    processor.write_to_csv(processed_dataframe)
