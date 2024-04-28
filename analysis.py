import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from db import Database

class DataAnalysis:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.db_instance = Database('./config.json')

    def read_csv(self):
        df = pd.read_csv(self.csv_path)
        return df

    def query_psql_table(self, query):
        try:
            self.db_instance.connect_postgres()
            cursor = self.db_instance.postgres_connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except Exception as e:
            print("Error executing query:", e)
            return None

    @staticmethod
    def extract_hashtags(text):     #making this function as a static , to independent of class instances
        return [word[1:] for word in text.split() if word.startswith('#')]


    def perform_data_analysis(self):
        df = self.read_csv()
        # Scatter plot likes vs shares to see correlation
        correlation = df['number_of_likes'].corr(df['number_of_shares'])
        print("Correlation coefficient between number_of_likes and number_of_shares is :", correlation)
        plt.figure(figsize=(8, 6))
        plt.scatter(df['number_of_likes'], df['number_of_shares'], color='blue', label='Observations')
        plt.xlabel('Number of Likes')
        plt.ylabel('Number of Shares')
        plt.title('Scatter Plot of Number of Likes vs Number of Shares')
        plt.legend()
        plt.grid(True)
        plt.savefig('./figures/Scatter_plot_likes_vs_shares.png')
        plt.clf()

        # Engagement distribution among authors
        engagement_by_author = df.groupby('author')['engagement'].sum().reset_index()
        plt.figure(figsize=(10, 6))
        plt.bar(engagement_by_author['author'], engagement_by_author['engagement'], color='blue')
        plt.xlabel('Author')
        plt.ylabel('Total Engagement')
        plt.title('Engagement Analysis by Author')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig('./figures/Engagement_distribution_of_authors.png')
        plt.clf()

        # Comparison of engagement for different sentiment categories
        engagement_by_sentiment = df.groupby('sentiment_category')['engagement'].mean().reset_index()
        plt.figure(figsize=(8, 6))
        plt.bar(engagement_by_sentiment['sentiment_category'], engagement_by_sentiment['engagement'], color='blue')
        plt.xlabel('Sentiment Category')
        plt.ylabel('Average Engagement')
        plt.title('Comparison of Engagement for Different Sentiment Categories')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('./figures/Comparison_of_engagement_for_different_sentiment_categories.png')
        plt.clf()

        # Engagement based on the hour of the day
        engagement_by_hour = df.groupby('activity_hour')['engagement'].mean().reset_index()
        plt.figure(figsize=(10, 6))
        plt.plot(engagement_by_hour['activity_hour'], engagement_by_hour['engagement'], marker='o', color='blue', linestyle='-')
        plt.xlabel('Hour of the Day')
        plt.ylabel('Average Engagement')
        plt.title('Engagement Based on Activity Hour')
        plt.xticks(range(24))
        plt.grid(True)
        plt.tight_layout()
        plt.savefig('./figures/Engagement_based_on_activity_hour.png')
        plt.clf()

        # Engagement based on the activity day of the week
        engagement_by_day = df.groupby('activity_day')['engagement'].mean().reset_index()
        plt.figure(figsize=(10, 6))
        plt.bar(engagement_by_day['activity_day'], engagement_by_day['engagement'], color='blue')
        plt.xlabel('Activity Day of the Week')
        plt.ylabel('Average Engagement')
        plt.title('Engagement Based on Activity Day of the Week')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('./figures/Engagement_based_on_activity_day_of_the_week.png')
        plt.clf()

        # Top Hashtags by Engagement
        df['hashtags'] = df['content'].apply(self.extract_hashtags)
        hashtags_df = df.explode('hashtags')
        hashtags_engagement = hashtags_df.groupby('hashtags')['engagement'].sum().reset_index()
        top_hashtags = hashtags_engagement.sort_values(by='engagement', ascending=False).head(5)
        print(f'the top hashtags are : {top_hashtags}')
        plt.figure(figsize=(10, 6))
        plt.bar(top_hashtags['hashtags'], top_hashtags['engagement'], color='blue')
        plt.xlabel('Hashtags')
        plt.ylabel('Total Engagement')
        plt.title('Top Hashtags by Engagement')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('./figures/Top_Hashtags_by_Engagement.png')
        plt.clf()

        # Creating the word cloud from all the tweets
        all_tweets = ' '.join(df['content'])
        wordcloud = WordCloud(width=800, height=400, background_color='white', colormap='viridis', max_words=100).generate(all_tweets)
        plt.figure(figsize=(10, 6))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Word Cloud of Most Frequent Words in Tweets')
        plt.savefig('./figures/Word_Cloud_of_Most_Frequent_Words_in_Tweets.png')
        plt.clf()

        # Executing the SQL query and fetch the data to do engagement analysis by activity hour
        sql_query = """
        SELECT activity_hour, AVG(engagement) AS avg_engagement
        FROM tweets_table
        GROUP BY activity_hour
        ORDER BY activity_hour;
        """
        result = self.query_psql_table(sql_query)
        if result:
            df = pd.DataFrame(result, columns=['activity_hour', 'avg_engagement'])
            plt.figure(figsize=(10, 6))
            plt.plot(df['activity_hour'], df['avg_engagement'], marker='o', linestyle='-')
            plt.xlabel('Hour of the Day')
            plt.ylabel('Average Engagement')
            plt.title('Engagement Analysis Based on Hour of the Day (psql fetch)')
            plt.grid(True)
            plt.savefig('./figures/Engagement_based_on_activity_hour_psql_fetch.png')
            plt.clf()
        else:
            print("Query execution failed.")

analysis = DataAnalysis('./data/processed_data.csv')
analysis.perform_data_analysis()