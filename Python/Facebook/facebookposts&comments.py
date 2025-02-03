import requests
import pandas as pd
import time
from datetime import datetime
import pyodbc
from sqlalchemy import create_engine
import post_message_categorization as pmc
import comment_sentiment as cs
import comment_message_categorization as cmt
import ast
import os
from dotenv import load_dotenv

load_dotenv()

server = os.getenv('server')
username = os.getenv('db_username')
password = os.getenv('db_password')
database = os.getenv('database')

META_TOKEN = os.getenv('META_TOKEN')
API_KEY = os.getenv('API_KEY')

# Establish the database connection
conn_str = (f'Driver={{ODBC Driver 17 for SQL Server}};Server={server};Database={database};UID={username};PWD={password}')
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Create SQLAlchemy engine to connect to SQL Server
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_str))

# Execute the query
start_tuples = cursor.execute("SELECT MAX([comments_created_time]) FROM [dbo].[tbl_facebook_comments]")

# Convert the result into a list of strings
start = [row[0] for row in start_tuples]

# Define the date filter 
SINCE_DATE = "2024-12-13T13:13:31+0000" if start[0] is None else start[0]
SINCE_TIMESTAMP = int(datetime.strptime(SINCE_DATE, "%Y-%m-%dT%H:%M:%S%z").timestamp())

# Base URL for posts
# BASE_POSTS_URL = f"https://graph.facebook.com/v20.0/466901410034470/posts?fields=message,created_time,likes.summary(true),comments.summary(true),permalink_url,id&access_token={META_TOKEN}"
BASE_POSTS_URL = f"https://graph.facebook.com/v20.0/466901410034470/posts?fields=message,created_time,likes.summary(true),comments.summary(true).filter(stream).since({SINCE_TIMESTAMP}),permalink_url,id&access_token={META_TOKEN}"

# Function to fetch paginated data
def fetch_page(url):
    data = []
    print("Process starting...")
    while url:
        response = requests.get(url).json()
        data.extend(response.get("data", []))
        url = response.get("paging", {}).get("next")
    print("Process completed...")
    return data

# Function to get post Ids
def get_ids(query):
    # Execute the query
    post_id_tuples = cursor.execute(query)

    # Convert the result into a list of strings
    post_id = [row[0] for row in post_id_tuples]
    return post_id

# Function to get post insights
def get_post_insights(post_id, access_token):
    insights_url = f"https://graph.facebook.com/v20.0/{post_id}/insights"
    params = {
        "metric": "post_impressions,post_impressions_unique",
        "access_token": access_token
    }

    try:
        # Fetch insights data
        response = requests.get(insights_url, params=params)
        response.raise_for_status()
        insights_data = response.json().get("data", [])
        
        # Process data into a DataFrame
        insights_list = []
        for insight in insights_data:
            insight_name = insight.get("name")
            for value_item in insight.get("values", []):
                insights_list.append({
                    "InsightName": insight_name,
                    "InsightValue": value_item.get("value")
                })
        
        # Convert to DataFrame
        insights_df = pd.DataFrame(insights_list)
        return insights_df
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching insights: {e}")
        return pd.DataFrame()

# Retrieve all posts
print("Retrieving posts...")
posts_data = fetch_page(BASE_POSTS_URL)
print("Posts retrieved...")

# Transform posts data into a DataFrame
posts_df = pd.json_normalize(posts_data)

# Select relevant columns
posts_df = posts_df.loc[:, ['message', 'created_time', 'likes.summary.total_count', 'permalink_url', 'id', 'comments.data', 'comments.summary.total_count']]
# posts_df.rename(columns={"likes.summary.total_count": "likes_summary_total_count","comments.data": "comments_data","comments.summary.total_count": "comments_summary_total_count"}, inplace=True)
posts_df.rename(columns={"message": "post_message","created_time": "post_created_time","likes.summary.total_count": "post_like_count","permalink_url": "post_url","id": "post_id","comments.data": "comments_data","comments.summary.total_count": "comments_summary_total_count"}, inplace=True)
posts_df['exists'] = 0

# Get post Ids from the database
post_id = get_ids("SELECT [post_id] FROM [dbo].[tbl_facebook_posts]")

# Check if the post exists
for index, row in posts_df.iterrows():
     if row.post_id in post_id:
          posts_df['exists'] = 1
     else:
          continue
     
# Categorize posts
print("Starting post categorization...")
categorizer = pmc.ContentCategorizer(api_key=API_KEY)
categorized_df = categorizer.batch_categorization(posts_df[posts_df['exists'] == 0],post_column = "post_message")
print("Post categorization completed...")

# Get post insights
print("Getting post insights...")
categorized_df['post_impressions'] = ''
categorized_df['post_impressions_unique'] = ''
for index, row in categorized_df.iterrows():
     if row.id in post_id:
         continue
        #   posts_df['exists'] = 1
     else:
        insights_df = get_post_insights(row.id, META_TOKEN)
        transformed_df = insights_df.pivot(columns="InsightName", values="InsightValue")
        categorized_df.loc[categorized_df['id'] == row.id, 'post_impressions'] = transformed_df[["post_impressions", "post_impressions_unique"]].max().to_frame().T['post_impressions'][0]
        categorized_df.loc[categorized_df['id'] == row.id, 'post_impressions_unique'] = transformed_df[["post_impressions", "post_impressions_unique"]].max().to_frame().T['post_impressions_unique'][0]
print("Post insights retrieved...")

# Save to database
print("Saving posts to database...")
# categorized_df.rename(columns={"message": "post_message","created_time": "post_created_time","likes_summary_total_count": "post_like_count","permalink_url": "post_url","id": "post_id"}, inplace=True)
df2 = categorized_df[['post_message', 'post_created_time', 'post_like_count', 'post_url', 'post_id', 'post_impressions', 'post_impressions_unique', 'Primary_Category', 'Secondary_Categories', 'Confidence_Score', 'Keywords', 'Categorization_Reasoning']]
df2.to_sql('tbl_facebook_posts', engine, if_exists='append', index=False)
del df2
print("Saved posts to database...")

# Get comments
df_comments = categorized_df.loc[:, ['post_id','comments_data']]
# df_comments = posts_df.loc[:, ['post_id','comments_data']]
df_comments = df_comments[(df_comments['comments_data'] != '[]') & (df_comments['comments_data'] != '')]
# df_comments['comments_data'] = df_comments['comments_data'].apply(ast.literal_eval)

# Flatten the JSON objects
df_comments_flattened = pd.DataFrame(columns=['post_id', 'created_time', 'message', 'id'])
for index, row in df_comments.iterrows():
    df8 = pd.DataFrame(row.comments_data)
    df8['post_id'] = row.post_id
    df_comments_flattened = pd.concat([df_comments_flattened, df8],ignore_index=True)

# Clean
df_comments_flattened.rename(columns={"created_time": "comments_created_time","message": "comments_message","id": "comments_id"}, inplace=True)
df_comments_flattened = df_comments_flattened[['comments_created_time', 'comments_message', 'comments_id', 'post_id']]
df_comments_flattened = df_comments_flattened[df_comments_flattened['comments_message'] != '']

# Get comment Ids
Comment_id = get_ids("SELECT [comments_id] FROM [dbo].[tbl_facebook_comments]")

# Check if the comment exists
df_comments_flattened['exists'] = 0
for index, row in df_comments_flattened.iterrows():
     if row.comments_id in Comment_id:
          df_comments_flattened['exists'] = 1
     else:
          continue
     
# Perform batch sentiment analysis
print("Performing sentiment analysis...")
analyzer = cs.SentimentAnalyzer(api_key=API_KEY)
processed_df = analyzer.batch_sentiment_analysis(df_comments_flattened[df_comments_flattened['exists'] == 0], comment_column='comments_message')
print("Sentiment analysis completed...")

# Categorize comments
print("Starting comment categorization...")
categorizer = cmt.ContentCategorizer(api_key=API_KEY)
processed_df = categorizer.batch_categorization(processed_df[processed_df['exists'] == 0],comment_column = "comments_message")
print("comment categorization completed...")

# Save to database
print("Saving comments to database...")
processed_df = processed_df[['comments_created_time', 'comments_message', 'comments_id', 'post_id', 'Sentiment', 'Confidence_Score', 'Key_Emotions', 'Reasoning', 'Primary_Category', 'Secondary_Categories', 'Cat_confidence_Score', 'Keywords', 'Categorization_Reasoning']]
processed_df.to_sql('tbl_facebook_comments', engine, if_exists='append', index=False)
print("Saved comments to database...")

# Close the database connection
conn.commit()
conn.close()
print("Database connection closed...")