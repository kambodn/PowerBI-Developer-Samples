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

INSTAGRAM_ACCOUNT_ID = "17841402337256516"


# Establish the database connection
conn_str = (f'Driver={{ODBC Driver 17 for SQL Server}};Server={server};Database={database};UID={username};PWD={password}')
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Create SQLAlchemy engine to connect to SQL Server
engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_str))


# Base URL for posts
BASE_POSTS_URL = f"https://graph.facebook.com/v20.0/{INSTAGRAM_ACCOUNT_ID}/media?fields=caption,like_count,comments_count,permalink,timestamp&access_token={META_TOKEN}"


# Function to fetch paginated data
def fetch_page(url):
    data = []
    while url:
        response = requests.get(url).json()
        data.extend(response.get("data", []))
        url = response.get("paging", {}).get("next")
    return data

# Function to get post Ids
def get_ids(query):
    # Execute the query
    post_id_tuples = cursor.execute(query)

    # Convert the result into a list of strings
    post_id = [row[0] for row in post_id_tuples]
    return post_id

# Function to get post insights
def get_post_insights(post_id, meta_token):
    insights_url = f"https://graph.facebook.com/v20.0/{post_id}/insights?metric=impressions,reach,saved&access_token={meta_token}"
    response = requests.get(insights_url)
    response.raise_for_status()
    insights_data = response.json().get("data", [])

    insights_list = []
    for insight in insights_data:
        insight_title = insight.get("title")
        for value in insight.get("values", []):
            insights_list.append({
                "InsightTitle": insight_title,
                "InsightValue": value.get("value")
            })

    insights_df = pd.DataFrame(insights_list)
    return insights_df

# Function to get comments
def get_comments(post_id,META_TOKEN):
    comments_url = f"https://graph.facebook.com/v20.0/{post_id}/comments?fields=text,like_count,timestamp&access_token={META_TOKEN}"
    try:
        response = requests.get(comments_url)
        response.raise_for_status()
        data = response.json().get("data", [])
    except requests.exceptions.RequestException:
        data = []
    return data

# Retrieve all posts
print("Retrieving posts...")
posts_data = fetch_page(BASE_POSTS_URL)
print("Posts retrieved...")

# Transform posts data into a DataFrame
posts_df = pd.json_normalize(posts_data)


# Select relevant columns
posts_df = posts_df.loc[:, ['id', 'caption', 'like_count', 'comments_count', 'permalink', 'timestamp']]
posts_df.rename(columns={"id": "post_id","caption": "post_message","like_count": "post_like_count","permalink": "post_url","timestamp": "post_created_time"}, inplace=True)
posts_df['exists'] = 0


# Get post Ids from the database
post_id = get_ids(f"SELECT [post_id] FROM [dbo].[tbl_instagram_posts] WHERE [Page_ID] = {INSTAGRAM_ACCOUNT_ID}")

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
for index, row in categorized_df[categorized_df['exists'] == 0].iterrows():
     try:
          insights_df = get_post_insights(row.post_id, META_TOKEN)
     except requests.exceptions.HTTPError:
          continue
     transformed_df = insights_df.pivot(columns="InsightTitle", values="InsightValue")
     categorized_df.loc[categorized_df['post_id'] == row.post_id, 'post_impressions'] = transformed_df[["Impressions", "Accounts reached", "Saved"]].max().to_frame().T['Impressions'][0]
     categorized_df.loc[categorized_df['post_id'] == row.post_id, 'post_reach'] = transformed_df[["Impressions", "Accounts reached", "Saved"]].max().to_frame().T['Accounts reached'][0]
     categorized_df.loc[categorized_df['post_id'] == row.post_id, 'post_saved'] = transformed_df[["Impressions", "Accounts reached", "Saved"]].max().to_frame().T['Saved'][0]
print("Post insights retrieved...")

categorized_df['Page_ID'] = INSTAGRAM_ACCOUNT_ID
df2 = categorized_df[['post_message', 'post_created_time', 'post_like_count', 'post_url', 'post_id', 'post_impressions', 'post_reach', 'post_saved', 'Primary_Category', 'Secondary_Categories', 'Confidence_Score', 'Keywords', 'Categorization_Reasoning','Page_ID']]
df2.rename(columns={"post_impressions_unique": "post_reach"}, inplace=True) 


# Save to database
print("Saving posts to database...")
df2['post_message'] = df2['post_message'].astype(str) 
df2['Categorization_Reasoning'] = df2['Categorization_Reasoning'].astype(str) 
df2['post_message'] = df2['post_message'].apply(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else "")
df2['Categorization_Reasoning'] = df2['Categorization_Reasoning'].apply(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else "")
df2.to_sql('tbl_instagram_posts', engine, if_exists='append', index=False)
del df2
print("Saved posts to database...")

# Get comments
df3 = categorized_df[['post_id','Page_ID']][categorized_df['comments_count']>0]

df_comments = pd.DataFrame(columns=['post_id','text','like_count','timestamp','id','Page_ID'])
for index, row in df3.iterrows():
    comnts = pd.DataFrame(get_comments(row.post_id,META_TOKEN))
    comnts['post_id'] = row.post_id
    comnts['Page_ID'] = row.Page_ID
    df_comments = pd.concat([df_comments, comnts],ignore_index=True)

df_comments.rename(columns={"text": "comments_message","timestamp":"comments_created_time","id":"comments_id"},inplace=True)

# Get comment Ids
Comment_id = get_ids(f"SELECT [comments_id] FROM [dbo].[tbl_instagram_comments] WHERE [Page_ID] = {INSTAGRAM_ACCOUNT_ID}")


# Check if the comment exists
df_comments['exists'] = 0
for index, row in df_comments.iterrows():
     if row.comments_id in Comment_id:
          df_comments['exists'] = 1
     else:
          continue


# Perform batch sentiment analysis
print("Performing sentiment analysis...")
analyzer = cs.SentimentAnalyzer(api_key=API_KEY)
processed_df = analyzer.batch_sentiment_analysis(df_comments[df_comments['exists'] == 0], comment_column='comments_message')
print("Sentiment analysis completed...")


# Categorize comments
print("Starting comment categorization...")
categorizer = cmt.ContentCategorizer(api_key=API_KEY)
processed_df = categorizer.batch_categorization(processed_df[processed_df['exists'] == 0],comment_column = "comments_message")
print("comment categorization completed...")


# Save to database
print("Saving comments to database...")
processed_df = processed_df[['comments_created_time', 'comments_message', 'comments_id', 'post_id', 'Sentiment', 'Confidence_Score', 'Key_Emotions', 'Reasoning', 'Primary_Category', 'Secondary_Categories', 'Cat_confidence_Score', 'Keywords', 'Categorization_Reasoning','Page_ID']]
processed_df['comments_message'] =processed_df['comments_message'].astype(str)
processed_df['Categorization_Reasoning'] =processed_df['Categorization_Reasoning'].astype(str)
processed_df['comments_message'] = processed_df['comments_message'].apply(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else "")
processed_df['Categorization_Reasoning'] = processed_df['Categorization_Reasoning'].apply(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else "")

processed_df.to_sql('tbl_instagram_comments', engine, if_exists='append', index=False)
print("Saved comments to database...")

# Close the database connection
conn.commit()
conn.close()

print("Database connection closed...")