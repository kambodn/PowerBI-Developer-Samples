import os
import time
import random
import pandas as pd
import json
from openai import OpenAI
from typing import Dict, List



class SentimentAnalyzer:
    def __init__(self, api_key: str = None):
        """
        Initialize the sentiment analyzer with OpenAI client.
        
        Args:
            api_key (str, optional): OpenAI API key. If None, reads from environment.
        """
        # Use environment variable if no API key provided
        self.client = OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))
    
    def advanced_sentiment_analysis(self, comment: str) -> Dict:
        """
        Perform advanced sentiment analysis using GPT-4 Omni.
        
        Args:
            comment (str): The text comment to analyze
        
        Returns:
            Dict: Structured sentiment analysis result
        """
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o-mini-2024-07-18",
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an advanced sentiment analysis AI reviewing sentiments made on Davis & Shirtliff products. "
                        "Provide a structured JSON response analyzing the sentiment of a given text."
                    },
                    {
                        "role": "user", 
                        "content": f"""Analyze the following comment and provide a detailed sentiment assessment on comments that relate to Davis & Shirtliff products or services:

Comment: {comment}

Provide a JSON response with these keys:
- sentiment: Overall sentiment (Positive/Negative/Neutral)
- confidence_score: Confidence level (0-1)
- key_emotions: Array of detected emotions
- reasoning: Brief explanation of sentiment classification

Ensure the response is a valid JSON object."""
                    }
                ],
                temperature=0.3,
                max_tokens=300
            )
            
            # Parse the response
            result = json.loads(response.choices[0].message.content)
            
            # Validate the result structure
            result['sentiment'] = result.get('sentiment', 'Neutral')
            result['confidence_score'] = float(result.get('confidence_score', 0.5))
            result['key_emotions'] = result.get('key_emotions', [])
            result['reasoning'] = result.get('reasoning', 'No specific reasoning provided')
            
            return result
        
        except Exception as e:
            # Fallback error handling
            return {
                "sentiment": "Neutral",
                "confidence_score": 0.5,
                "key_emotions": [],
                "reasoning": f"Error in analysis: {str(e)}"
            }
    
    def batch_sentiment_analysis(
        self, 
        df: pd.DataFrame, 
        comment_column: str, 
        batch_size: int = 10, 
        delay_range: tuple = (1, 3)
    ) -> pd.DataFrame:
        """
        Perform batch sentiment analysis with rate limiting and error handling.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            comment_column (str): Name of the column containing comments
            batch_size (int): Number of comments to process in each batch
            delay_range (tuple): Range of random delay between batches (min, max)
        
        Returns:
            pd.DataFrame: DataFrame with added sentiment analysis columns
        """
        # Create a copy of the DataFrame to avoid modifying the original
        result_df = df.copy()
        
        # Prepare columns for sentiment analysis results
        result_df['Sentiment'] = 'Neutral'
        result_df['Confidence_Score'] = 0.5
        result_df['Key_Emotions'] = None
        result_df['Reasoning'] = ''
        
        # Process comments in batches
        for i in range(0, len(result_df), batch_size):
            try:
                # Select batch of comments
                batch = result_df.iloc[i:i+batch_size]
                
                # Perform sentiment analysis for each comment in the batch
                for idx, row in batch.iterrows():
                    try:
                        comment = row[comment_column]
                        
                        # Skip processing for non-string or empty comments
                        if not isinstance(comment, str) or not comment.strip():
                            continue
                        
                        # Perform sentiment analysis
                        result = self.advanced_sentiment_analysis(comment)
                        
                        # Update DataFrame with results
                        result_df.at[idx, 'Sentiment'] = result['sentiment']
                        result_df.at[idx, 'Confidence_Score'] = result['confidence_score']
                        result_df.at[idx, 'Key_Emotions'] = ', '.join(result['key_emotions'])
                        result_df.at[idx, 'Reasoning'] = result['reasoning']
                    
                    except Exception as comment_error:
                        # Log individual comment processing errors
                        result_df.at[idx, 'Reasoning'] = f"Individual comment analysis error: {str(comment_error)}"
                
                # Random delay between batches to avoid rate limiting
                delay = random.uniform(delay_range[0], delay_range[1])
                time.sleep(delay)
            
            except Exception as batch_error:
                print(f"Batch processing error at index {i}: {str(batch_error)}")
        
        return result_df
    

if __name__ == "__main__":
    print("This is a module and cannot be run directly. Please import it and use the methods directly.")
