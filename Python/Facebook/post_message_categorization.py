import os
import time
import random
import pandas as pd
import json
from openai import OpenAI
from typing import Dict, List

class ContentCategorizer:
    def __init__(self, api_key: str = None):
        """
        Initialize the content categorizer with OpenAI client.
        
        Args:
            api_key (str, optional): OpenAI API key. If None, reads from environment.
        """
        self.client = OpenAI(api_key=api_key or os.environ.get("OPENAI_API_KEY"))
        
        # Define categories and their descriptions
        self.categories = {
            "Sustainability": "Posts about eco-friendly products, green initiatives, or energy-efficient solutions",
            "Products": "Posts highlighting specific products, new arrivals, or product features",
            "Careers": "Posts about job openings, employee spotlights, or recruitment-related content",
            "Events": "Posts about trade shows, conferences, or corporate social responsibility activities",
            "Customer_Engagement": "Posts containing user testimonials, reviews, and customer stories",
            "Promotions": "Posts announcing discounts, special deals, and seasonal campaigns"
        }
    
    def categorize_content(self, post: str) -> Dict:
        """
        Categorize a post into predefined categories using GPT-4 Omni.
        
        Args:
            post (str): The social media post to categorize
        
        Returns:
            Dict: Structured categorization result
        """
        try:
            # Create a description of categories for the prompt
            categories_desc = "\n".join([f"- {k}: {v}" for k, v in self.categories.items()])
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini-2024-07-18",
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system", 
                        "content": "You are an AI specialized in categorizing Davis & Shirtliff's social media content. "
                        "Analyze posts and categorize them based on predefined categories."
                    },
                    {
                        "role": "user", 
                        "content": f"""Analyze the following post and categorize it according to these categories:

{categories_desc}

Post: {post}

Provide a JSON response with these keys:
- primary_category: Main category the post belongs to
- secondary_categories: Array of other relevant categories (if any)
- confidence_score: Confidence level (0-1)
- keywords: Array of key terms that influenced the categorization
- reasoning: Brief explanation of the categorization

Ensure the response is a valid JSON object."""
                    }
                ],
                temperature=0.3,
                max_tokens=300
            )
            
            # Parse the response
            result = json.loads(response.choices[0].message.content)
            
            # Validate the result structure
            result['primary_category'] = result.get('primary_category', 'Uncategorized')
            result['secondary_categories'] = result.get('secondary_categories', [])
            result['confidence_score'] = float(result.get('confidence_score', 0.5))
            result['keywords'] = result.get('keywords', [])
            result['reasoning'] = result.get('reasoning', 'No specific reasoning provided')
            
            return result
        
        except Exception as e:
            return {
                "primary_category": "Uncategorized",
                "secondary_categories": [],
                "confidence_score": 0.5,
                "keywords": [],
                "reasoning": f"Error in categorization: {str(e)}"
            }
    
    def batch_categorization(
        self, 
        df: pd.DataFrame, 
        post_column: str, 
        batch_size: int = 10, 
        delay_range: tuple = (1, 3)
    ) -> pd.DataFrame:
        """
        Perform batch categorization with rate limiting and error handling.
        
        Args:
            df (pd.DataFrame): Input DataFrame
            post_column (str): Name of the column containing posts
            batch_size (int): Number of posts to process in each batch
            delay_range (tuple): Range of random delay between batches (min, max)
        
        Returns:
            pd.DataFrame: DataFrame with added categorization columns
        """
        result_df = df.copy()
        
        # Prepare columns for categorization results
        result_df['Primary_Category'] = 'Uncategorized'
        result_df['Secondary_Categories'] = None
        result_df['Confidence_Score'] = 0.5
        result_df['Keywords'] = None
        result_df['Categorization_Reasoning'] = ''
        
        # Process posts in batches
        for i in range(0, len(result_df), batch_size):
            try:
                batch = result_df.iloc[i:i+batch_size]
                
                for idx, row in batch.iterrows():
                    try:
                        post = row[post_column]
                        
                        if not isinstance(post, str) or not post.strip():
                            continue
                        
                        # Perform categorization
                        result = self.categorize_content(post)
                        
                        # Update DataFrame with results
                        result_df.at[idx, 'Primary_Category'] = result['primary_category']
                        result_df.at[idx, 'Secondary_Categories'] = ', '.join(result['secondary_categories'])
                        result_df.at[idx, 'Confidence_Score'] = result['confidence_score']
                        result_df.at[idx, 'Keywords'] = ', '.join(result['keywords'])
                        result_df.at[idx, 'Categorization_Reasoning'] = result['reasoning']
                    
                    except Exception as post_error:
                        result_df.at[idx, 'Categorization_Reasoning'] = f"Individual post categorization error: {str(post_error)}"
                
                # Random delay between batches
                delay = random.uniform(delay_range[0], delay_range[1])
                time.sleep(delay)
            
            except Exception as batch_error:
                print(f"Batch processing error at index {i}: {str(batch_error)}")
        
        return result_df
    


if __name__ == "__main__":
    print("This is a module and cannot be run directly. Please import it and use the methods directly.")