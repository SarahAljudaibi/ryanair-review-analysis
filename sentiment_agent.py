import requests
import json
from sqlalchemy import create_engine, text
import pandas as pd

# Database configuration
from sqlite_config import get_sqlite_engine

class SentimentAgent:
    def __init__(self, ollama_url="http://localhost:11434"):
        self.ollama_url = ollama_url
        self.model = "llama3.2"  # Change to your preferred Ollama model
        
    def get_sentiment_prompt(self, review_text):
        """Create few-shot prompt for sentiment analysis"""
        return f"""You are a sentiment analysis expert. You will receive customer reviews and classify sentiment as "Positive", "Neutral", or "Negative". Respond ONLY in JSON format.

Examples:
Review: "The check-in process was smooth and the flight was on time. Great service overall!"
{{"review": "The check-in process was smooth and the flight was on time. Great service overall!", "sentiment": "Positive", "reason": "smooth check-in, on time, great service"}}

Review: "It was okay, nothing special. Seats were a bit cramped."
{{"review": "It was okay, nothing special. Seats were a bit cramped.", "sentiment": "Neutral", "reason": "okay experience, minor complaint about seats"}}

Review: "Very disappointed with the delay and rude staff."
{{"review": "Very disappointed with the delay and rude staff.", "sentiment": "Negative", "reason": "disappointed, delay, rude staff"}}

Now analyze this review:
Review: "{review_text}"
"""

    def analyze_sentiment(self, review_text):
        """Send review to Ollama for sentiment analysis"""
        try:
            prompt = self.get_sentiment_prompt(review_text)
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                # Parse JSON from response
                try:
                    sentiment_data = json.loads(result['response'])
                    return sentiment_data
                except:
                    # Fallback if JSON parsing fails
                    return {
                        "review": review_text,
                        "sentiment": "Neutral",
                        "reason": "Analysis failed"
                    }
            else:
                return None
                
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return None

    def add_sentiment_column(self):
        """Add sentiment columns to database table"""
        try:
            engine = get_sqlite_engine()
            with engine.connect() as conn:
                # SQLite ALTER TABLE syntax
                try:
                    conn.execute(text("ALTER TABLE ryanair_reviews ADD COLUMN sentiment TEXT"))
                except:
                    pass  # Column already exists
                try:
                    conn.execute(text("ALTER TABLE ryanair_reviews ADD COLUMN sentiment_reason TEXT"))
                except:
                    pass  # Column already exists
                conn.commit()
            print("Sentiment columns ready")
        except Exception as e:
            print(f"Error adding columns: {e}")

    def process_reviews(self):
        """Process reviews and update with sentiment analysis"""
        try:
            engine = get_sqlite_engine()
            
            # Count unprocessed reviews first
            count_query = """
                SELECT COUNT(*) as unprocessed_count
                FROM ryanair_reviews 
                WHERE (sentiment IS NULL OR sentiment = '') AND Comment IS NOT NULL AND Comment != ''
            """
            count_df = pd.read_sql(count_query, engine)
            total_unprocessed = count_df.iloc[0]['unprocessed_count']
            
            if total_unprocessed == 0:
                print("All reviews already have sentiment analysis!")
                return
            
            print(f"Found {total_unprocessed} reviews without sentiment analysis")
            
            query = """
                SELECT id, Comment as comment 
                FROM ryanair_reviews 
                WHERE (sentiment IS NULL OR sentiment = '') 
                AND Comment IS NOT NULL AND Comment != ''
                ORDER BY id
            """
            
            df = pd.read_sql(query, engine)
            print(f"Processing {len(df)} reviews for sentiment analysis...")
            
            for idx, row in df.iterrows():
                review_id = row['id']
                comment = row['comment']
                
                print(f"Analyzing review {idx+1}/{len(df)}...")
                
                # Get sentiment analysis
                sentiment_result = self.analyze_sentiment(comment)
                
                if sentiment_result:
                    # Ensure reason is a string (handle list responses)
                    reason = sentiment_result['reason']
                    if isinstance(reason, list):
                        reason = ', '.join(reason)
                    elif not isinstance(reason, str):
                        reason = str(reason)
                    
                    # Update database
                    with engine.connect() as conn:
                        conn.execute(text("""
                            UPDATE ryanair_reviews 
                            SET sentiment = :sentiment, SentimentReason = :reason 
                            WHERE id = :id
                        """), {
                            'sentiment': sentiment_result['sentiment'],
                            'reason': reason,
                            'id': review_id
                        })
                        conn.commit()
                    
                    print(f"Updated review {review_id}: {sentiment_result['sentiment']}")
                
        except Exception as e:
            print(f"Error processing reviews: {e}")
    
    def add_new_review(self, comment, rating=None, country=None, aircraft=None, traveller_type=None, origin=None, destination=None):
        """Add new review to database and return its ID"""
        try:
            engine = get_sqlite_engine()
            with engine.connect() as conn:
                result = conn.execute(text("""
                    INSERT INTO ryanair_reviews (
                        Comment, "OverallRating", "PassengerCountry", Aircraft, 
                        "TypeOfTraveller", Origin, Destination, "DatePublished"
                    )
                    VALUES (:comment, :rating, :country, :aircraft, :travellertype, :origin, :destination, date('now'))
                """), {
                    'Comment': Comment,
                    'Rating': Rating,
                    'Country': Country,
                    'Aircraft': Aircraft,
                    'TravellerType': TravellerType,
                    'Origin': Origin,
                    'Destination': Destination
                })
                conn.commit()
                return result.lastrowid
        except Exception as e:
            print(f"Error adding review: {e}")
            return None
    
    def add_reviews_from_excel(self, excel_path):
        """Add reviews from Excel file to database"""
        try:
            # Read Excel file
            df = pd.read_excel(excel_path)
            print(f"Loaded Excel with {len(df)} rows")
            
            # Clean column names
            df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
            
            review_ids = []
            engine = get_sqlite_engine()
            
            for _, row in df.iterrows():
                # Extract data from row
                comment = row.get('Comment', row.get('review', ''))
                rating = row.get('OverallRating', row.get('rating', None))
                country = row.get('PassengerCountry', row.get('country', None))
                aircraft = row.get('Aircraft', None)
                traveller_type = row.get('TypeOfTraveller', row.get('TravellerType', None))
                origin = row.get('Origin', None)
                destination = row.get('Destination', None)
                
                if comment:  # Only add if comment exists
                    with engine.connect() as conn:
                        result = conn.execute(text("""
                            INSERT INTO ryanair_reviews (
                                Comment, "OverallRating", "PassengerCountry", Aircraft,
                                "TypeOfTraveller", Origin, Destination, "DatePublished"
                            )
                            VALUES (:Comment, :Rating, :Country, :Aircraft, :TravellerType, :Origin, :Destination, date('now'))
                        """), {
                            'comment': Comment,
                            'Rating': Rating,
                            'Country': Country,
                            'Aircraft': Aircraft,
                            'TravellerType': TravellerType,
                            'Origin': Origin,
                            'Destination': Destination
                        })
                        conn.commit()
                        review_ids.append(result.lastrowid)
            
            return review_ids
            
        except Exception as e:
            print(f"Error adding reviews from Excel: {e}")
            return []
    
    def process_single_review(self, review_id):
        """Process sentiment analysis for a single review"""
        try:
            engine = get_sqlite_engine()
            
            # Get the specific review
            query = "SELECT id, Comment as comment FROM ryanair_reviews WHERE id = ? AND Comment IS NOT NULL"
            df = pd.read_sql(query, engine, params=[review_id])
            
            if df.empty:
                print("Review not found or has no comment")
                return
            
            review_id = df.iloc[0]['id']
            comment = df.iloc[0]['comment']
            
            print(f"Analyzing review: {comment[:100]}...")
            
            # Get sentiment analysis
            sentiment_result = self.analyze_sentiment(comment)
            
            if sentiment_result:
                # Ensure reason is a string (handle list responses)
                reason = sentiment_result['reason']
                if isinstance(reason, list):
                    reason = ', '.join(reason)
                elif not isinstance(reason, str):
                    reason = str(reason)
                
                # Update database
                with engine.connect() as conn:
                    conn.execute(text("""
                        UPDATE ryanair_reviews 
                        SET sentiment = :sentiment, SentimentReason = :reason 
                        WHERE id = :id
                    """), {
                        'sentiment': sentiment_result['sentiment'],
                        'reason': reason,
                        'id': review_id
                    })
                    conn.commit()
                
                print(f"Sentiment: {sentiment_result['sentiment']} - {sentiment_result['reason']}")
            
        except Exception as e:
            print(f"Error processing single review: {e}")

# Usage example
if __name__ == "__main__":
    agent = SentimentAgent()
    
    # Add sentiment columns to database
    agent.add_sentiment_column()
    
    # Process ALL unanalyzed reviews (or set limit=5 for testing)
    agent.process_reviews()  # Process all unanalyzed reviews
