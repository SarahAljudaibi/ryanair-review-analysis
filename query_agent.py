import requests
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Database configuration
from sqlite_config import get_sqlite_engine

class QueryAgent:
    def __init__(self, ollama_url="http://localhost:11434"):
        self.ollama_url = ollama_url
        self.model = "llama3.2"  # Change to your preferred Ollama model
        self.engine = get_sqlite_engine()
        self.query_cache = {}
        
    def get_query_prompt(self, user_question):
        """Create few-shot prompt for SQL generation"""
        return f"""You are a database assistant that converts natural language questions into SQL queries for a customer review database.

Database Schema:
Table: ryanair_reviews
Columns with data types and valid values:
- id: INTEGER (auto-increment primary key)
- date_published: DATE (format: YYYY-MM-DD)
- overall_rating: INTEGER (1-10 scale)
- passenger_country: VARCHAR (e.g., 'United States', 'United Kingdom', 'Germany')
- trip_verified: VARCHAR ('Trip Verified', 'Not Verified')
- comment_title: TEXT (review titles)
- comment: TEXT (full review text)
- aircraft: VARCHAR (e.g., 'Boeing 737-800', 'Boeing 737 MAX')
- type_of_traveller: VARCHAR ('Solo Leisure', 'Couple Leisure', 'Family Leisure', 'Business')
- seat_type: VARCHAR ('Economy Class', 'Premium Economy')
- origin: VARCHAR (departure city/airport)
- destination: VARCHAR (arrival city/airport)
- date_flown: VARCHAR (month/year format like 'January 2024')
- seat_comfort: DECIMAL(3,1) (1.0-5.0 rating scale)
- cabin_staff_service: DECIMAL(3,1) (1.0-5.0 rating scale)
- food_beverages: DECIMAL(3,1) (1.0-5.0 rating scale)
- ground_service: DECIMAL(3,1) (1.0-5.0 rating scale)
- value_for_money: DECIMAL(3,1) (1.0-5.0 rating scale)
- recommended: VARCHAR ('yes', 'no') - IMPORTANT: Use 'yes'/'no' NOT 1/0
- inflight_entertainment: DECIMAL(3,1) (1.0-5.0 rating scale)
- wifi_connectivity: DECIMAL(3,1) (1.0-5.0 rating scale)
- sentiment: VARCHAR ('Positive', 'Neutral', 'Negative')
- sentiment_reason: TEXT (e.g., 'early departure, cheap fare, welcoming staff', 'denied boarding, defrosting issue')

Examples:

Question: "How many customers from the United States used Ryanair and were happy with their experience?"
SQL: SELECT COUNT(*) FROM ryanair_reviews WHERE passenger_country = 'United States' AND (sentiment = 'Positive' OR overall_rating >= 7);

Question: "On average, how many customers were not happy with flight attendants?"
SQL: SELECT AVG(cabin_staff_service) as avg_rating, COUNT(*) as total_reviews FROM ryanair_reviews WHERE cabin_staff_service < 3;

Question: "Which countries have the most negative reviews?"
SQL: SELECT passenger_country, COUNT(*) as negative_count FROM ryanair_reviews WHERE sentiment = 'Negative' GROUP BY passenger_country ORDER BY negative_count DESC LIMIT 5;

Question: "What's the average rating for each aircraft type?"
SQL: SELECT aircraft, AVG(overall_rating) as avg_rating, COUNT(*) as review_count FROM ryanair_reviews WHERE aircraft IS NOT NULL GROUP BY aircraft ORDER BY avg_rating DESC;

Question: "What percentage of customers recommend Ryanair?"
SQL: SELECT ROUND((COUNT(CASE WHEN recommended = 'yes' THEN 1 END) * 100.0 / COUNT(*)), 2) as recommendation_percentage FROM ryanair_reviews;

IMPORTANT RULES:
- Use 'yes'/'no' for recommended column, NOT 1/0
- Use 'Positive'/'Neutral'/'Negative' for sentiment
- Rating columns are DECIMAL(3,1) from 1.0 to 5.0
- overall_rating is INTEGER from 1 to 10

Now convert this question to SQL:
Question: "{user_question}"
SQL:"""

    def clean_sql_response(self, response):
        """Extract clean SQL from LLM response"""
        import re
        
        # Remove SQL: prefix
        if response.startswith('SQL:'):
            response = response[4:].strip()
        
        # Remove code blocks
        response = re.sub(r'```sql\s*', '', response, flags=re.IGNORECASE)
        response = re.sub(r'```\s*', '', response)
        
        # Extract first SQL statement
        lines = response.split('\n')
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            if re.match(r'^(SELECT|INSERT|UPDATE|DELETE|WITH)', line, re.IGNORECASE):
                sql_lines = [line]
                continue
            elif sql_lines and line and not line.startswith(('In this', 'This', 'The')):
                sql_lines.append(line)
            elif line.endswith(';'):
                sql_lines.append(line)
                break
        
        return ' '.join(sql_lines).strip()

    def generate_sql(self, user_question):
        """Generate SQL query from natural language question"""
        try:
            prompt = self.get_query_prompt(user_question)
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "top_p": 0.9,
                        "num_predict": 100
                    }
                },
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                sql_query = self.clean_sql_response(result['response'])
                return sql_query
            else:
                return None
                
        except Exception as e:
            print(f"Error generating SQL: {e}")
            return None

    def fix_sql_error(self, original_query, error_message, user_question):
        """Fix SQL query based on error message"""
        fix_prompt = f"""You are a SQL error fixing expert. Fix the broken SQL query based on the error message.

Original Question: "{user_question}"
Broken SQL: {original_query}
Error: {error_message}

Database Schema:
Table: ryanair_reviews
Columns with data types and valid values:
- id: INTEGER (auto-increment primary key)
- date_published: DATE (format: YYYY-MM-DD)
- overall_rating: INTEGER (1-10 scale)
- passenger_country: VARCHAR (e.g., 'United States', 'United Kingdom', 'Germany')
- trip_verified: VARCHAR ('Trip Verified', 'Not Verified')
- comment_title: TEXT (review titles)
- comment: TEXT (full review text)
- aircraft: VARCHAR (e.g., 'Boeing 737-800', 'Boeing 737 MAX')
- type_of_traveller: VARCHAR ('Solo Leisure', 'Couple Leisure', 'Family Leisure', 'Business')
- seat_type: VARCHAR ('Economy Class', 'Premium Economy')
- origin: VARCHAR (departure city/airport)
- destination: VARCHAR (arrival city/airport)
- date_flown: VARCHAR (month/year format like 'January 2024')
- seat_comfort: DECIMAL(3,1) (1.0-5.0 rating scale)
- cabin_staff_service: DECIMAL(3,1) (1.0-5.0 rating scale)
- food_beverages: DECIMAL(3,1) (1.0-5.0 rating scale)
- ground_service: DECIMAL(3,1) (1.0-5.0 rating scale)
- value_for_money: DECIMAL(3,1) (1.0-5.0 rating scale)
- recommended: VARCHAR ('yes', 'no') - IMPORTANT: Use 'yes'/'no' NOT 1/0
- inflight_entertainment: DECIMAL(3,1) (1.0-5.0 rating scale)
- wifi_connectivity: DECIMAL(3,1) (1.0-5.0 rating scale)
- sentiment: VARCHAR ('Positive', 'Neutral', 'Negative')
- sentiment_reason: TEXT (explanation for sentiment)

Return ONLY the corrected SQL query:"""
        
        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": fix_prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 50
                    }
                },
                timeout=8
            )
            
            if response.status_code == 200:
                result = response.json()
                fixed_query = self.clean_sql_response(result['response'])
                return fixed_query
            else:
                return None
                
        except Exception as e:
            print(f"Error fixing SQL: {e}")
            return None

    def execute_query(self, sql_query, user_question=None, max_retries=5):
        """Execute SQL query with automatic error fixing"""
        # Check cache first
        cache_key = sql_query.strip().lower()
        if cache_key in self.query_cache:
            print("⚡ Using cached result")
            return self.query_cache[cache_key]
        
        attempts = []
        
        for attempt in range(max_retries):
            try:
                # Use text() to avoid SQLAlchemy parameter issues
                from sqlalchemy import text
                df = pd.read_sql(text(sql_query), self.engine)
                
                # Cache successful result
                self.query_cache[cache_key] = df
                return df
                
            except Exception as e:
                error_msg = str(e)
                attempts.append({'sql': sql_query, 'error': error_msg})
                print(f"❌ SQL Error (attempt {attempt + 1}): {error_msg}")
                
                if attempt < max_retries - 1 and user_question:
                    print(f"🔧 Attempting to fix SQL...")
                    fixed_query = self.fix_sql_error(sql_query, error_msg, user_question)
                    
                    if fixed_query and fixed_query != sql_query:
                        print(f"🔄 Fixed SQL: {fixed_query}")
                        sql_query = fixed_query
                    else:
                        print("❌ Could not fix SQL query")
                        break
                else:
                    print("❌ Max retries reached or no question provided")
                    break
        
        # Log error to database if all attempts failed
        if user_question and attempts:
            self.log_error_to_db(user_question, attempts)
        
        return None
    
    def log_error_to_db(self, user_question, attempts):
        """Log failed query attempts to database"""
        try:
            from create_error_table import log_query_error
            log_query_error(user_question, attempts)
        except Exception as e:
            print(f"⚠️ Could not log error to database: {e}")

    def format_answer(self, user_question, sql_query, results):
        """Format the results into a natural language answer with better styling"""
        if results is None or results.empty:
            return "📭 **No data found** for your question. Try rephrasing or asking about different aspects of the reviews."
        
        # Determine answer type based on question keywords
        question_lower = user_question.lower()
        
        # Single value results
        if len(results.columns) == 1 and len(results) == 1:
            value = results.iloc[0, 0]
            
            # Format based on question type
            if 'how many' in question_lower or 'count' in question_lower:
                return f"📊 **Total Count:** {value:,} reviews"
            elif 'average' in question_lower or 'avg' in question_lower:
                if isinstance(value, (int, float)):
                    return f"📈 **Average:** {value:.2f}"
                return f"📈 **Average:** {value}"
            elif 'percentage' in question_lower or '%' in question_lower:
                return f"📊 **Percentage:** {value}%"
            else:
                return f"✅ **Result:** {value}"
        
        # Two column results (typically category and count/value)
        elif len(results.columns) == 2:
            col1, col2 = results.columns
            answer = f"📋 **{col1.replace('_', ' ').title()} Analysis:**\n\n"
            
            for i, (_, row) in enumerate(results.head(10).iterrows(), 1):
                key = row.iloc[0]
                val = row.iloc[1]
                
                # Format value based on type
                if isinstance(val, (int, float)):
                    if val > 1000:
                        formatted_val = f"{val:,.0f}"
                    elif isinstance(val, float):
                        formatted_val = f"{val:.2f}"
                    else:
                        formatted_val = str(val)
                else:
                    formatted_val = str(val)
                
                answer += f"{i}. **{key}:** {formatted_val}\n"
            
            if len(results) > 10:
                answer += f"\n*Showing top 10 of {len(results)} results*"
            
            return answer
        
        # Multiple columns - show as formatted table
        else:
            answer = f"📊 **Detailed Results** ({len(results)} rows):\n\n"
            
            # Show first few rows in a readable format
            for i, (_, row) in enumerate(results.head(5).iterrows(), 1):
                answer += f"**#{i}**\n"
                for col in results.columns:
                    col_name = col.replace('_', ' ').title()
                    value = row[col]
                    
                    # Format different types of values
                    if pd.isna(value):
                        formatted_value = "N/A"
                    elif isinstance(value, (int, float)):
                        if col in ['overall_rating', 'seat_comfort', 'cabin_staff_service', 'food_beverages', 'ground_service', 'value_for_money']:
                            formatted_value = f"{value}/5 ⭐" if value <= 5 else f"{value}/10 ⭐"
                        else:
                            formatted_value = f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
                    elif col == 'comment' and len(str(value)) > 150:  # Long comments
                        formatted_value = f"{str(value)[:150]}... [Click to expand in dashboard for full comment]"
                    elif len(str(value)) > 100:  # Other long text
                        formatted_value = f"{str(value)[:100]}..."
                    else:
                        formatted_value = str(value)
                    
                    answer += f"• **{col_name}:** {formatted_value}\n"
                answer += "\n"
            
            if len(results) > 5:
                answer += f"*Showing first 5 of {len(results)} results*\n\n"
            
            # Add summary statistics if numeric columns exist
            numeric_cols = results.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                answer += "📈 **Quick Stats:**\n"
                for col in numeric_cols[:3]:  # Show stats for first 3 numeric columns
                    col_name = col.replace('_', ' ').title()
                    mean_val = results[col].mean()
                    answer += f"• **{col_name} Average:** {mean_val:.2f}\n"
            
            return answer

    def log_success_to_db(self, user_question, sql_query, answer_text):
        """Log successful query to database"""
        try:
            from create_error_table import log_successful_query
            log_successful_query(user_question, sql_query, answer_text)
        except Exception as e:
            print(f"⚠️ Could not log success to database: {e}")

    def answer_question(self, user_question):
        """Complete pipeline: question -> SQL -> results -> answer"""
        print(f"🤔 Question: {user_question}")
        
        # Generate SQL
        sql_query = self.generate_sql(user_question)
        if not sql_query:
            return """🤖 **I'm having trouble understanding your question.**
            
            Could you try rephrasing it? Here are some examples of questions I can help with:
            
            **📊 Statistics & Counts:**
            • "How many reviews are there in total?"
            • "How many customers from Germany left reviews?"
            • "What percentage of customers recommend Ryanair?"
            
            **🌍 Country Analysis:**
            • "Which countries have the most reviews?"
            • "Show me reviews from the United States"
            
            **😊 Sentiment Analysis:**
            • "How many positive reviews are there?"
            • "Show me negative reviews about staff"
            • "What are customers saying about food?"
            
            **✈️ Aircraft & Routes:**
            • "Which aircraft type has the best ratings?"
            • "Show me reviews for Boeing 737"
            
            **⭐ Ratings Analysis:**
            • "What's the average rating by country?"
            • "Show me reviews with rating above 8"""
        
        print(f"🔍 Generated SQL: {sql_query}")
        
        # Execute query with auto-fix
        results = self.execute_query(sql_query, user_question)
        if results is None:
            return """🔧 **I encountered some technical difficulties** while processing your question.
            
            The error has been logged and our system will learn from this to improve future responses.
            
            **Please try:**
            • Rephrasing your question slightly
            • Being more specific about what you want to know
            • Asking a simpler version first
            
            **Quick examples to try:**
            • "How many reviews are there?"
            • "What countries have reviews?"
            • "Show me positive reviews"
            • "What's the average rating?"
            
            Thank you for your patience! 🙏"""
        
        # Format answer
        answer = self.format_answer(user_question, sql_query, results)
        print(f"✅ Answer: {answer}")
        
        # Log successful query
        self.log_success_to_db(user_question, sql_query, answer)
        
        return answer

# Usage example
if __name__ == "__main__":
    agent = QueryAgent()
    
    # Test questions
    questions = [
        "How many customers from the United States used Ryanair and were happy with their experience?",
        "What's the average overall rating by country?",
        "How many negative reviews are there?",
        "Which aircraft type has the best ratings?",
        "What percentage of customers recommend Ryanair?",
        "share with me comments of customers who had negative experience with reason from staff"
    ]
    
    for question in questions:
        print("\n" + "="*50)
        agent.answer_question(question)
        print("="*50)