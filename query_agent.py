import pandas as pd
from sqlalchemy import text
import streamlit as st
from huggingface_hub import InferenceClient
from sqlite_config import get_sqlite_engine
import re


class QueryAgent:
    def __init__(self):
        # Load HF token
        self.token = st.secrets["HF_API_KEY"]

        # Use conversational-capable model
        self.client = InferenceClient(
            model="meta-llama/Llama-3.2-1B-Instruct",   # or replace with Qwen 1.5B if you prefer text-generation
            token=self.token
        )

        # DB engine
        self.engine = get_sqlite_engine()
        self.query_cache = {}

    # -------------------- Helper Methods --------------------

    def get_query_prompt(self, user_question: str) -> str:
        """Build a structured few-shot SQL instruction prompt."""
        return f"""
You are an expert SQL assistant. Convert the user's question into a valid SQLite query
for a table named ryanair_reviews.

Only output SQL — no explanation.

Example questions and SQL:

Q: How many positive reviews are there?
A: SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Positive';

Q: What is the average overall rating by country?
A: SELECT "Passenger Country", AVG("Overall Rating") AS avg_rating
   FROM ryanair_reviews
   GROUP BY "Passenger Country";

Now answer:
Q: {user_question}
A:
"""

    def generate_sql(self, user_question: str) -> str:
        """Generate SQL using conversational endpoint (required by Streamlit Cloud)."""
        prompt = self.get_query_prompt(user_question)

        try:
            response = self.client.chat_completion(
                messages=[
                    {"role": "user", "content": prompt}
                ],
                max_tokens=200,
                temperature=0.1
            )
    
            model_text = response.choices[0].message["content"]
            sql_query = self.clean_sql_response(model_text)
            return sql_query

        except Exception as e:
            st.warning(f"⚠️ Error generating SQL via Hugging Face: {e}")
            return self.fallback_sql(user_question)

    def clean_sql_response(self, text: str) -> str:
        """Extract a clean SQL statement from the model output."""
        text = text.strip()
        text = re.sub(r"```sql|```", "", text)
        if text.lower().startswith("sql:"):
            text = text[4:]
        return text.strip().split(";")[0] + ";"

    def fallback_sql(self, user_question: str) -> str:
        """Provide a fallback SQL query when generation fails."""
        q = user_question.lower()
        if "positive" in q:
            return "SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Positive';"
        elif "negative" in q:
            return "SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Negative';"
        elif "average" in q and "country" in q:
            return (
                'SELECT "Passenger Country", AVG("Overall Rating") '
                'FROM ryanair_reviews GROUP BY "Passenger Country";'
            )
        else:
            return "SELECT COUNT(*) FROM ryanair_reviews;"

    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """Execute SQL query."""
        try:
            df = pd.read_sql(text(sql_query), self.engine)
            return df
        except Exception as e:
            return pd.DataFrame([{"Error": str(e)}])

    def format_answer(self, user_question: str, df: pd.DataFrame) -> str:
        """Format SQL output."""
        if df.empty:
            return "No results found."
        elif "Error" in df.columns:
            return f"⚠️ SQL Error: {df.iloc[0]['Error']}"
        elif len(df.columns) == 1:
            return f"**Result:** {df.iloc[0, 0]}"
        else:
            return df.to_markdown(index=False)

    def answer_question(self, user_question: str) -> str:
        """Full pipeline: question → SQL → execution → formatted answer."""
        sql_query = self.generate_sql(user_question)
        if not sql_query:
            return "I couldn't generate an SQL query for that question."

        df = self.execute_query(sql_query)
        answer = self.format_answer(user_question, df)
        return answer
