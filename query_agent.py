

import pandas as pd
from sqlalchemy import text
import streamlit as st
from huggingface_hub import InferenceClient
from sqlite_config import get_sqlite_engine
import re


class QueryAgent:
    def __init__(self):
        # ✅ Load Hugging Face token from Streamlit secrets (your working setup)
        self.token = st.secrets["HF_API_KEY"]

        # ✅ Initialize client using Llama-3.2-1B-Instruct
        self.client = InferenceClient(
            model="mistralai/Mistral-7B-Instruct-v0.3",#"meta-llama/Llama-3.2-1B-Instruct",
            token=self.token
        )

        # ✅ Initialize SQLite engine
        self.engine = get_sqlite_engine()
        self.query_cache = {}

    # -------------------- Helper Methods --------------------

    def get_query_prompt(self, user_question: str) -> str:
        """Build a structured few-shot prompt for generating SQL."""
        return f"""
You are an expert SQL assistant. Convert the user's question into a valid SQLite query
for a table named ryanair_reviews.

Example questions and SQL:

Q: How many positive reviews are there?
A: SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Positive';

Q: What is the average overall rating by country?
A: SELECT "Passenger Country", AVG("Overall Rating") AS avg_rating FROM ryanair_reviews GROUP BY "Passenger Country";

Now answer:
Q: {user_question}
A:
"""

    def generate_sql(self, user_question: str) -> str:
        """Use Hugging Face Llama-3.2-1B-Instruct to generate SQL."""
        prompt = self.get_query_prompt(user_question)
        try:
            response = self.client.text_generation(
                prompt,
                max_new_tokens=120,
                temperature=0.1,
            )
            sql_query = self.clean_sql_response(response)
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
            return 'SELECT "Passenger Country", AVG("Overall Rating") FROM ryanair_reviews GROUP BY "Passenger Country";'
        else:
            return "SELECT COUNT(*) FROM ryanair_reviews;"

    def execute_query(self, sql_query: str) -> pd.DataFrame:
        """Execute the generated SQL query and return a DataFrame."""
        try:
            df = pd.read_sql(text(sql_query), self.engine)
            return df
        except Exception as e:
            return pd.DataFrame([{"Error": str(e)}])

    def format_answer(self, user_question: str, df: pd.DataFrame) -> str:
        """Format the SQL query result for display."""
        if df.empty:
            return "No results found."
        elif "Error" in df.columns:
            return f"⚠️ SQL Error: {df.iloc[0]['Error']}"
        elif len(df.columns) == 1:
            return f"**Result:** {df.iloc[0, 0]}"
        else:
            return df.to_markdown(index=False)

    def answer_question(self, user_question: str) -> str:
        """Full pipeline: user question → SQL → execution → formatted answer."""
        sql_query = self.generate_sql(user_question)
        if not sql_query:
            return "I couldn't generate an SQL query for that question."

        df = self.execute_query(sql_query)
        answer = self.format_answer(user_question, df)
        return answer
