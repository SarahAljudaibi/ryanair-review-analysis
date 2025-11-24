import pandas as pd
from sqlalchemy import text
import streamlit as st
from huggingface_hub import InferenceClient
from sqlite_config import get_sqlite_engine
import re


class QueryAgent:
    def __init__(self):
        # --- HuggingFace Token ---
        self.token = st.secrets["HF_API_KEY"]

        # --- Main (Fast) SQL Generation Model ---
        self.client_main = InferenceClient(
            model="meta-llama/Llama-3.2-1B-Instruct",
            token=self.token
        )

        # --- Stronger Model for SQL Repair ---
        self.client_repair = InferenceClient(
            model="Qwen/Qwen2.5-7B-Instruct",
            token=self.token
        )

        # --- SQLite Engine ---
        self.engine = get_sqlite_engine()
        self.query_cache = {}

    # -----------------------------------------------------------
    # PROMPT BUILDER
    # -----------------------------------------------------------
    def get_query_prompt(self, user_question: str) -> str:
        return f"""
You are an expert SQL assistant. Convert the user's question into a valid SQLite query
for a table named ryanair_reviews.

Only output SQL — no explanations.

Examples:

Q: How many positive reviews are there?
A: SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Positive';

Q: What is the average overall rating by country?
A: SELECT "Passenger Country", AVG("Overall Rating") AS avg_rating
   FROM ryanair_reviews
   GROUP BY "Passenger Country";

Now convert this question:
Q: {user_question}
A:
"""

    # -----------------------------------------------------------
    # MAIN SQL GENERATION
    # -----------------------------------------------------------
    def generate_sql(self, user_question: str) -> str:
        prompt = self.get_query_prompt(user_question)

        try:
            response = self.client_main.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0.1
            )

            model_text = response.choices[0].message["content"]
            sql_query = self.clean_sql_response(model_text)
            return sql_query

        except Exception as e:
            st.warning(f"⚠️ Error generating SQL: {e}")
            return self.fallback_sql(user_question)

    # -----------------------------------------------------------
    # SQL POST-PROCESSING
    # -----------------------------------------------------------
    def clean_sql_response(self, text: str) -> str:
        """Clean up model output to extract pure SQL."""
        text = text.strip()
        text = re.sub(r"```sql|```", "", text)
        if text.lower().startswith("sql:"):
            text = text[4:].strip()
        return text.split(";")[0].strip() + ";"

    # -----------------------------------------------------------
    # FALLBACK SQL RULES
    # -----------------------------------------------------------
    def fallback_sql(self, user_question: str) -> str:
        q = user_question.lower()

        if "positive" in q:
            return "SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Positive';"

        if "negative" in q:
            return "SELECT COUNT(*) FROM ryanair_reviews WHERE sentiment = 'Negative';"

        if "average" in q and "country" in q:
            return """
                SELECT "Passenger Country",
                       AVG("Overall Rating")
                FROM ryanair_reviews
                GROUP BY "Passenger Country";
            """

        return "SELECT COUNT(*) FROM ryanair_reviews;"  # Safe default

    # -----------------------------------------------------------
    # SQL REPAIR LOGIC (5 ATTEMPTS)
    # -----------------------------------------------------------
    def repair_sql(self, bad_sql: str, error_message: str, user_question: str) -> str:
        for attempt in range(1, 6):
            st.info(f"🔧 Attempt {attempt}/5 to fix SQL error...")

            repair_prompt = f"""
You are an expert SQLite engineer.
The SQL query failed. Fix it.

User question:
{user_question}

Bad SQL:
{bad_sql}

Error:
{error_message}

Return ONLY corrected SQL.
"""

            try:
                response = self.client_repair.chat_completion(
                    messages=[{"role": "user", "content": repair_prompt}],
                    max_tokens=200,
                    temperature=0.0
                )

                fixed_sql = self.clean_sql_response(
                    response.choices[0].message["content"]
                )

                # Try executing repaired SQL
                try:
                    pd.read_sql(text(fixed_sql), self.engine)
                    return fixed_sql  # Success!
                except Exception:
                    continue  # Try again

            except Exception:
                continue

        return None  # All 5 attempts failed

    # -----------------------------------------------------------
    # SQL EXECUTION (WITH AUTO-REPAIR)
    # -----------------------------------------------------------
    def execute_query(self, sql_query: str, user_question: str = None) -> pd.DataFrame:
        try:
            return pd.read_sql(text(sql_query), self.engine)

        except Exception as error:
            if user_question is None:
                return pd.DataFrame([{"Error": str(error)}])

            # Try to fix SQL
            fixed_sql = self.repair_sql(sql_query, str(error), user_question)

            if fixed_sql:
                st.success("✅ SQL error fixed automatically!")
                return pd.read_sql(text(fixed_sql), self.engine)

            # Unfixable
            return pd.DataFrame([{"UnfixableError": str(error)}])

    # -----------------------------------------------------------
    # RESULT FORMATTING
    # -----------------------------------------------------------
    def format_answer(self, user_question: str, df: pd.DataFrame) -> str:
        if df.empty:
            return "No results found."

        if "Error" in df.columns:
            return f"⚠️ SQL Error: {df.iloc[0]['Error']}"

        if "UnfixableError" in df.columns:
            return (
                "😔 Sorry, I couldn't fix this SQL even after 5 attempts. "
                "Try rephrasing your question."
            )

        if len(df.columns) == 1:
            return f"**Result:** {df.iloc[0, 0]}"

        return df.to_markdown(index=False)

    # -----------------------------------------------------------
    # MAIN USER-FACING PIPELINE
    # -----------------------------------------------------------
    def answer_question(self, user_question: str) -> str:
        sql_query = self.generate_sql(user_question)

        if not sql_query:
            return "I couldn't generate a SQL query for that question."

        df = self.execute_query(sql_query, user_question=user_question)
        return self.format_answer(user_question, df)
