import pandas as pd
from sqlalchemy import text
import streamlit as st
from huggingface_hub import InferenceClient
from sqlite_config import get_sqlite_engine
import re


class QueryAgent:
    def __init__(self):
        # HF Token
        self.token = st.secrets["HF_API_KEY"]

        # Main SQL generation (fast)
        self.client_main = InferenceClient(
            model="meta-llama/Llama-3.2-1B-Instruct",
            token=self.token
        )

        # Strong repair & intent reinterpretation model
        self.client_repair = InferenceClient(
            model="Qwen/Qwen2.5-7B-Instruct",
            token=self.token
        )

        # DB connection
        self.engine = get_sqlite_engine()

    # -----------------------------------------------------------
    #==================== PROMPT BUILDER ========================
    #------------------------------------------------------------
    def get_query_prompt(self, user_question: str) -> str:
        return f"""
You are an expert SQL assistant for a PostgreSQL table named `ryanair_reviews`.

Below are ALL columns and their meanings:

- id: Unique review identifier.
- date_published: Date the review was posted (YYYY-MM-DD).
- overall_rating: Rating score (1–10).
- passenger_country: Passenger’s country of origin.
- trip_verified: “Trip Verified” or “Not Verified”.
- comment_title: Title of the review.
- comment: Full text written by the passenger. Contains issues like:
  - fees, charges, expensive prices, delays, rude staff, lost bags, etc.
- aircraft: Aircraft model (e.g., Boeing 737-800).
- type_of_traveller: Solo, Couple Leisure, Family Leisure, Business, etc.
- seat_type: Economy Class, Business Class.
- origin: Departure city.
- destination: Arrival city.
- date_flown: Period flown (e.g., “Oct-23”).
- seat_comfort: Seat comfort rating.
- cabin_staff_service: Cabin crew rating.
- food_beverages: Food and drinks rating.
- ground_service: Airport service rating.
- value_for_money: Value for money rating.
- recommended: Yes / No.
- inflight_entertainment: Entertainment rating (nullable).
- wifi_connectivity: WiFi rating (nullable).
- sentiment: Positive or Negative.
- sentiment_reason: AI-generated list of topic tags summarizing main themes of the comment.
  Example: “impressed with price, soft seats, plenty of legroom”.

Example real row from the dataset:
id: 3
date_published: "2024-01-20"
overall_rating: 10
passenger_country: "United Kingdom"
trip_verified: "Trip Verified"
comment_title: "Really impressed!"
comment: "Really impressed! You get what you pay for... Highly recommend."
aircraft: "Boeing 737-800"
type_of_traveller: "Couple Leisure"
seat_type: "Economy Class"
origin: "Edinburgh"
destination: "Paris Beauvais"
date_flown: "Oct-23"
seat_comfort: 5
cabin_staff_service: 5
food_beverages: 4
ground_service: 5
value_for_money: 5
recommended: "yes"
sentiment: "Positive"
sentiment_reason: "impressed with price, soft seats, plenty of legroom"

IMPORTANT:
When the user asks about ANY topic in the reviews — whether positive, neutral, or negative —
such as:

- high fees, expensive prices, extra charges, hidden fees
- delays, late flights, waiting times
- rude staff, excellent staff, good service, bad service
- lost baggage, baggage issues
- seat comfort, legroom, soft seats
- cleanliness, cabin condition
- value for money, price satisfaction
- turbulence, smooth flight
- entertainment, WiFi
- food quality, beverages
- boarding experience, ground service

You MUST search for these topics using a HYBRID FILTER on BOTH columns:
1. comment (long free text)
2. sentiment_reason (AI-generated list of topic keywords)

Because:
- The comment may contain long descriptions.
- sentiment_reason contains extracted topics that help identify meaning.

Always use SQL like:

WHERE (
    LOWER(comment) LIKE '%<keyword1>%' OR
    LOWER(comment) LIKE '%<synonym1>%' OR
    LOWER(comment) LIKE '%<synonym2>%' 
)
OR (
    LOWER(sentiment_reason) LIKE '%<keyword1>%' OR
    LOWER(sentiment_reason) LIKE '%<synonym1>%'
)

Examples:

User: "How many customers complained about high fees?"
SQL:
SELECT COUNT(*)
FROM ryanair_reviews
WHERE (
    LOWER(comment) LIKE '%fee%' OR
    LOWER(comment) LIKE '%price%' OR
    LOWER(comment) LIKE '%expensive%' OR
    LOWER(comment) LIKE '%charge%' OR
    LOWER(comment) LIKE '%cost%'
)
OR (
    LOWER(sentiment_reason) LIKE '%fee%' OR
    LOWER(sentiment_reason) LIKE '%price%'
);

User: "Retrieve all comments from Turkish passengers"
SQL:
SELECT comment
FROM ryanair_reviews
WHERE passenger_country = 'Turkey';

User: "What is the average overall rating by country?"
SQL:
SELECT passenger_country, AVG(overall_rating) AS avg_rating
FROM ryanair_reviews
GROUP BY passenger_country;

Now generate SQL ONLY. No explanation.

User question:
{user_question}

SQL:
"""

    # -----------------------------------------------------------
    #============== INTENT CLEANING BEFORE SQL ==================
    #------------------------------------------------------------
    def interpret_question(self, user_question: str) -> str:
        """Rewrite vague question → clean SQL-friendly question."""
        prompt = f"""
Rewrite the user's question clearly and explicitly for SQL.
Clarify vague terms like: fee, expensive, delay, service.
Return only the rewritten question.

User: {user_question}
Rewritten:
"""
        response = self.client_repair.chat_completion(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=120,
            temperature=0.0
        )
        return response.choices[0].message["content"].strip()

    # -----------------------------------------------------------
    #==================== SQL GENERATION =========================
    #------------------------------------------------------------
    def generate_sql(self, user_question: str) -> str:
        prompt = self.get_query_prompt(user_question)

        try:
            response = self.client_main.chat_completion(
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
                temperature=0.1
            )
            raw = response.choices[0].message["content"]
            return self.clean_sql(raw)
        except Exception as e:
            st.warning(f"SQL generation error: {e}")
            return None

    # -----------------------------------------------------------
    #============== CLEAN RAW SQL FROM LLM ======================
    #------------------------------------------------------------
    def clean_sql(self, text: str) -> str:
        text = re.sub(r"```sql|```", "", text).strip()
        if text.lower().startswith("sql:"):
            text = text[4:].strip()
        return text.split(";")[0].strip() + ";"

    # -----------------------------------------------------------
    #====================== SQL REPAIR ==========================
    #------------------------------------------------------------
    def repair_sql(self, bad_sql, error_msg, user_question):
        for attempt in range(1, 6):
            st.info(f"🔧 Attempt {attempt}/5 to fix SQL...")

            prompt = f"""
Fix the SQL so it works in PostgreSQL.

User question:
{user_question}

Bad SQL:
{bad_sql}

Error:
{error_msg}

Return ONLY the fixed SQL.
"""

            try:
                response = self.client_repair.chat_completion(
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=200,
                    temperature=0.0,
                )
                candidate = self.clean_sql(response.choices[0].message["content"])

                try:
                    pd.read_sql(candidate, self.engine)
                    return candidate  # success!
                except:
                    continue

            except:
                continue

        return None

    # -----------------------------------------------------------
    #==================== EXECUTE SQL ===========================
    #------------------------------------------------------------
    def execute_query(self, sql_query, user_question):
        try:
            return pd.read_sql(sql_query, self.engine)
        except Exception as err:
            fixed = self.repair_sql(sql_query, str(err), user_question)
            if fixed:
                st.success("✅ SQL fixed automatically!")
                return pd.read_sql(fixed, self.engine)

            return pd.DataFrame([{"UnfixableError": str(err)}])

    # -----------------------------------------------------------
    #=================== FORMAT ANSWER ==========================
    #------------------------------------------------------------
    def format_answer(self, df: pd.DataFrame):
        if df.empty:
            return "No results found."

        if "UnfixableError" in df.columns:
            return "❌ Sorry, I couldn't fix the SQL after 5 attempts. Try rephrasing the question."

        if len(df.columns) == 1:
            return f"**Result:** {df.iloc[0, 0]}"

        return df.to_markdown(index=False)

    # -----------------------------------------------------------
    #======================== MAIN API ==========================
    #------------------------------------------------------------
    def answer_question(self, user_question: str) -> str:

        cleaned = self.interpret_question(user_question)
        sql_query = self.generate_sql(cleaned)

        if not sql_query:
            return "Couldn't generate SQL for that question."

        df = self.execute_query(sql_query, cleaned)
        return self.format_answer(df)
