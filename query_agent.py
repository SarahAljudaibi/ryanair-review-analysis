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
            model="google/gemma-2-9b-it",#"meta-llama/Llama-3.2-1B-Instruct",
            token=self.token
        )

        # Strong repair & intent reinterpretation model
        self.client_repair = InferenceClient(
            model="google/gemma-2-9b-it",#"Qwen/Qwen2.5-7B-Instruct",
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
- DatePublished: Date the review was posted (YYYY-MM-DD).
- OverallRating: Rating score (1–10).
- PassengerCountry: Passenger’s country of origin.
- TripVerified: “Trip Verified” or “Not Verified”.
- CommentTitle: Title of the review.
- Comment: Full text written by the passenger. Contains issues like:
  - fees, charges, expensive prices, delays, rude staff, lost bags, etc.
- Aircraft: Aircraft model (e.g., Boeing 737-800).
- TypeOfTraveller: Solo, Couple Leisure, Family Leisure, Business, etc.
- SeatType: Economy Class, Business Class.
- Origin: Departure city.
- Destination: Arrival city.
- DateFlown: Period flown (e.g., “Oct-23”).
- SeatComfort: Seat comfort rating.
- CabinStaffService: Cabin crew rating.
- FoodBeverages: Food and drinks rating.
- GroundService: Airport service rating.
- ValueForMoney: Value for money rating.
- Recommended: Yes / No.
- InflightEntertainment: Entertainment rating (nullable).
- WifiConnectivity: WiFi rating (nullable).
- Sentiment: Positive or Negative.
- SentimentReason: AI-generated list of topic tags summarizing main themes of the comment.
  Example: “impressed with price, soft seats, plenty of legroom”.

Example real row from the dataset:
id: 3
DatePublished: "2024-01-20"
OverallRating: 10
PassengerCountry: "United Kingdom"
TripVerified: "Trip Verified"
CommentTitle: "Really impressed!"
Comment: "Really impressed! You get what you pay for... Highly recommend."
Aircraft: "Boeing 737-800"
TypeOfTraveller: "Couple Leisure"
SeatType: "Economy Class"
Origin: "Edinburgh"
Destination: "Paris Beauvais"
DateFlown: "Oct-23"
SeatComfort: 5
CabinStaffService: 5
FoodBeverages: 4
GroundService: 5
ValueForMoney: 5
Recommended: "yes"
Sentiment: "Positive"
SentimentReason: "impressed with price, soft seats, plenty of legroom"

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
1. Comment (long free text)
2. SentimentReason (AI-generated list of topic keywords)

Because:
- The Comment may contain long descriptions.
- SentimentReason contains extracted topics that help identify meaning.

Always use SQL like:

WHERE (
    LOWER(Comment) LIKE '%<keyword1>%' OR
    LOWER(Comment) LIKE '%<synonym1>%' OR
    LOWER(Comment) LIKE '%<synonym2>%' 
)
OR (
    LOWER(SentimentReason) LIKE '%<keyword1>%' OR
    LOWER(SentimentReason) LIKE '%<synonym1>%'
)

Examples:

User: "How many customers complained about high fees?"
SQL:
SELECT COUNT(*)
FROM ryanair_reviews
WHERE (
    LOWER(Comment) LIKE '%fee%' OR
    LOWER(Comment) LIKE '%price%' OR
    LOWER(Comment) LIKE '%expensive%' OR
    LOWER(Comment) LIKE '%charge%' OR
    LOWER(Comment) LIKE '%cost%'
)
OR (
    LOWER(SentimentReason) LIKE '%fee%' OR
    LOWER(SentimentReason) LIKE '%price%'
);

User: "Retrieve all comments from Turkish passengers"
SQL:
SELECT comment
FROM ryanair_reviews
WHERE PassengerCountry = 'Turkey';

User: "What is the average overall rating by country?"
SQL:
SELECT PassengerCountry, AVG(OverallRating) AS AvgRating
FROM ryanair_reviews
GROUP BY PassengerCountry;

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
You are an expert PostgreSQL SQL mechanic. 
Your job is to FIX invalid SQL queries so they successfully run.

You MUST follow these rules:

1. ALWAYS generate correct PostgreSQL SQL.
2. ALWAYS use the correct table name: ryanair_reviews
3. ONLY use existing column names:
   id, DatePublished, OverallRating, PassengerCountry, TripVerified,
   CommentTitle, Comment, Aircraft, TypeOfTraveller, SeatType, 
   Origin, Destination, DateFlown, SeatComfort, CabinStaffService,
   FoodBeverages, GroundService, ValueForMoney, Recommended,
   InflightEntertainment, WifiConnectivity, Sentiment, SentimentReason

4. When the query uses grouping or aggregation, ALWAYS include GROUP BY.
5. Never guess column names. Only use the list above.
6. If the user question requests a count by category, generate:
   SELECT <column>, COUNT(*) FROM ryanair_reviews GROUP BY <column>
7. If the query contains errors, FIX them step-by-step.
8. If the SQL is structurally wrong, rewrite it from scratch.

User question:
{user_question}

Broken SQL:
{bad_sql}

SQL Error:
{error_msg}

Now produce VALID PostgreSQL SQL that answers the question.
Return ONLY the fixed SQL (no explanation)
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
        st.info(f"🧠 Rewritten question: `{cleaned}`")
        sql_query = self.generate_sql(cleaned)

        if not sql_query:
            return "Couldn't generate SQL for that question."            
        st.code(sql_query, language="sql")  # ⬅ SHOW GENERATED SQL


        df = self.execute_query(sql_query, cleaned)
        return self.format_answer(df)
