import streamlit as st
import pandas as pd

st.title("Test Streamlit App")
st.write("Hello World!")

# Test basic functionality
if st.button("Test Button"):
    st.success("Button works!")

# Test imports
try:
    from sentiment_agent import SentimentAgent
    from query_agent import QueryAgent
    st.success("✅ All imports successful!")
except Exception as e:
    st.error(f"❌ Import error: {e}")