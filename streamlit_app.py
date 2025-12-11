import streamlit as st
import pandas as pd
from sentiment_agent import SentimentAgent
from query_agent import QueryAgent
import uuid
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Ryanair Review Analysis",
    page_icon="✈️",
    layout="wide"
)

# Initialize agents
@st.cache_resource
def get_agents():
    return SentimentAgent(), QueryAgent()

sentiment_agent, query_agent = get_agents()

# Initialize session state
if 'chat_tabs' not in st.session_state:
    st.session_state.chat_tabs = {"Chat 1": {"id": str(uuid.uuid4()), "messages": []}}
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "Chat 1"

# Sidebar
st.sidebar.title("🛫 Ryanair Analysis")
page = st.sidebar.selectbox("Choose Page", ["💬 Chat Analysis", "📊 Sentiment Dashboard"])

if page == "💬 Chat Analysis":
    st.title("💬 Ryanair Review Chat Analysis")
    
    # Chat tab management
    col1, col2, col3 = st.columns([3, 1, 1])
    
    with col1:
        # Tab selector
        tab_names = list(st.session_state.chat_tabs.keys())
        selected_tab = st.selectbox("Select Chat Tab", tab_names, index=tab_names.index(st.session_state.active_tab))
        st.session_state.active_tab = selected_tab
    
    with col2:
        # Add new tab
        if st.button("➕ New Chat"):
            new_tab_name = f"Chat {len(st.session_state.chat_tabs) + 1}"
            st.session_state.chat_tabs[new_tab_name] = {"id": str(uuid.uuid4()), "messages": []}
            st.session_state.active_tab = new_tab_name
            st.rerun()
    
    with col3:
        # Delete tab
        if len(st.session_state.chat_tabs) > 1 and st.button("🗑️ Delete"):
            del st.session_state.chat_tabs[st.session_state.active_tab]
            st.session_state.active_tab = list(st.session_state.chat_tabs.keys())[0]
            st.rerun()
    
    # Current chat messages
    current_chat = st.session_state.chat_tabs[st.session_state.active_tab]
    
    # Display chat messages
    for message in current_chat["messages"]:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                st.markdown(message["content"])  # Use markdown for assistant responses
            else:
                st.write(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about Ryanair reviews..."):
        # Add user message
        current_chat["messages"].append({"role": "user", "content": prompt})
        
        with st.chat_message("user"):
            st.write(prompt)
        
        # Get AI response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing your question..."):
                response = query_agent.answer_question(prompt)
                st.markdown(response)  # Use markdown for better formatting
                
                # Add assistant message
                current_chat["messages"].append({"role": "assistant", "content": response})
    
    # Sidebar for adding reviews
    st.sidebar.markdown("---")
    st.sidebar.subheader("📝 Add New Review")
    
    with st.sidebar.expander("Upload Excel File"):
        uploaded_file = st.file_uploader("Choose Excel file", type=['xlsx', 'xls'])
        if uploaded_file and st.button("Upload & Analyze"):
            with st.spinner("Processing Excel file..."):
                # Save uploaded file temporarily
                temp_path = f"temp_{uploaded_file.name}"
                with open(temp_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Process file
                sentiment_agent.add_sentiment_column()
                review_ids = sentiment_agent.add_reviews_from_excel(temp_path)
                
                if review_ids:
                    st.success(f"✅ Added {len(review_ids)} reviews")
                    for review_id in review_ids:
                        sentiment_agent.process_single_review(review_id)
                    st.success("✅ Sentiment analysis completed!")
                else:
                    st.error("❌ Failed to process Excel file")
    
    with st.sidebar.expander("Manual Entry"):
        with st.form("manual_review"):
            comment = st.text_area("Review Comment*", height=100)
            rating = st.slider("Overall Rating", 1, 10, 5)
            country = st.text_input("Country")
            aircraft = st.text_input("Aircraft Type")
            
            if st.form_submit_button("Add & Analyze"):
                if comment.strip():
                    with st.spinner("Adding review..."):
                        sentiment_agent.add_sentiment_column()
                        review_id = sentiment_agent.add_new_review(
                            comment, rating, country or None, aircraft or None
                        )
                        
                        if review_id:
                            st.success(f"✅ Review added (ID: {review_id})")
                            sentiment_agent.process_single_review(review_id)
                            st.success("✅ Sentiment analysis completed!")
                        else:
                            st.error("❌ Failed to add review")
                else:
                    st.error("❌ Comment is required!")

elif page == "📊 Sentiment Dashboard":
    st.title("📊 Sentiment Analysis Dashboard")
    
    # Load sentiment data
    @st.cache_data(ttl=60)  # Cache for 1 minute
    def load_sentiment_data():
        try:
            from sqlite_config import get_sqlite_engine
            engine = get_sqlite_engine()
            
            query = """
                SELECT id, Comment as comment, sentiment, sentiment_reason, 
                       "OverallRating" as overall_rating, 
                       "PassengerCountry" as passenger_country, 
                       Aircraft as aircraft, "DatePublished" as date_published
                FROM ryanair_reviews 
                WHERE sentiment IS NOT NULL AND sentiment != ''
                ORDER BY id DESC
            """
            
            return pd.read_sql(query, engine)
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return pd.DataFrame()
    
    df = load_sentiment_data()
    
    if not df.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Reviews", len(df))
        
        with col2:
            positive_pct = (df['sentiment'] == 'Positive').mean() * 100
            st.metric("Positive %", f"{positive_pct:.1f}%")
        
        with col3:
            negative_pct = (df['sentiment'] == 'Negative').mean() * 100
            st.metric("Negative %", f"{negative_pct:.1f}%")
        
        with col4:
            avg_rating = df['overall_rating'].mean()
            st.metric("Avg Rating", f"{avg_rating:.1f}/10")
        
        # Sentiment distribution
        st.subheader("📈 Sentiment Distribution")
        sentiment_counts = df['sentiment'].value_counts()
        st.bar_chart(sentiment_counts)
        
        # Filters
        st.subheader("🔍 Filter Reviews")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            sentiment_filter = st.selectbox("Sentiment", ["All"] + list(df['sentiment'].unique()))
        
        with col2:
            country_filter = st.selectbox("Country", ["All"] + list(df['passenger_country'].dropna().unique()))
        
        with col3:
            min_rating = st.slider("Min Rating", 1, 10, 1)
        
        # Apply filters
        filtered_df = df.copy()
        if sentiment_filter != "All":
            filtered_df = filtered_df[filtered_df['sentiment'] == sentiment_filter]
        if country_filter != "All":
            filtered_df = filtered_df[filtered_df['passenger_country'] == country_filter]
        filtered_df = filtered_df[filtered_df['overall_rating'] >= min_rating]
        
        # Display filtered results
        st.subheader(f"📋 Reviews ({len(filtered_df)} results)")
        
        for _, row in filtered_df.head(20).iterrows():
            # Show preview of comment in expander title
            comment_preview = row['comment'][:50] + "..." if len(str(row['comment'])) > 50 else row['comment']
            with st.expander(f"Review #{row['id']} - {row['sentiment']} ({row['overall_rating']}/10) - {comment_preview}"):
                st.markdown(f"**Full Comment:**\n\n{row['comment']}")
                st.write("**Sentiment Reason:**", row['sentiment_reason'])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.write("**Country:**", row['passenger_country'] or "N/A")
                with col2:
                    st.write("**Aircraft:**", row['aircraft'] or "N/A")
                with col3:
                    st.write("**Date:**", row['date_published'])
        
        # Refresh button
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    else:
        st.info("No sentiment analysis data available. Add some reviews first!")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("**🛫 Ryanair Review Analysis System**")
st.sidebar.markdown("Powered by Llama 3.2 & Streamlit")
