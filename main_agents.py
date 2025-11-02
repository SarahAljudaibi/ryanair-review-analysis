from sentiment_agent import SentimentAgent
from query_agent import QueryAgent

def main():
    """Main function to run both agents"""
    
    print("🚀 Ryanair Review Analysis System")
    print("="*50)
    
    # Initialize agents
    sentiment_agent = SentimentAgent()
    query_agent = QueryAgent()
    
    while True:
        print("\nChoose an option:")
        print("1. Sentiment Analysis (add new review or process existing)")
        print("2. Ask a question about the data")
        print("3. Exit")
        
        choice = input("\nEnter your choice (1-3): ").strip()
        
        if choice == "1":
            print("\n📊 Sentiment Analysis Options:")
            print("a. Add new review and analyze")
            print("b. Analyze existing unprocessed reviews")
            
            sub_choice = input("\nChoose (a/b): ").strip().lower()
            
            if sub_choice == "a":
                print("\n✍️ Add New Review Options:")
                print("1. Upload Excel file")
                print("2. Enter review manually")
                
                input_choice = input("\nChoose (1/2): ").strip()
                
                if input_choice == "1":
                    # Excel upload option
                    excel_path = input("\nEnter Excel file path: ").strip()
                    if excel_path:
                        sentiment_agent.add_sentiment_column()
                        review_ids = sentiment_agent.add_reviews_from_excel(excel_path)
                        
                        if review_ids:
                            print(f"\n✅ Added {len(review_ids)} reviews")
                            print("🔄 Running sentiment analysis...")
                            for review_id in review_ids:
                                sentiment_agent.process_single_review(review_id)
                        else:
                            print("❌ Failed to add reviews from Excel")
                    
                elif input_choice == "2":
                    # Manual input option
                    print("\n✍️ Enter Review Details:")
                    
                    comment = input("Review comment: ").strip()
                    if not comment:
                        print("❌ Comment is required!")
                        continue
                    
                    rating = input("Overall rating (1-10): ").strip()
                    country = input("Country (optional): ").strip() or None
                    aircraft = input("Aircraft type (optional): ").strip() or None
                    traveller_type = input("Type of traveller (optional): ").strip() or None
                    origin = input("Origin (optional): ").strip() or None
                    destination = input("Destination (optional): ").strip() or None
                    
                    # Add review to database
                    sentiment_agent.add_sentiment_column()
                    review_id = sentiment_agent.add_new_review(
                        comment, rating, country, aircraft, traveller_type, origin, destination
                    )
                    
                    if review_id:
                        print(f"\n✅ Review added with ID: {review_id}")
                        print("🔄 Running sentiment analysis...")
                        sentiment_agent.process_single_review(review_id)
                    else:
                        print("❌ Failed to add review")
                        
                else:
                    print("❌ Invalid choice")
                    
            elif sub_choice == "b":
                print("\n📊 Running sentiment analysis on existing reviews...")
                sentiment_agent.add_sentiment_column()
                sentiment_agent.process_reviews()
            else:
                print("❌ Invalid choice")
            
        elif choice == "2":
            print("\n🤔 Ask me anything about the Ryanair reviews!")
            print("Examples:")
            print("- How many customers from the United States were happy?")
            print("- What's the average rating by country?")
            print("- Which aircraft has the most complaints?")
            
            question = input("\nYour question: ").strip()
            if question:
                query_agent.answer_question(question)
            
        elif choice == "3":
            print("👋 Goodbye!")
            break
            
        else:
            print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main()