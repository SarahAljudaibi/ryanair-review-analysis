import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import os

def get_sqlite_engine():
    """Create SQLite engine for local/cloud deployment"""
    db_path = os.path.join(os.path.dirname(__file__), 'ryanair_reviews.db')
    return create_engine(f'sqlite:///{db_path}')

def setup_sqlite_db():
    """Convert CSV to SQLite database"""
    engine = get_sqlite_engine()
    
    # Read CSV and create database
    df = pd.read_csv('ryanair_reviews.csv')
    df.to_sql('ryanair_reviews', engine, if_exists='replace', index=False)
    
    print(f"SQLite database created with {len(df)} reviews")
    return engine

if __name__ == "__main__":
    setup_sqlite_db()