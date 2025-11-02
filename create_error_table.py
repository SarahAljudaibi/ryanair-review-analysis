from datetime import datetime
from sqlite_config import get_sqlite_engine
from sqlalchemy import text

def create_error_log_table():
    """Create table to log query errors and attempts"""
    try:
        engine = get_sqlite_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_question TEXT NOT NULL,
                    original_sql TEXT,
                    attempt_1_sql TEXT,
                    attempt_1_error TEXT,
                    attempt_2_sql TEXT,
                    attempt_2_error TEXT,
                    attempt_3_sql TEXT,
                    attempt_3_error TEXT,
                    attempt_4_sql TEXT,
                    attempt_4_error TEXT,
                    attempt_5_sql TEXT,
                    attempt_5_error TEXT,
                    final_status TEXT DEFAULT 'FAILED',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS query_success_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_question TEXT NOT NULL,
                    sql_query TEXT NOT NULL,
                    answer_text TEXT NOT NULL,
                    execution_time_ms INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
        print("Tables 'query_error_log' and 'query_success_log' created successfully!")
        
    except Exception as e:
        print(f"Error creating error log table: {e}")

def log_query_error(user_question, attempts):
    """Log failed query attempts to database"""
    try:
        engine = get_sqlite_engine()
        with engine.connect() as conn:
            # Prepare data for insertion
            insert_data = {
                'user_question': user_question,
                'original_sql': attempts[0]['sql'] if attempts else None,
            }
            
            # Add attempt data
            for i in range(5):
                if i < len(attempts):
                    insert_data[f'attempt_{i+1}_sql'] = attempts[i]['sql']
                    insert_data[f'attempt_{i+1}_error'] = attempts[i]['error']
                else:
                    insert_data[f'attempt_{i+1}_sql'] = None
                    insert_data[f'attempt_{i+1}_error'] = None
            
            # Insert into database
            conn.execute(text("""
                INSERT INTO query_error_log (
                    user_question, original_sql,
                    attempt_1_sql, attempt_1_error,
                    attempt_2_sql, attempt_2_error,
                    attempt_3_sql, attempt_3_error,
                    attempt_4_sql, attempt_4_error,
                    attempt_5_sql, attempt_5_error,
                    final_status
                ) VALUES (
                    :user_question, :original_sql,
                    :attempt_1_sql, :attempt_1_error,
                    :attempt_2_sql, :attempt_2_error,
                    :attempt_3_sql, :attempt_3_error,
                    :attempt_4_sql, :attempt_4_error,
                    :attempt_5_sql, :attempt_5_error,
                    'FAILED'
                )
            """), insert_data)
            
            conn.commit()
        print("Error logged to database for developer review")
        
    except Exception as e:
        print(f"Error logging to database: {e}")

def log_successful_query(user_question, sql_query, answer_text, execution_time_ms=None):
    """Log successful query and answer to database"""
    try:
        engine = get_sqlite_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO query_success_log (user_question, sql_query, answer_text, execution_time_ms)
                VALUES (:user_question, :sql_query, :answer_text, :execution_time_ms)
            """), {
                'user_question': user_question,
                'sql_query': sql_query,
                'answer_text': answer_text,
                'execution_time_ms': execution_time_ms
            })
            
            conn.commit()
        print("Successful query logged to database")
        
    except Exception as e:
        print(f"Could not log successful query: {e}")

if __name__ == "__main__":
    create_error_log_table()