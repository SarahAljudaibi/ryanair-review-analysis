import psycopg2
from datetime import datetime

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ryanaircs',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def create_error_log_table():
    """Create table to log query errors and attempts"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS query_error_log (
        id SERIAL PRIMARY KEY,
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
        final_status VARCHAR(20) DEFAULT 'FAILED',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS query_success_log (
        id SERIAL PRIMARY KEY,
        user_question TEXT NOT NULL,
        sql_query TEXT NOT NULL,
        answer_text TEXT NOT NULL,
        execution_time_ms INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ Tables 'query_error_log' and 'query_success_log' created successfully!")
        
    except Exception as e:
        print(f"❌ Error creating error log table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def log_query_error(user_question, attempts):
    """Log failed query attempts to database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
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
        cursor.execute("""
            INSERT INTO query_error_log (
                user_question, original_sql,
                attempt_1_sql, attempt_1_error,
                attempt_2_sql, attempt_2_error,
                attempt_3_sql, attempt_3_error,
                attempt_4_sql, attempt_4_error,
                attempt_5_sql, attempt_5_error,
                final_status
            ) VALUES (
                %(user_question)s, %(original_sql)s,
                %(attempt_1_sql)s, %(attempt_1_error)s,
                %(attempt_2_sql)s, %(attempt_2_error)s,
                %(attempt_3_sql)s, %(attempt_3_error)s,
                %(attempt_4_sql)s, %(attempt_4_error)s,
                %(attempt_5_sql)s, %(attempt_5_error)s,
                'FAILED'
            )
        """, insert_data)
        
        conn.commit()
        print("📝 Error logged to database for developer review")
        
    except Exception as e:
        print(f"❌ Error logging to database: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def log_successful_query(user_question, sql_query, answer_text, execution_time_ms=None):
    """Log successful query and answer to database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO query_success_log (user_question, sql_query, answer_text, execution_time_ms)
            VALUES (%s, %s, %s, %s)
        """, (user_question, sql_query, answer_text, execution_time_ms))
        
        conn.commit()
        print("✅ Successful query logged to database")
        
    except Exception as e:
        print(f"⚠️ Could not log successful query: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    create_error_log_table()