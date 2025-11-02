import pandas as pd
import psycopg2
from sqlalchemy import create_engine, inspect

# ==============================
# Database Configuration
# ==============================
DB_CONFIG = {
    'host': 'localhost',
    'database': 'ryanaircs',
    'user': 'postgres',
    'password': 'admin',  # change if needed
    'port': '5432'
}


# ==============================
# Create Table
# ==============================
def create_table():
    """Creates the ryanair_reviews table in PostgreSQL"""
    create_table_sql = """
    DROP TABLE IF EXISTS ryanair_reviews;
    CREATE TABLE ryanair_reviews (
        id SERIAL PRIMARY KEY,
        date_published DATE,
        overall_rating INTEGER,
        passenger_country VARCHAR(100),
        trip_verified VARCHAR(20),
        comment_title TEXT,
        comment TEXT,
        aircraft VARCHAR(100),
        type_of_traveller VARCHAR(100),
        seat_type VARCHAR(100),
        origin VARCHAR(100),
        destination VARCHAR(100),
        date_flown VARCHAR(50),
        seat_comfort DECIMAL(3,1),
        cabin_staff_service DECIMAL(3,1),
        food_beverages DECIMAL(3,1),
        ground_service DECIMAL(3,1),
        value_for_money DECIMAL(3,1),
        recommended VARCHAR(10),
        inflight_entertainment DECIMAL(3,1),
        wifi_connectivity DECIMAL(3,1)
    );
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ Table 'ryanair_reviews' created successfully!")
    except Exception as e:
        print(f"❌ Error creating table: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


# ==============================
# Import CSV Data
# ==============================
def import_csv_data(csv_file_path):
    """Imports cleaned CSV data into PostgreSQL"""
    try:
        # Step 1: Load CSV
        df = pd.read_csv(csv_file_path, keep_default_na=True)
        print(f"📄 Loaded CSV with {len(df)} rows and {len(df.columns)} columns")

        # Step 2: Clean column names
        df.columns = (
            df.columns
            .str.strip()
            .str.replace(' ', '_')
            .str.replace('&', '')
            .str.replace('__', '_')  # ✅ fix double underscores
            .str.lower()
        )

        # Step 3: Drop unwanted columns
        for col in ['unnamed:_0', 'id']:
            if col in df.columns:
                df = df.drop(col, axis=1)

        # Step 4: Convert types
        if 'date_published' in df.columns:
            df['date_published'] = pd.to_datetime(df['date_published'], errors='coerce')

        numeric_cols = [
            'overall_rating', 'seat_comfort', 'cabin_staff_service',
            'food_beverages', 'ground_service', 'value_for_money',
            'inflight_entertainment', 'wifi_connectivity'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Step 5: Connect to PostgreSQL
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Step 6: Validate columns
        inspector = inspect(engine)
        db_cols = [col['name'] for col in inspector.get_columns('ryanair_reviews')]
        csv_cols = df.columns.tolist()
        missing_in_db = [c for c in csv_cols if c not in db_cols]
        missing_in_csv = [c for c in db_cols if c not in csv_cols and c != 'id']

        print("\n🔍 Column Alignment Check:")
        print("Columns in CSV:", csv_cols)
        print("Columns in DB:", db_cols)
        if missing_in_db:
            print("⚠️ CSV columns not found in DB:", missing_in_db)
        if missing_in_csv:
            print("⚠️ DB columns missing in CSV:", missing_in_csv)
        print()

        # Step 7: Insert data
        df.to_sql('ryanair_reviews', engine, if_exists='append', index=False)
        print(f"✅ Successfully imported {len(df)} records into 'ryanair_reviews'!")

    except Exception as e:
        print(f"❌ Error importing data: {e}")


# ==============================
# Run Script
# ==============================
if __name__ == "__main__":
    create_table()
    csv_file_path = r"c:\Users\SarahAljudaibi\Downloads\KDDRAG\ryanair_reviews.csv"
    import_csv_data(csv_file_path)
