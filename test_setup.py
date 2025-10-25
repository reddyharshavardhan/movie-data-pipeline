import pandas as pd
import sqlite3
from dotenv import load_dotenv
import os
import requests

# Load environment variables
load_dotenv()

def test_csv_files():
    """Test if CSV files are accessible"""
    try:
        movies_df = pd.read_csv('ml-latest-small/movies.csv')
        ratings_df = pd.read_csv('ml-latest-small/ratings.csv')
        print(f"✓ Movies CSV: {len(movies_df)} records")
        print(f"✓ Ratings CSV: {len(ratings_df)} records")
        print("\nSample movie data:")
        print(movies_df.head(3))
        return True
    except Exception as e:
        print(f"✗ Error reading CSV files: {e}")
        return False

def test_database_connection():
    """Test SQLite database creation"""
    try:
        db_path = os.getenv('DB_PATH', 'movie_db.sqlite')
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT sqlite_version();")
        version = cursor.fetchone()
        print(f"✓ SQLite database connection successful")
        print(f"  SQLite version: {version[0]}")
        conn.close()
        return True
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        return False

def test_api_key():
    """Test if API key is configured and working"""
    api_key = os.getenv('OMDB_API_KEY')
    if api_key and api_key != 'YOUR_API_KEY_HERE':
        # Test API key with a sample request
        try:
            response = requests.get(
                'http://www.omdbapi.com/',
                params={'apikey': api_key, 't': 'The Matrix', 'y': 1999}
            )
            data = response.json()
            if data.get('Response') == 'True':
                print("✓ OMDb API key configured and working")
                print(f"  Test query returned: {data.get('Title')} ({data.get('Year')})")
                return True
            else:
                print("✗ OMDb API key is invalid")
                return False
        except Exception as e:
            print(f"✗ Error testing API: {e}")
            return False
    else:
        print("✗ OMDb API key not configured")
        print("  Get your free key from: http://www.omdbapi.com/apikey.aspx")
        return False

if __name__ == "__main__":
    print("Testing Movie Data Pipeline Setup")
    print("=" * 50 + "\n")
    
    csv_ok = test_csv_files()
    print("\n" + "-" * 50 + "\n")
    
    db_ok = test_database_connection()
    print("\n" + "-" * 50 + "\n")
    
    api_ok = test_api_key()
    
    print("\n" + "=" * 50)
    if csv_ok and db_ok:
        print("✓ Basic setup is working!")
        if not api_ok:
            print("! Don't forget to add your OMDb API key to .env file")
    else:
        print("✗ Please fix the issues above before proceeding")