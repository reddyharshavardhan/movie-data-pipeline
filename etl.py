import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine, text
import time
import os
from datetime import datetime
import re
import logging
from typing import Dict, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MovieETLPipeline:
    def __init__(self):
        # Database configuration
        self.db_type = os.getenv('DB_TYPE', 'sqlite')
        self.db_path = os.getenv('DB_PATH', 'movie_db.sqlite')
        
        # OMDb API configuration
        self.omdb_api_key = os.getenv('OMDB_API_KEY', 'YOUR_API_KEY_HERE')
        self.omdb_base_url = 'http://www.omdbapi.com/'
        
        # Create database engine
        self.engine = None
        
    def _create_engine(self):
        """Create SQLAlchemy engine for SQLite"""
        return create_engine(f'sqlite:///{self.db_path}')
    
    def setup_database(self):
        """Create database and run schema"""
        try:
            # Create engine
            self.engine = self._create_engine()
            
            # Run schema.sql
            with open('schema.sql', 'r') as f:
                schema_sql = f.read()
            
            # Execute schema
            with self.engine.connect() as conn:
                # Split and execute each statement separately for SQLite
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                for statement in statements:
                    conn.execute(text(statement))
                conn.commit()
            
            logger.info("Database schema created successfully")
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            raise
    
    def extract_csv_data(self):
        """Extract data from MovieLens CSV files"""
        try:
            # Read movies.csv
            movies_df = pd.read_csv('ml-latest-small/movies.csv')
            logger.info(f"Loaded {len(movies_df)} movies from CSV")
            
            # Read ratings.csv
            ratings_df = pd.read_csv('ml-latest-small/ratings.csv')
            logger.info(f"Loaded {len(ratings_df)} ratings from CSV")
            
            return movies_df, ratings_df
        except Exception as e:
            logger.error(f"Error reading CSV files: {e}")
            raise
    
    def extract_year_from_title(self, title: str) -> Optional[int]:
        """Extract year from movie title"""
        # Fixed regex pattern - looking for (YYYY) at the end of title
        match = re.search(r'$(\d{4})$$', title)
        if match:
            return int(match.group(1))
        return None
    
    def clean_title(self, title: str) -> str:
        """Remove year from movie title"""
        # Fixed regex pattern - remove (YYYY) from end of title
        return re.sub(r'\s*$\d{4}$$', '', title).strip()
    
    def get_omdb_data(self, title: str, year: Optional[int] = None) -> Dict:
        """Fetch movie data from OMDb API"""
        params = {
            'apikey': self.omdb_api_key,
            't': title,
            'type': 'movie'
        }
        
        if year:
            params['y'] = year
        
        try:
            response = requests.get(self.omdb_base_url, params=params, timeout=5)
            data = response.json()
            
            if data.get('Response') == 'True':
                return data
            else:
                logger.debug(f"Movie not found in OMDb: {title}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {title}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error for {title}: {e}")
            return {}
    
    def transform_data(self, movies_df: pd.DataFrame, ratings_df: pd.DataFrame) -> tuple:
        """Transform and enrich movie data"""
        # Extract year and clean titles
        movies_df['release_year'] = movies_df['title'].apply(self.extract_year_from_title)
        movies_df['clean_title'] = movies_df['title'].apply(self.clean_title)
        
        # Add decade column
        movies_df['decade'] = movies_df['release_year'].apply(
            lambda x: (x // 10) * 10 if pd.notna(x) else None
        )
        
        # Initialize columns for API data
        movies_df['imdb_id'] = None
        movies_df['director'] = None
        movies_df['plot'] = None
        movies_df['box_office'] = None
        movies_df['runtime'] = None
        movies_df['imdb_rating'] = None
        
        # Check if API key is configured
        if self.omdb_api_key == 'YOUR_API_KEY_HERE':
            logger.warning("OMDb API key not configured. Skipping API enrichment.")
            logger.warning("Get your free key from: http://www.omdbapi.com/apikey.aspx")
        else:
            # Fetch data from OMDb API (limit for demo)
            logger.info("Fetching data from OMDb API...")
            movies_to_process = min(50, len(movies_df))  # Limit to 50 for demo
            
            for idx, row in movies_df[:movies_to_process].iterrows():
                if idx % 10 == 0:
                    logger.info(f"Processing movie {idx + 1}/{movies_to_process}")
                
                api_data = self.get_omdb_data(row['clean_title'], row['release_year'])
                
                if api_data:
                    movies_df.at[idx, 'imdb_id'] = api_data.get('imdbID')
                    movies_df.at[idx, 'director'] = api_data.get('Director')
                    movies_df.at[idx, 'plot'] = api_data.get('Plot')
                    movies_df.at[idx, 'box_office'] = api_data.get('BoxOffice')
                    movies_df.at[idx, 'runtime'] = api_data.get('Runtime')
                    
                    # Convert IMDB rating to float
                    imdb_rating = api_data.get('imdbRating')
                    if imdb_rating and imdb_rating != 'N/A':
                        try:
                            movies_df.at[idx, 'imdb_rating'] = float(imdb_rating)
                        except:
                            pass
                
                # Rate limiting
                time.sleep(0.1)  # Be nice to the API
        
        # Prepare genres data
        genres_set = set()
        movie_genres_list = []
        
        for idx, row in movies_df.iterrows():
            if pd.notna(row['genres']):
                genres = row['genres'].split('|')
                for genre in genres:
                    genres_set.add(genre.strip())
                    movie_genres_list.append({
                        'movie_id': row['movieId'],
                        'genre_name': genre.strip()
                    })
        
        genres_df = pd.DataFrame({'genre_name': list(genres_set)})
        movie_genres_df = pd.DataFrame(movie_genres_list)
        
        return movies_df, ratings_df, genres_df, movie_genres_df
    
    def load_data(self, movies_df, ratings_df, genres_df, movie_genres_df):
        """Load data into SQLite database"""
        try:
            # Load genres first
            genres_df.to_sql('genres', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(genres_df)} genres")
            
            # Get genre IDs
            genre_mapping = pd.read_sql("SELECT genre_id, genre_name FROM genres", self.engine)
            
            # Prepare movies data
            movies_to_load = movies_df[[
                'movieId', 'title', 'release_year', 'imdb_id', 
                'director', 'plot', 'box_office', 'runtime', 
                'imdb_rating', 'decade'
            ]].copy()
            movies_to_load.columns = [
                'movie_id', 'title', 'release_year', 'imdb_id',
                'director', 'plot', 'box_office', 'runtime',
                'imdb_rating', 'decade'
            ]
            
            # Load movies
            movies_to_load.to_sql('movies', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(movies_to_load)} movies")
            
            # Prepare movie_genres data with proper IDs
            movie_genres_final = movie_genres_df.merge(
                genre_mapping, on='genre_name', how='left'
            )[['movie_id', 'genre_id']]
            
            # Load movie_genres
            movie_genres_final.to_sql('movie_genres', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(movie_genres_final)} movie-genre relationships")
            
            # Prepare ratings data
            ratings_to_load = ratings_df[['userId', 'movieId', 'rating', 'timestamp']].copy()
            ratings_to_load.columns = ['user_id', 'movie_id', 'rating', 'timestamp']
            
            # Load ratings
            ratings_to_load.to_sql('ratings', self.engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(ratings_to_load)} ratings")
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def run(self):
        """Run the complete ETL pipeline"""
        logger.info("Starting ETL pipeline...")
        
        # Setup database
        logger.info("Setting up database...")
        self.setup_database()
        
        # Extract
        logger.info("Extracting data from CSV files...")
        movies_df, ratings_df = self.extract_csv_data()
        
        # Transform
        logger.info("Transforming and enriching data...")
        movies_df, ratings_df, genres_df, movie_genres_df = self.transform_data(
            movies_df, ratings_df
        )
        
        # Load
        logger.info("Loading data into database...")
        self.load_data(movies_df, ratings_df, genres_df, movie_genres_df)
        
        logger.info("ETL pipeline completed successfully!")

def main():
    # Create .env file if it doesn't exist
    if not os.path.exists('.env'):
        with open('.env', 'w') as f:
            f.write("""# Database Configuration
DB_TYPE=sqlite
DB_PATH=movie_db.sqlite

# OMDb API Key
OMDB_API_KEY=YOUR_API_KEY_HERE
""")
        logger.info("Created .env file. Please update OMDB_API_KEY with your actual key.")
    
    # Run pipeline
    pipeline = MovieETLPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()