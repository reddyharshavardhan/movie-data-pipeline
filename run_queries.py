import sqlite3
import pandas as pd

conn = sqlite3.connect('movie_db.sqlite')

# Query 1: Movie with highest average rating
print("1. Movie with highest average rating:")
query1 = """
WITH movie_avg_ratings AS (
    SELECT m.movie_id, m.title, AVG(r.rating) as avg_rating, COUNT(r.rating) as rating_count
    FROM movies m JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
    HAVING COUNT(r.rating) >= 10
)
SELECT title, ROUND(avg_rating, 2) as average_rating, rating_count
FROM movie_avg_ratings
ORDER BY avg_rating DESC LIMIT 1;
"""
df1 = pd.read_sql_query(query1, conn)
print(df1)

# Query 2: Top 5 genres by average rating
print("\n2. Top 5 genres by average rating:")
query2 = """
WITH genre_ratings AS (
    SELECT g.genre_name, AVG(r.rating) as avg_rating, 
           COUNT(DISTINCT m.movie_id) as movie_count, COUNT(r.rating) as total_ratings
    FROM genres g
    JOIN movie_genres mg ON g.genre_id = mg.genre_id
    JOIN movies m ON mg.movie_id = m.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY g.genre_name
    HAVING COUNT(r.rating) >= 100
)
SELECT genre_name, ROUND(avg_rating, 2) as average_rating, movie_count, total_ratings
FROM genre_ratings
ORDER BY avg_rating DESC LIMIT 5;
"""
df2 = pd.read_sql_query(query2, conn)
print(df2)

# Query 3: Director with most movies
print("\n3. Director with most movies:")
query3 = """
SELECT director, COUNT(*) as movie_count
FROM movies
WHERE director IS NOT NULL AND director != '' AND director != 'N/A'
GROUP BY director
ORDER BY movie_count DESC LIMIT 1;
"""
df3 = pd.read_sql_query(query3, conn)
print(df3)

# Check movies with API data
print("\n4. Sample movies with OMDb API data:")
query4 = """
SELECT title, release_year, director, imdb_rating, 
       SUBSTR(plot, 1, 50) || '...' as plot_preview
FROM movies 
WHERE director IS NOT NULL 
LIMIT 5;
"""
df4 = pd.read_sql_query(query4, conn)
print(df4)

conn.close()