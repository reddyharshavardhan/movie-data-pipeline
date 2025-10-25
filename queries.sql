-- 1. Which movie has the highest average rating?
WITH movie_avg_ratings AS (
    SELECT 
        m.movie_id,
        m.title,
        AVG(r.rating) as avg_rating,
        COUNT(r.rating) as rating_count
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
    HAVING COUNT(r.rating) >= 10  -- Filter movies with at least 10 ratings
)
SELECT 
    title,
    ROUND(avg_rating, 2) as average_rating,
    rating_count
FROM movie_avg_ratings
ORDER BY avg_rating DESC
LIMIT 1;

-- 2. What are the top 5 movie genres that have the highest average rating?
WITH genre_ratings AS (
    SELECT 
        g.genre_name,
        AVG(r.rating) as avg_rating,
        COUNT(DISTINCT m.movie_id) as movie_count,
        COUNT(r.rating) as total_ratings
    FROM genres g
    JOIN movie_genres mg ON g.genre_id = mg.genre_id
    JOIN movies m ON mg.movie_id = m.movie_id
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY g.genre_name
    HAVING COUNT(r.rating) >= 100  -- Filter genres with sufficient ratings
)
SELECT 
    genre_name,
    ROUND(avg_rating, 2) as average_rating,
    movie_count,
    total_ratings
FROM genre_ratings
ORDER BY avg_rating DESC
LIMIT 5;

-- 3. Who is the director with the most movies in this dataset?
SELECT 
    director,
    COUNT(*) as movie_count
FROM movies
WHERE director IS NOT NULL 
    AND director != ''
    AND director != 'N/A'
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4. What is the average rating of movies released each year?
WITH yearly_ratings AS (
    SELECT 
        m.release_year,
        AVG(r.rating) as avg_rating,
        COUNT(DISTINCT m.movie_id) as movie_count,
        COUNT(r.rating) as total_ratings
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    WHERE m.release_year IS NOT NULL
    GROUP BY m.release_year
)
SELECT 
    release_year,
    ROUND(avg_rating, 2) as average_rating,
    movie_count,
    total_ratings
FROM yearly_ratings
ORDER BY release_year DESC;

-- Bonus queries
-- 5. Top 10 highest rated movies with their genres
WITH movie_ratings AS (
    SELECT 
        m.movie_id,
        m.title,
        m.release_year,
        m.director,
        AVG(r.rating) as avg_rating,
        COUNT(r.rating) as rating_count
    FROM movies m
    JOIN ratings r ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title, m.release_year, m.director
    HAVING COUNT(r.rating) >= 20
),
movie_genre_list AS (
    SELECT 
        mg.movie_id,
        GROUP_CONCAT(g.genre_name, ', ') as genres
    FROM movie_genres mg
    JOIN genres g ON mg.genre_id = g.genre_id
    GROUP BY mg.movie_id
)
SELECT 
    mr.title,
    mr.release_year,
    mr.director,
    ROUND(mr.avg_rating, 2) as average_rating,
    mr.rating_count,
    COALESCE(mgl.genres, 'N/A') as genres
FROM movie_ratings mr
LEFT JOIN movie_genre_list mgl ON mr.movie_id = mgl.movie_id
ORDER BY mr.avg_rating DESC
LIMIT 10;

-- 6. Rating distribution across decades
SELECT 
    decade,
    COUNT(DISTINCT m.movie_id) as movie_count,
    ROUND(AVG(r.rating), 2) as avg_rating,
    ROUND(MIN(r.rating), 2) as min_rating,
    ROUND(MAX(r.rating), 2) as max_rating,
    COUNT(r.rating) as total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE decade IS NOT NULL
GROUP BY decade
ORDER BY decade DESC;