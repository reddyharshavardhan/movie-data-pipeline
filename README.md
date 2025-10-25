# Movie Data Pipeline

A data engineering project that builds an ETL pipeline to ingest, transform, and analyze movie data from MovieLens dataset and OMDb API.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)
- [API Usage](#api-usage)
- [Analytical Queries](#analytical-queries)
- [Design Decisions](#design-decisions)
- [Challenges & Solutions](#challenges--solutions)
- [Future Improvements](#future-improvements)

## Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline that:

- Extracts movie and rating data from MovieLens CSV files
- Enriches movie information using the OMDb API
- Transforms and cleans the data
- Loads it into a SQLite database
- Provides analytical queries to derive insights

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  MovieLens      │     │    OMDb API     │     │     SQLite      │
│   CSV Files     │────▶│  (Enrichment)   │────▶│    Database     │
└─────────────────┘     └─────────────────┘     └─────────────────┘
         │                       │                        │
         └───────────────────────┴────────────────────────┘
                                 │
                         ┌───────▼────────┐
                         │  ETL Pipeline  │
                         │    (etl.py)    │
                         └────────────────┘
```

## Prerequisites

- Python 3.8 or higher
- pip (Python package manager)
- Internet connection (for API calls)
- 500MB free disk space

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/movie-data-pipeline.git
cd movie-data-pipeline
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Download MovieLens dataset

```bash
# Create directory
mkdir ml-latest-small

# Option 1: Using curl (macOS/Linux)
curl -O https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
unzip ml-latest-small.zip

# Option 2: Using PowerShell (Windows)
Invoke-WebRequest -Uri "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip" -OutFile "ml-latest-small.zip"
Expand-Archive -Path "ml-latest-small.zip" -DestinationPath "."

# Option 3: Manual download
# Visit: https://grouplens.org/datasets/movielens/latest/
# Download "ml-latest-small.zip" and extract to project directory
```

## Configuration

### Get OMDb API Key

1. Visit [OMDb API](http://www.omdbapi.com/apikey.aspx)
2. Sign up for a FREE API key (1,000 daily limit)
3. Check your email for the API key

### Configure environment variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` file and add your API key:

```
OMDB_API_KEY=your_actual_api_key_here
```

## Running the Pipeline

### Step 1: Test Your Setup

```bash
python test_setup.py
```

Expected output:

```
Testing Movie Data Pipeline Setup
==================================================

✓ Movies CSV: 9742 records
✓ Ratings CSV: 100836 records
✓ SQLite database connection successful
✓ OMDb API key configured and working
```

### Step 2: Run the ETL Pipeline

```bash
python etl.py
```

This will:

- Create the database schema
- Extract data from CSV files
- Fetch additional movie details from OMDb API (limited to 50 movies in demo)
- Transform and clean the data
- Load everything into SQLite database

**Expected runtime:** 2-5 minutes (depending on API response time)

### Step 3: Run Analytical Queries

```bash
python run_queries.py
```

This will execute all analytical queries and display results.

**Alternative: Run Individual SQL Queries**

```bash
# Using SQLite CLI
sqlite3 movie_db.sqlite < queries.sql

# Or interactively
sqlite3 movie_db.sqlite
sqlite> .read queries.sql
```

## Project Structure

```
movie-data-pipeline/
├── ml-latest-small/          # MovieLens dataset (after download)
│   ├── movies.csv
│   └── ratings.csv
├── etl.py                    # Main ETL pipeline script
├── schema.sql                # Database schema definition
├── queries.sql               # Analytical SQL queries
├── run_queries.py            # Script to execute queries
├── test_setup.py             # Environment validation script
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables (create from .env.example)
├── .gitignore                # Git ignore file
└── README.md                 # This file
```

## Database Schema

The database consists of 4 tables:

### movies - Core movie information

- `movie_id` (PK)
- `title`, `release_year`, `director`, `plot`, etc.

### genres - Genre lookup table

- `genre_id` (PK)
- `genre_name`

### movie_genres - Movie-genre relationships (junction table)

- `movie_id` (FK)
- `genre_id` (FK)

### ratings - User ratings

- `rating_id` (PK)
- `user_id`, `movie_id` (FK), `rating`, `timestamp`

## API Usage

The pipeline fetches additional movie details from OMDb API:

- Director
- Plot summary
- Box office revenue
- Runtime
- IMDB rating

**Note:** Free tier is limited to 1,000 requests/day. The demo processes only 50 movies.

## Analytical Queries

1. **Which movie has the highest average rating?**
   - Filters movies with at least 10 ratings for statistical significance

2. **Top 5 movie genres by average rating**
   - Aggregates ratings by genre
   - Requires minimum 100 ratings per genre

3. **Director with the most movies**
   - Based on available director data from API

4. **Average rating by release year**
   - Shows rating trends over time

## Design Decisions

### Why SQLite?

- No server setup required
- Perfect for development and small-medium datasets
- Easy to distribute and version control
- Supports all required SQL features

### Why Normalized Schema?

- Eliminates data redundancy (genres stored separately)
- Maintains data integrity
- Allows flexible querying
- Follows database best practices

### API Rate Limiting Strategy

- 0.1 second delay between requests
- Graceful handling of failed requests
- Limited demo to 50 movies to stay within free tier

## Challenges & Solutions

### Movie Title Matching

- **Challenge:** MovieLens titles include year, OMDb expects clean titles
- **Solution:** Regex to extract year and clean title

### Missing Data Handling

- **Challenge:** Not all movies found in OMDb
- **Solution:** Graceful fallback with NULL values

### Genre Normalization

- **Challenge:** Genres stored as pipe-separated string
- **Solution:** Parse and normalize into separate table

### API Rate Limits

- **Challenge:** Free tier limitations
- **Solution:** Configurable batch size, delays between requests

## Future Improvements

### Short-term

- [ ] Add data validation layer
- [ ] Implement retry logic with exponential backoff
- [ ] Add progress bars for better UX
- [ ] Cache API responses to avoid duplicate calls

### Long-term

- [ ] Migrate to PostgreSQL for production
- [ ] Implement Apache Airflow for scheduling
- [ ] Add data quality checks with Great Expectations
- [ ] Implement incremental loading
- [ ] Add unit and integration tests
- [ ] Dockerize the application
- [ ] Add real-time streaming with Kafka

## .env.example File

Create a `.env.example` file with the following content:

```bash
# Database Configuration
DB_TYPE=sqlite
DB_PATH=movie_db.sqlite

# OMDb API Key (get from http://www.omdbapi.com/apikey.aspx)
OMDB_API_KEY=YOUR_API_KEY_HERE
```

## Contributing

Feel free to open issues or submit pull requests with improvements.

## License

This project is part of a technical assessment and is for educational purposes.

## Acknowledgments

- [GroupLens](https://grouplens.org/) for the MovieLens dataset
- [OMDb API](http://www.omdbapi.com/) for movie metadata
- SQLite for the database engine
