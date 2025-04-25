import json
import os
import glob
import sys
import re
import psycopg2
from psycopg2.extras import execute_values

def create_schema_and_tables(conn):
    """Create the necessary schema and tables if they don't exist."""
    with conn.cursor() as cur:
        # Create schema
        cur.execute("CREATE SCHEMA IF NOT EXISTS quotes;")
        
        # Create movies table with additional columns for year and movie_id
        cur.execute("""
        DROP TABLE quotes.movies CASCADE;
        CREATE TABLE quotes.movies (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            year INTEGER,
            movie_id INTEGER,
            url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create quotes table with foreign key reference to movies
        cur.execute("""
        CREATE TABLE IF NOT EXISTS quotes.quotes (
            id SERIAL PRIMARY KEY,
            movie_id INTEGER REFERENCES quotes.movies(id),
            quote_text TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create indexes for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movie_title ON quotes.movies(title);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movie_year ON quotes.movies(year);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_movie_movie_id ON quotes.movies(movie_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_quote_movie_id ON quotes.quotes(movie_id);")
        
        conn.commit()

def parse_movie_title(raw_title):
    """
    Parse a raw movie title in the format "title (year) movie_id"
    Returns a tuple of (title, year, movie_id)
    """
    # Regular expression to match the pattern: title (year) movie_id
    pattern = r"(.*)\s*\((\d{4})(?:/[ivxlcdm]+)?\)\s*(\d+)$"
    match = re.match(pattern, raw_title)
    
    if match:
        title = match.group(1).strip()
        year = int(match.group(2))
        movie_id = int(match.group(3))
        return title, year, movie_id
    else:
        # If the pattern doesn't match, return the original title and None for year and movie_id
        return raw_title, None, None

def process_json_files(conn, json_dir_path):
    """Process all JSON files in the specified directory and insert data into the database."""
    # Get all JSON files in the directory
    json_files = glob.glob(os.path.join(json_dir_path, "*.json"))
    
    if not json_files:
        print(f"No JSON files found in {json_dir_path}")
        return
    
    with conn.cursor() as cur:
        # Process each JSON file
        for json_file in json_files:
            print(f"Processing file: {json_file}")
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Process each movie entry in the JSON file
                for movie_entry in data:
                    raw_title = movie_entry.get('title')
                    url = movie_entry.get('url')
                    quotes_data = movie_entry.get('quotes', [])
                    
                    if not raw_title:
                        print(f"Skipping entry without title in file {json_file}")
                        continue
                    
                    # Parse the title to extract title, year, and movie_id
                    title, year, movie_id = parse_movie_title(raw_title)
                    
                    # Insert or get movie ID
                    cur.execute(
                        "SELECT id FROM quotes.movies WHERE title = %s AND (year = %s OR year IS NULL) AND (movie_id = %s OR movie_id IS NULL)",
                        (title, year, movie_id)
                    )
                    result = cur.fetchone()
                    
                    if result:
                        db_movie_id = result[0]
                        print(f"Movie '{title}' already exists with ID {db_movie_id}")
                    else:
                        cur.execute(
                            "INSERT INTO quotes.movies (title, year, movie_id, url) VALUES (%s, %s, %s, %s) RETURNING id",
                            (title, year, movie_id, url)
                        )
                        db_movie_id = cur.fetchone()[0]
                        print(f"Inserted movie '{title}' ({year}, {movie_id}) with DB ID {db_movie_id}")
                    
                    # Insert quotes for this movie
                    if quotes_data:
                        quote_values = [(db_movie_id, quote.get('text')) for quote in quotes_data if quote.get('text')]
                        
                        if quote_values:
                            execute_values(
                                cur,
                                "INSERT INTO quotes.quotes (movie_id, quote_text) VALUES %s",
                                quote_values
                            )
                            print(f"Inserted {len(quote_values)} quotes for movie '{title}'")
                
                conn.commit()
                print(f"Successfully processed file: {json_file}")
                
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON format in file {json_file}")
                conn.rollback()
            except Exception as e:
                print(f"Error processing file {json_file}: {str(e)}")
                conn.rollback()

def display_info(conn):
    """Display information about the database tables."""
    with conn.cursor() as cur:
        # Get total counts
        cur.execute("SELECT COUNT(*) FROM quotes.movies")
        total_movies = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM quotes.quotes")
        total_quotes = cur.fetchone()[0]
        
        print("\n=== Database Information ===")
        print(f"Total Movies: {total_movies}")
        print(f"Total Quotes: {total_quotes}")
        
        # Get movie counts grouped by first letter
        cur.execute("""
        SELECT 
            UPPER(LEFT(title, 1)) AS first_letter,
            COUNT(*) AS count
        FROM quotes.movies
        GROUP BY first_letter
        ORDER BY first_letter
        """)
        
        letter_counts = cur.fetchall()
        
        print("\n=== Movies by First Letter ===")
        for letter, count in letter_counts:
            print(f"{letter} = {count}")
        
        # Get average quotes per movie
        cur.execute("""
        SELECT AVG(quote_count) FROM (
            SELECT movie_id, COUNT(*) as quote_count
            FROM quotes.quotes
            GROUP BY movie_id
        ) AS subquery
        """)
        
        avg_quotes = cur.fetchone()[0]
        if avg_quotes:
            print(f"\nAverage Quotes per Movie: {avg_quotes:.2f}")

def main():
    """Main function to run the ETL process."""
    # Database connection parameters - replace with your actual values
    db_params = {
        'dbname': 'pg_malone',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': '15432'
    }
    
    # Directory containing JSON files - replace with your actual path
    json_dir_path = './crawler/results/'
    
    try:
        # Connect to the database
        print(f"Connecting to PostgreSQL database: {db_params['dbname']}")
        conn = psycopg2.connect(**db_params)
        
        # Check if the info command was provided
        if len(sys.argv) > 1 and sys.argv[1].lower() == 'info':
            display_info(conn)
        else:
            # Create schema and tables
            print("Creating schema and tables if they don't exist...")
            create_schema_and_tables(conn)
            
            # Process JSON files and insert data
            print(f"Processing JSON files from: {json_dir_path}")
            process_json_files(conn, json_dir_path)
            
            print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()
