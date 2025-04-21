import subprocess
import os
import sys
import json
import argparse
from datetime import datetime

def run_crawler(batch_size=20, start_index=0, max_movies=0, display_quotes=True):
    """
    Runs the Scrapy crawler with batch processing.
    
    Args:
        batch_size (int): Number of movies to process in this batch
        start_index (int): Starting index in the movie list
        max_movies (int): Maximum number of movies to process (0 for no limit)
        display_quotes (bool): Whether to display quotes in the terminal
    """
    try:
        # Create crawler directory if it doesn't exist
        os.makedirs("crawler", exist_ok=True)
        
        # Create jobs directory for resuming
        jobs_dir = os.path.join("crawler", "jobs")
        os.makedirs(jobs_dir, exist_ok=True)
        
        # Determine output path
        output_filename = "movies.json"
        output_path = os.path.join("crawler", output_filename)
            
        # Build command with arguments
        cmd = [
            "poetry", "run", "python", "crawler/run.py",
            "--batch-size", str(batch_size),
            "--start-index", str(start_index),
        ]
        
        if max_movies > 0:
            cmd.extend(["--max-movies", str(max_movies)])
            
        # Always append to movies.json
        cmd.append("--append")
            
        # Run the crawler using the run.py script with Poetry
        print(f"Starting crawler with batch_size={batch_size}, start_index={start_index}, max_movies={max_movies}")
        print(f"Output will be saved to: {output_path}")
        
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        
        print(f"Scrapy command output: {result.stdout}")
        
        if result.stderr:
            print(f"Scrapy error output: {result.stderr}")
        
        # Check if the output file was created
        if os.path.exists(output_path):
            print(f"Output file created at: {output_path}")
            
            # Display quotes if requested
            if display_quotes:
                with open(output_path, "r") as f:
                    content = f.read()
                    if content.strip():
                        print(f"Successfully scraped data")
                        
                        # Print out the quote list
                        print("\n===== MOVIE QUOTES =====\n")
                        try:
                            movies = json.loads(content)
                            for movie in movies:
                                print(f"Movie: {movie['title']}")
                                print(f"URL: {movie['url']}")
                                print(f"Quotes ({len(movie['quotes'])}):")
                                
                                for i, quote in enumerate(movie['quotes'], 1):
                                    print(f"  {i}. {quote['text'][:100]}..." if len(quote['text']) > 100 else f"  {i}. {quote['text']}")
                                
                                print("\n" + "-" * 50 + "\n")
                        except json.JSONDecodeError:
                            print("Error: Could not parse JSON data")
                    else:
                        print("Warning: Output file is empty")
            
            # Calculate next batch information
            next_start = start_index + batch_size
            print(f"\nTo process the next batch, run with: --start-index {next_start}")
        else:
            print(f"Warning: Output file not found at {output_path}")
            
    except subprocess.CalledProcessError as e:
        print(f"Error running Scrapy: {e.stderr}")
        return 1
    except FileNotFoundError:
        print("Scrapy command not found. Ensure Scrapy is installed.")
        return 1
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return 1
    
    return 0

def main():
    """Main entry point with command line argument parsing."""
    parser = argparse.ArgumentParser(description='Run the movie quotes crawler with batch processing')
    parser.add_argument('--batch-size', type=int, default=20,
                        help='Number of movies to process in this batch (default: 20)')
    parser.add_argument('--start-index', type=int, default=0,
                        help='Starting index in the movie list (default: 0)')
    parser.add_argument('--max-movies', type=int, default=0,
                        help='Maximum number of movies to process, 0 for no limit (default: 0)')
    # --append flag is no longer needed but kept for backward compatibility
    parser.add_argument('--append', action='store_true',
                        help='[Deprecated] Always appends to movies.json')
    parser.add_argument('--no-display', action='store_true',
                        help='Do not display quotes in the terminal')
    
    args = parser.parse_args()
    
    return run_crawler(
        batch_size=args.batch_size,
        start_index=args.start_index,
        max_movies=args.max_movies,
        display_quotes=not args.no_display
    )

if __name__ == "__main__":
    sys.exit(main())