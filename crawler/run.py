from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os
import sys
import argparse
import json
from datetime import datetime

# Add the current directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from spiders.movie_quotes_spider import MovieQuotesSpider

def run_spider(batch_size=20, start_index=0, max_movies=0, append=True):
    """
    Run the movie quotes spider with batch processing.
    
    Args:
        batch_size (int): Number of movies to process in this batch
        start_index (int): Starting index in the movie list
        max_movies (int): Maximum number of movies to process (0 for no limit)
        append (bool): Whether to append to existing output file (default: True)
    """
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(os.path.abspath(__file__)), exist_ok=True)
    
    # Create jobs directory for resuming
    jobs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs")
    os.makedirs(jobs_dir, exist_ok=True)
    
    # Set up the settings
    settings = get_project_settings()
    
    # Set up the output file - always use movies.json
    output_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "movies.json")
    
    # Configure the settings to output to a JSON file
    settings.set('FEED_URI', f'file://{output_file}')
    settings.set('FEED_FORMAT', 'json')
    settings.set('FEED_EXPORT_ENCODING', 'utf-8')
    
    # Always set append mode
    settings.set('FEED_EXPORT_FLAGS', ['a'])
    
    # Configure job directory for resuming
    settings.set('JOBDIR', jobs_dir)
    
    print(f"Starting crawler with batch_size={batch_size}, start_index={start_index}, max_movies={max_movies}")
    print(f"Output will be saved to: {output_file}")
    
    # Run the crawler
    process = CrawlerProcess(settings)
    process.crawl(
        MovieQuotesSpider,
        batch_size=batch_size,
        start_index=start_index,
        max_movies=max_movies
    )
    process.start()
    
    print(f"Crawler finished. Output saved to: {output_file}")
    
    # Print next batch information
    next_start = start_index + batch_size
    print(f"\nTo process the next batch, run with: --start-index {next_start}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run the movie quotes crawler with batch processing')
    parser.add_argument('--batch-size', type=int, default=20,
                        help='Number of movies to process in this batch (default: 20)')
    parser.add_argument('--start-index', type=int, default=0,
                        help='Starting index in the movie list (default: 0)')
    parser.add_argument('--max-movies', type=int, default=0,
                        help='Maximum number of movies to process, 0 for no limit (default: 0)')
    # --append flag is kept for backward compatibility but is now ignored
    parser.add_argument('--append', action='store_true',
                        help='[Deprecated] Always appends to movies.json')
    
    args = parser.parse_args()
    
    run_spider(
        batch_size=args.batch_size,
        start_index=args.start_index,
        max_movies=args.max_movies
    )