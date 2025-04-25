import scrapy
import sys
import os
import time
import random
import json
import logging
from datetime import datetime

# Add the parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from items import MovieItem, QuoteItem

class MovieQuotesSpider(scrapy.Spider):
    name = "movie_quotes"
    allowed_domains = ["quotes.net"]
    start_urls = [
        "https://www.quotes.net/allmovies/Z"
        ]
    
    # Custom settings for this spider
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'DOWNLOAD_DELAY': 2,  # 2 seconds delay between requests
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 1,  # Only one request at a time
        'ROBOTSTXT_OBEY': False,  # We need to set this to False as the site might block bots
        'JOBDIR': 'crawler/jobs',  # Directory to store job state for resuming
        'LOG_LEVEL': 'INFO',
    }
    
    def __init__(self, *args, **kwargs):
        super(MovieQuotesSpider, self).__init__(*args, **kwargs)
        
        # Get batch parameters from command line arguments
        self.batch_size = int(kwargs.get('batch_size', 20))  # Default batch size: 20 movies
        self.start_index = int(kwargs.get('start_index', 0))  # Default start index: 0
        self.max_movies = int(kwargs.get('max_movies', 0))  # Default: 0 (no limit)
        
        # State file to keep track of processed movies
        self.state_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'state.json')
        
        # Load state if exists
        self.processed_movies = self.load_state()
        
        self.logger.info(f"Starting crawler with batch_size={self.batch_size}, start_index={self.start_index}")
        self.logger.info(f"Already processed {len(self.processed_movies)} movies")
    
    def load_state(self):
        """Load the state of processed movies from a file."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                self.logger.error(f"Error loading state file: {self.state_file}")
                return []
        return []
    
    def save_state(self):
        """Save the state of processed movies to a file."""
        with open(self.state_file, 'w') as f:
            json.dump(self.processed_movies, f)
        self.logger.info(f"Saved state with {len(self.processed_movies)} processed movies")

    def parse(self, response):
        """Parses the main movie list page."""
        self.logger.info(f"Parsing movie list page: {response.url}")
        
        # Check if we got a 403 error
        if response.status == 403:
            self.logger.error(f"Received 403 Forbidden error for {response.url}")
            return
        
        # Extract movie links from the page
        movie_links = response.css("a[href^='/movies/']::attr(href)").getall()
        self.logger.info(f"Found {len(movie_links)} movie links")
        
        # Calculate the end index for this batch
        end_index = self.start_index + self.batch_size
        if self.max_movies > 0:
            end_index = min(end_index, self.max_movies)
        
        # Process movies in the current batch
        batch_links = movie_links[self.start_index:end_index]
        self.logger.info(f"Processing batch from index {self.start_index} to {end_index-1} ({len(batch_links)} movies)")
        
        # Counter for processed movies in this batch
        processed_count = 0
        
        for link in batch_links:
            # Skip already processed movies
            movie_url = response.urljoin(link)
            if movie_url in self.processed_movies:
                self.logger.info(f"Skipping already processed movie: {movie_url}")
                continue
            
            # Add a small random delay
            time.sleep(random.uniform(1, 3))
            
            # Create a movie item
            # Extract movie title from URL, handling cases where the title contains slashes
            movie_path = link.split('/movies/')[-1]  # Get everything after '/movies/'
            movie_title = movie_path.replace('_', ' ')
            
            movie_item = MovieItem()
            movie_item['title'] = movie_title
            movie_item['url'] = movie_url
            
            yield scrapy.Request(
                movie_url,
                callback=self.parse_movie_details,
                meta={'movie_item': movie_item},
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            )
            
            processed_count += 1
        
        self.logger.info(f"Scheduled {processed_count} new movies for processing")
        
        # If we've reached the end of all movies or hit the max_movies limit
        if end_index >= len(movie_links) or (self.max_movies > 0 and end_index >= self.max_movies):
            self.logger.info("Reached the end of movie list or hit max_movies limit")
        else:
            # Log information about the next batch
            next_start = end_index
            next_end = next_start + self.batch_size
            if self.max_movies > 0:
                next_end = min(next_end, self.max_movies)
            
            self.logger.info(f"To process the next batch, run with start_index={next_start} (will process {next_start} to {next_end-1})")

    def parse_movie_details(self, response):
        """Parses the movie details page and extracts quotes."""
        self.logger.info(f"Parsing movie page: {response.url}")
        
        # Check if we got a 403 error
        if response.status == 403:
            self.logger.error(f"Received 403 Forbidden error for {response.url}")
            return
        
        movie_item = response.meta['movie_item']
        
        # Extract movie title - adjust selector based on the actual HTML structure
        title = response.css("h1::text").get()
        if title:
            movie_item['title'] = title.strip()
        
        # Extract movie quotes - adjust selectors based on the actual HTML structure
        quotes = []
        
        # Based on the provided URL content, quotes are in mquote tags
        quote_blocks = response.css("a[href^='/mquote/']")
        
        for quote_block in quote_blocks:
            quote_text = quote_block.xpath("string()").get()
            if quote_text:
                quote_item = QuoteItem()
                quote_item['text'] = quote_text.strip()
                quote_item['movie_title'] = movie_item['title']
                quotes.append(dict(quote_item))
        
        movie_item['quotes'] = quotes
        
        # Add this movie to the processed list
        self.processed_movies.append(movie_item['url'])
        self.save_state()
        
        self.logger.info(f"Extracted {len(quotes)} quotes from {movie_item['title']}")
        yield movie_item
    
    def closed(self, reason):
        """Called when the spider is closed."""
        self.logger.info(f"Spider closed: {reason}")
        self.logger.info(f"Total movies processed: {len(self.processed_movies)}")
        
        # Save final state
        self.save_state()
        
        # Create a summary file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), f'summary_{timestamp}.txt')
        
        with open(summary_file, 'w') as f:
            f.write(f"Crawler run completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total movies processed: {len(self.processed_movies)}\n")
            f.write(f"Batch size: {self.batch_size}\n")
            f.write(f"Start index: {self.start_index}\n")
            f.write(f"Max movies limit: {self.max_movies if self.max_movies > 0 else 'No limit'}\n")
            
            # Calculate next batch start index
            next_start = self.start_index + self.batch_size
            f.write(f"\nTo process the next batch, run with start_index={next_start}\n")
        
        self.logger.info(f"Summary saved to {summary_file}")