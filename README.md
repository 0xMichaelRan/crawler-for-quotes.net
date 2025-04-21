# Movie Quotes Crawler

A web scraper that extracts movie quotes from quotes.net using Scrapy and provides an API using FastAPI.

## Project Structure

```
movie_quotes_crawler/
├── main.py          # FastAPI application
├── crawler/
│   ├── __init__.py
│   ├── spiders/
│   │   ├── __init__.py
│   │   └── movie_quotes_spider.py  # Scrapy spider
│   ├── items.py      # Scrapy items
│   ├── pipelines.py  # Scrapy pipelines
│   ├── settings.py   # Scrapy settings
│   └── scrapy.cfg    # Scrapy configuration
├── requirements.txt
└── run_crawler.py    # Script to run the crawler
```

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/movie_quotes_crawler.git
cd movie_quotes_crawler

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Running the API

```bash
# Start the FastAPI application
uvicorn main:app --reload --port 8000
```

### API Endpoints

- `POST /crawl`: Start the crawling process
- `GET /movies`: Get the scraped movie data
- `GET /health`: Check the API health

### Example using curl

```bash
# Start the crawling process
curl -X POST http://localhost:8000/crawl

# Get the scraped movie data
curl http://localhost:8000/movies
```

### Running the Crawler Directly

#### Basic Usage

```bash
# Run the crawler without the API
python run_crawler.py
```

#### Batch Processing

The crawler supports batch processing to handle large numbers of movies. You can specify:

- `--batch-size`: Number of movies to process in each batch (default: 20)
- `--start-index`: Starting index in the movie list (default: 0)
- `--max-movies`: Maximum number of movies to process (default: 0, meaning no limit)
- `--append`: Append to existing output file instead of creating a new one
- `--no-display`: Do not display quotes in the terminal

Example to process movies 50-99:

```bash
python run_crawler.py --batch-size 50 --start-index 50
```

#### Resuming from Breakpoints

The crawler automatically saves state information in `crawler/state.json`. If you stop the crawler and want to continue from where you left off, simply run the next batch:

```bash
python run_crawler.py --start-index <next_index> --append
```

The crawler will automatically skip movies that have already been processed.

#### Processing All Movies

To process all movies from the website, you can run batches sequentially:

1. Run the first batch:
   ```bash
   python run_crawler.py --batch-size 100
   ```

2. After it completes, it will tell you the start index for the next batch. Run:
   ```bash
   python run_crawler.py --batch-size 100 --start-index <next_index> --append
   ```

3. Repeat until all movies are processed.

Alternatively, you can use the provided batch crawler script to automate this process:

```bash
# Make the script executable
chmod +x batch_crawler.sh

# Run with default settings (batch size: 100, start index: 0)
./batch_crawler.sh

# Run with custom settings
./batch_crawler.sh --batch-size 50 --start-index 100 --delay 120
```

The batch crawler script supports the following options:
- `--batch-size`: Number of movies to process in each batch (default: 100)
- `--start-index`: Starting index in the movie list (default: 0)
- `--max-movies`: Maximum number of movies to process (default: 0, meaning no limit)
- `--delay`: Delay between batches in seconds (default: 60)

The script will automatically:
- Run batches sequentially
- Append results to the same output file
- Log all activity to a timestamped log file in `crawler/logs/`
- Handle errors and provide resume instructions

## Troubleshooting

If you encounter any issues:

1. Make sure all dependencies are installed correctly
2. Check that the Scrapy configuration is set up properly
3. Verify that the website structure hasn't changed (which might break the scraper)
4. Look at the error messages in the terminal for more information

## License

MIT