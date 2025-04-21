from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.responses import JSONResponse
from typing import List
import json
import os
import subprocess
import httpx

app = FastAPI()

CRAWLER_OUTPUT_FILE = "movies.json"

def run_scrapy_crawler():
    """Runs the Scrapy crawler and saves the output to a JSON file."""
    try:
        # Create crawler directory if it doesn't exist
        os.makedirs("crawler", exist_ok=True)
        
        # Remove existing output file if it exists
        output_path = os.path.join("crawler", CRAWLER_OUTPUT_FILE)
        if os.path.exists(output_path):
            os.remove(output_path)
            
        # Run the crawler using the run.py script with Poetry
        result = subprocess.run(
            ["poetry", "run", "python", f"crawler/run.py"],
            check=True,
            capture_output=True,
            text=True
        )
        
        print(f"Scrapy command output: {result.stdout}")
        print(f"Scrapy error output: {result.stderr}")
        
        # Check if the output file was created
        if os.path.exists(output_path):
            print(f"Scrapy crawler output file created: {output_path}")
        else:
            print(f"Warning: Output file not found at {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error running Scrapy: {e.stderr}")
        raise HTTPException(status_code=500, detail=f"Error running Scrapy: {e.stderr}")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="Scrapy command not found. Ensure Scrapy is installed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.post("/crawl")
async def crawl_website():
    """Starts the web crawling process."""
    run_scrapy_crawler()
    return {"message": "Crawling started. Check /movies for results."}

@app.get("/movies")
async def get_movies():
    """Retrieves the scraped movie data."""
    output_path = os.path.join("crawler", CRAWLER_OUTPUT_FILE)
    if not os.path.exists(output_path):
        raise HTTPException(status_code=404, detail="No data found. Run /crawl first.")
    try:
        with open(output_path, "r") as f:
            content = f.read()
            if not content.strip():
                raise HTTPException(status_code=404, detail="Empty data file. Try running /crawl again.")
            data = json.loads(content)
        return JSONResponse(content=data)
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Error decoding JSON data.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok"}