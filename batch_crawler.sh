#!/bin/bash

# Batch crawler script for movie quotes
# This script runs the crawler in batches until all movies are processed

# Configuration
BATCH_SIZE=100
START_INDEX=0
MAX_MOVIES=0  # 0 means no limit
DELAY=10      # Delay between batches in seconds

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --batch-size)
      BATCH_SIZE="$2"
      shift 2
      ;;
    --start-index)
      START_INDEX="$2"
      shift 2
      ;;
    --max-movies)
      MAX_MOVIES="$2"
      shift 2
      ;;
    --delay)
      DELAY="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "Starting batch crawler with:"
echo "  Batch size: $BATCH_SIZE"
echo "  Start index: $START_INDEX"
echo "  Max movies: $MAX_MOVIES (0 = no limit)"
echo "  Delay between batches: $DELAY seconds"
echo ""

# Create output directory
mkdir -p crawler/logs

# Run batches until completion or error
CURRENT_INDEX=$START_INDEX
BATCH_NUMBER=1
LOG_FILE="crawler/logs/batch_crawler_$(date +%Y%m%d_%H%M%S).log"

echo "Logging to: $LOG_FILE"
echo "Starting batch crawler at $(date)" > "$LOG_FILE"

while true; do
    echo "Running batch #$BATCH_NUMBER (index $CURRENT_INDEX)" | tee -a "$LOG_FILE"
    
    # Run the crawler with the current batch
    python3 run_crawler.py --batch-size "$BATCH_SIZE" --start-index "$CURRENT_INDEX" --max-movies "$MAX_MOVIES" --no-display 2>&1 | tee -a "$LOG_FILE"
    
    # Check if the crawler exited with an error
    if [ $? -ne 0 ]; then
        echo "Error running batch #$BATCH_NUMBER. Check the log file for details." | tee -a "$LOG_FILE"
        echo "To resume, run: ./batch_crawler.sh --start-index $CURRENT_INDEX" | tee -a "$LOG_FILE"
        exit 1
    fi
    
    # Calculate next index
    CURRENT_INDEX=$((CURRENT_INDEX + BATCH_SIZE))
    BATCH_NUMBER=$((BATCH_NUMBER + 1))
    
    # Check if we've reached the max movies limit
    if [ "$MAX_MOVIES" -gt 0 ] && [ "$CURRENT_INDEX" -ge "$MAX_MOVIES" ]; then
        echo "Reached max movies limit ($MAX_MOVIES). Crawler completed successfully." | tee -a "$LOG_FILE"
        break
    fi
    
    echo "Waiting $DELAY seconds before starting next batch..." | tee -a "$LOG_FILE"
    sleep "$DELAY"
done

echo "Batch crawler completed at $(date)" | tee -a "$LOG_FILE"
echo "All batches completed successfully!" | tee -a "$LOG_FILE"