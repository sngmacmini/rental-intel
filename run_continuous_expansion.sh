#!/bin/bash
# Continuous Expansion Runner - Runs in background

LOG="/tmp/rent_continuous.log"
SCRIPT_DIR="$HOME/Projects/rental-intel"

echo "[$(date)] Rent Trend 24/7 Expansion Started" >> $LOG

# Priority cities to expand
declare -a PRIORITY_CITIES=(
  "Concord,CA,100"
  "Berkeley,CA,150"
  "Oakland,CA,200"
  "Palo Alto,CA,100"
  "Mountain View,CA,100"
  "Sunnyvale,CA,100"
  "Santa Clara,CA,100"
  "Fremont,CA,150"
  "San Jose,CA,300"
  "Richmond,CA,100"
  "San Leandro,CA,100"
  "Hayward,CA,100"
)

TOTAL_ADDED=0
CYCLE=0

while true; do
  CYCLE=$((CYCLE + 1))
  echo "[$(date)] Starting cycle $CYCLE" >> $LOG
  
  for city_data in "${PRIORITY_CITIES[@]}"; do
    IFS=',' read -r city state count <<< "$city_data"
    
    echo "[$(date)] Processing $city, $state (target: $count)" >> $LOG
    
    cd $SCRIPT_DIR
    python3 expand_coverage.py >> $LOG 2>&1
    
    sleep 10
  done
  
  echo "[$(date)] Cycle $CYCLE complete. Sleeping 1 hour." >> $LOG
  sleep 3600
done
