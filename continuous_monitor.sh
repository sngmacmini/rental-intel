#!/bin/bash
# Rent Trend Continuous Expansion Monitor
# Runs every hour to check progress and expand coverage

LOG_FILE="/tmp/rent_expansion.log"
EXPAND_SCRIPT="$HOME/Projects/rental-intel/expand_coverage.py"

echo "[$(date)] Rent Trend Expansion Monitor Started" >> $LOG_FILE

while true; do
    echo "[$(date)] Checking expansion status..." >> $LOG_FILE
    
    # Check if expand_coverage is running
    if ! pgrep -f "expand_coverage.py" > /dev/null; then
        echo "[$(date)] Restarting expansion process..." >> $LOG_FILE
        cd ~/Projects/rental-intel
        nohup python3 expand_coverage.py >> expand_coverage.log 2>&1 &
    fi
    
    # Check database stats hourly
    ssh buzzthat.net "mysql -u buzznet_rental_user -p'Rent4lD@t@2026!' buzznet_rental_intel -e 'SELECT state, COUNT(*) as cnt FROM properties GROUP BY state ORDER BY cnt DESC LIMIT 5;'" >> $LOG_FILE 2>&1
    echo "[$(date)] Stats updated" >> $LOG_FILE
    
    # Run for 1 hour then check again
    sleep 3600
done
