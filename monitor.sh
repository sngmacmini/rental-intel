#!/bin/bash
# Monitor rental database growth

echo "=== Rental Intelligence Database Status ==="
echo "Timestamp: $(date)"
echo ""

# Get current count
COUNT=$(psql -d rental_intel -t -c "SELECT COUNT(*) FROM rental_intel.properties;" 2>/dev/null | xargs)
STATES=$(psql -d rental_intel -t -c "SELECT COUNT(DISTINCT state) FROM rental_intel.properties;" 2>/dev/null | xargs)

echo "Total Properties: $COUNT"
echo "States Covered: $STATES/50"
echo "Progress: $(echo "scale=2; ($COUNT / 10000000) * 100" | bc)%"
echo ""

# Show top states by count
echo "Top 10 States:"
psql -d rental_intel -c "SELECT state, COUNT(*) as cnt FROM rental_intel.properties GROUP BY state ORDER BY cnt DESC LIMIT 10;" 2>/dev/null | grep -E "^\s+[A-Z]{2}"
echo ""

# Check if ingestion is running
if pgrep -f "continuous_ingestion_10m" > /dev/null; then
    echo "Status: ✅ Ingestion RUNNING"
else
    echo "Status: ⚠️ Ingestion STOPPED"
fi
echo ""
echo "Last 5 lines of log:"
tail -5 /Users/sngmacmini/Projects/rental-intel/ingestion.log 2>/dev/null || echo "No log yet"
