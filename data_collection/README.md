# Multi-State Rental Data Collection System

## Overview
Collect rental data from ALL 50 US states + major cities.

## Data Sources

### Tier 1 (Official APIs)
- Apartments.com API
- Zillow API
- Rent.com API
- Realtor.com API

### Tier 2 (Scrapers)
- Craigslist
- Facebook Marketplace
- Local property management sites

### Tier 3 (Government)
- HUD Fair Market Rent data
- Census Bureau housing data

## Coverage Target

| State | Priority Cities | Status |
|-------|-----------------|--------|
| CA | LA, SF, SD, SJ | ⏳ |
| TX | Houston, Dallas, Austin, SA | ⏳ |
| NY | NYC, Buffalo, Rochester | ⏳ |
| FL | Miami, Orlando, Tampa, Jax | ⏳ |
| IL | Chicago | ⏳ |
| PA | Philly, Pittsburgh | ⏳ |
| OH | Columbus, Cleveland, Cincy | ⏳ |
| GA | Atlanta | ⏳ |
| NC | Charlotte, Raleigh | ⏳ |
| MI | Detroit | ⏳ |
| ... | All 50 states | ⏳ |

## Architecture

```
┌─────────────────┐
│  Data Sources   │
└────────┬────────┘
         │
┌────────▼────────┐
│   Collectors    │ ← 50 state modules
└────────┬────────┘
         │
┌────────▼────────┐
│    Queue        │ ← Redis/RabbitMQ
└────────┬────────┘
         │
┌────────▼────────┐
│   Normalizer    │ ← Address hashing
└────────┬────────┘
         │
┌────────▼────────┐
│   PostgreSQL    │ ← rental_intel schema
└─────────────────┘
```

## Status
Building collectors now...
