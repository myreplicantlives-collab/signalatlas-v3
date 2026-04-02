# SignalAtlas v3

**Generic multi-source intelligence feed pipeline with Redis caching.**

Covers ALL feeds from the World Monitor Google Sheet (251 feeds) across 44 categories.

## Architecture

```
Google Sheet → Seed Script → Redis Cache → API Server → Frontend
```

**Feed Types Supported:**
- RSS/XML feeds (52 feeds) — articles parsed and cached
- REST/JSON APIs (135 feeds) — JSON responses cached
- GeoJSON APIs — spatial data cached
- CSV feeds — tabular data parsed and cached
- Atom feeds — article entries parsed
- GDELT (6 anchor topics) — high-priority geopolitical intel

## Memory Safety

- All Node processes: `NODE_OPTIONS=--max-old-space-size=4096`
- Redis: 512MB maxmemory, allkeys-lru eviction
- API server: restarts if heap exceeds 3GB
- Launchd service: 512MB physical memory limit
- Seed script: stateless batch processing with concurrency limits

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Redis, GDELT status, feed counts, memory |
| `GET /api/categories` | All categories with feed counts |
| `GET /api/gdelt/topics` | GDELT 6 topics with article counts |
| `GET /api/gdelt/:topic` | Articles for a GDELT topic |
| `GET /api/feeds` | All feeds summary (first 100) |
| `GET /api/feeds/:category` | All feeds in a category |
| `GET /api/feed/:category/:name` | Single feed data |

## Categories

44 categories including: Intelligence, Intel/Military, Markets, Crypto, Energy, Climate, Health, Trade, Supply Chain, Maritime, Aviation, Infrastructure, Cyber Threat Intel, Conflict/Protests, Humanitarian, Displacement, and more.

## Quick Start

```bash
# Install dependencies
cd /Users/twain/.openclaw/workspace-sally/signalatlas/v3
npm install

# Seed all feeds (first run - takes time)
npm run seed

# Seed only RSS feeds (Phase 1 - fastest)
npm run seed:rss

# Seed only JSON/REST feeds (Phase 2)
npm run seed:json

# Seed GDELT anchor feeds
node scripts/seed-gdelt-intel.mjs

# Start API server
npm run api
```

## API Examples

```bash
# Health check
curl http://localhost:3047/api/health

# List categories with data
curl http://localhost:3047/api/categories

# GDELT topics
curl http://localhost:3047/api/gdelt/topics

# GDELT military intel
curl http://localhost:3047/api/gdelt/military

# Feeds in Intelligence category
curl http://localhost:3047/api/feeds/Intelligence

# Specific feed
curl http://localhost:3047/api/feed/Cyber%20Threat%20Intel/ListCyberThreats
```

## Launchd Service (Persistent)

```bash
cp launchd/ai.openclaw.signalatlas-v3-api.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/ai.openclaw.signalatlas-v3-api.plist
```

## Redis Key Schema

```
gdelt:{topic}:articles     - GDELT articles (6h TTL)
feed:{category}:{name}:articles - RSS/Atom articles
feed:{category}:{name}:data     - JSON/REST/CSV/GeoJSON data
feed:{category}:{name}:meta     - Feed metadata
feeds:lastSeed             - Last seed timestamp
feeds:totalCount           - Total feeds processed
feeds:successCount         - Successful feeds
feeds:errorCount           - Failed feeds
```

## Phased Build

- **Phase 1**: RSS/XML/Atom feeds (52) — articles extraction
- **Phase 2**: JSON/REST APIs (135) — response caching
- **Phase 3**: CSV, GeoJSON, specialty formats

## Troubleshooting

```bash
# Check Redis
redis-cli ping
redis-cli info memory
redis-cli keys "feed:*" | wc -l
redis-cli keys "gdelt:*"

# Check API logs
tail -f launchd/ai.openclaw.signalatlas-v3-api.log

# Clear all feed data (re-seed)
redis-cli flushall

# Test specific feed
curl -I "https://th.usembassy.gov/rss.xml"
```

## File Structure

```
signalatlas/v3/
├── package.json
├── README.md
├── scripts/
│   ├── seed-feeds.mjs        # Generic feed seeder
│   └── seed-gdelt-intel.mjs  # GDELT anchor feed
├── server/
│   └── index.js              # API server (Fastify)
└── launchd/
    └── ai.openclaw.signalatlas-v3-api.plist
```
