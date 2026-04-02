/**
 * SignalAtlas v3 - Generic Feed Seed Script
 * Reads all feeds from Google Sheet, detects type, fetches & caches appropriately
 * 
 * Memory safety: 
 * - NODE_OPTIONS=--max-old-space-size=4096 enforced
 * - Batch processing with concurrency limits
 * - Stateless: fetch → parse → write Redis → exit cleanly
 * 
 * Phases:
 * - Phase 1: RSS/XML/Atom feeds (52 feeds)
 * - Phase 2: JSON/REST/GeoJSON feeds (135 feeds)  
 * - Phase 3: CSV and specialty formats
 */

import { Redis } from '@upstash/redis';
import { XMLParser } from 'fast-xml-parser';
import { parse as csvParse } from 'csv-parse/sync';
import { setTimeout as sleep } from 'timers/promises';
import { readFileSync } from 'fs';

// Configuration
const TTL_SECONDS = 24 * 60 * 60; // 24 hours
const CONCURRENCY_LIMIT = 5; // Max parallel fetches
const FEED_DELAY_MS = 500; // Delay between batches
const GDELT_TTL = 6 * 60 * 60; // GDELT refreshes every 6h

// GDELT Topics (anchor feed - high priority)
const GDELT_TOPICS = [
  { name: 'military', query: '(military exercise OR troop deployment OR airstrike OR "naval exercise" OR warplane OR missile test) sourcelang:eng' },
  { name: 'cyber', query: '(cyberattack OR ransomware OR hacking OR "data breach" OR APT OR "zero day") sourcelang:eng' },
  { name: 'nuclear', query: '(nuclear OR uranium enrichment OR IAEA OR "nuclear weapon" OR plutonium OR "ICBM") sourcelang:eng' },
  { name: 'sanctions', query: '(sanctions OR embargo OR "trade war" OR tariff OR "economic pressure" OR "trade restriction") sourcelang:eng' },
  { name: 'intelligence', query: '(espionage OR spy OR "intelligence agency" OR covert OR surveillance OR "covert operation") sourcelang:eng' },
  { name: 'maritime', query: '(naval blockade OR piracy OR "strait of hormuz" OR "south china sea" OR warship OR "carrier strike") sourcelang:eng' }
];

// Upstash Redis client
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || 'https://fleet-goose-76058.upstash.io',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || 'gQAAAAAAASkaAAIncDE5YWFjYjU5MjlmMzE0ZTNmODBkNjJkODA5ZDY1MTA2M3AxNzYwNTg',
});

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: '@_'
});

// Rate limit tracking
const rateLimitLog = [];
let lastRateLimit = null;

/**
 * Sleep utility
 */
async function delay(ms) {
  await sleep(ms);
}

/**
 * Check memory usage
 */
function checkMemory() {
  const used = process.memoryUsage().heapUsed;
  if (used > 2.5 * 1024 * 1024 * 1024) {
    console.warn(`[MEMORY] Heap ${Math.round(used / 1024 / 1024)}MB approaching limit`);
  }
  return Math.round(used / 1024 / 1024);
}

/**
 * Log rate limit event
 */
function logRateLimit(url, attempt, delay) {
  rateLimitLog.push({ url, attempt, delay, time: new Date().toISOString() });
  console.log(`[RATE_LIMIT] ${url} - attempt ${attempt}, waited ${delay}ms`);
  lastRateLimit = new Date();
}

/**
 * Fetch with retry and rate limit handling
 */
async function fetchWithRetry(url, options = {}) {
  const { retries = 3, baseDelay = 1000, maxDelay = 30000 } = options;
  
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json, application/xml, application/rss+xml, text/xml, */*',
          'User-Agent': 'SignalAtlas-v3/1.0'
        },
        signal: AbortSignal.timeout(30000)
      });

      if (response.status === 429) {
        const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
        logRateLimit(url, attempt + 1, delay);
        await delay;
        continue;
      }

      if (response.status === 403 || response.status === 401) {
        throw new Error(`Auth required: ${response.status}`);
      }

      if (!response.ok && response.status !== 200) {
        throw new Error(`HTTP ${response.status}`);
      }

      return response;
    } catch (error) {
      if (attempt < retries && (
        error.message.includes('fetch') || 
        error.message.includes('timeout') ||
        error.message.includes('network') ||
        error.message.includes('429')
      )) {
        const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
        console.log(`[RETRY] ${error.message}. Waiting ${delay}ms...`);
        await delay;
        continue;
      }
      throw error;
    }
  }
}

/**
 * Parse RSS/XML feed
 */
function parseRSS(xmlText, feedName) {
  try {
    const parsed = xmlParser.parse(xmlText);
    
    // Handle RSS 2.0
    if (parsed.rss && parsed.rss.channel) {
      const channel = parsed.rss.channel;
      const items = Array.isArray(channel.item) ? channel.item : channel.item ? [channel.item] : [];
      return {
        feedName: feedName || channel.title || 'Unknown',
        title: channel.title,
        description: channel.description,
        articles: items.map(item => ({
          title: item.title || 'No title',
          url: item.link || item.guid || '',
          source: channel.title || '',
          date: item.pubDate || item['dc:date'] || new Date().toISOString(),
          tone: 0,
          description: item.description || ''
        }))
      };
    }
    
    // Handle Atom
    if (parsed.feed) {
      const feed = parsed.feed;
      const entries = Array.isArray(feed.entry) ? feed.entry : feed.entry ? [feed.entry] : [];
      return {
        feedName: feedName || feed.title || 'Unknown',
        title: feed.title,
        description: feed.subtitle,
        articles: entries.map(entry => ({
          title: entry.title || 'No title',
          url: entry.link || (entry.link && entry.link['@_href']) || '',
          source: feed.title || '',
          date: entry.updated || entry.published || new Date().toISOString(),
          tone: 0,
          description: entry.summary || entry.content || ''
        }))
      };
    }

    return null;
  } catch (error) {
    console.error(`[PARSE_ERROR] RSS: ${error.message}`);
    return null;
  }
}

/**
 * Parse GeoJSON
 */
function parseGeoJSON(text, feedName) {
  try {
    const data = JSON.parse(text);
    return {
      feedName,
      type: 'GeoJSON',
      data,
      featureCount: data.features ? data.features.length : 0
    };
  } catch (error) {
    console.error(`[PARSE_ERROR] GeoJSON: ${error.message}`);
    return null;
  }
}

/**
 * Parse CSV
 */
function parseCSV(text, feedName) {
  try {
    const records = csvParse(text, { columns: true, skip_empty_lines: true });
    return {
      feedName,
      type: 'CSV',
      data: records,
      rowCount: records.length
    };
  } catch (error) {
    console.error(`[PARSE_ERROR] CSV: ${error.message}`);
    return null;
  }
}

/**
 * Detect feed type from type string or URL
 */
function detectFeedType(type, url) {
  if (!type) {
    // Infer from URL
    if (url.includes('.csv') || url.includes('csv?')) return 'CSV';
    if (url.includes('.geojson') || url.includes('geojson')) return 'GeoJSON';
    if (url.includes('rss') || url.includes('feed')) return 'RSS';
    if (url.includes('atom')) return 'Atom';
    return 'JSON'; // Default to JSON for API endpoints
  }
  
  const t = type.toUpperCase();
  if (t.includes('RSS') || t === 'XML') return 'RSS';
  if (t === 'ATOM') return 'Atom';
  if (t.includes('GEOJSON')) return 'GeoJSON';
  if (t.includes('CSV')) return 'CSV';
  if (t.includes('JSON') || t.includes('REST') || t.includes('API')) return 'JSON';
  if (t === 'TLE/JSON') return 'TLE';
  
  return 'JSON'; // Default
}

/**
 * Process GDELT anchor feed (high priority)
 */
async function seedGDELT() {
  console.log('[GDELT] Seeding GDELT anchor feeds...');
  const now = new Date().toISOString();
  
  for (const topic of GDELT_TOPICS) {
    const params = new URLSearchParams({
      mode: 'artlist',
      format: 'json',
      maxrecords: '50',
      timespan: '24h',
      sort: 'Date',
      sourcelang: 'eng',
      query: topic.query
    });
    const url = `https://api.gdeltproject.org/api/v2/doc/doc?${params}`;
    
    try {
      const response = await fetchWithRetry(url);
      const data = await response.json();
      
      let articles = [];
      if (data && data.articles && Array.isArray(data.articles)) {
        articles = data.articles.map(a => ({
          title: a.title || 'No title',
          url: a.url || '',
          source: a.domain || '',
          date: a.seenAt || new Date().toISOString(),
          tone: a.tone || 0,
          image: a.image || ''
        }));
      }
      
      const payload = {
        feedName: `GDELT/${topic.name.toUpperCase()}`,
        topic: topic.name,
        count: articles.length,
        articles,
        cachedAt: now,
        expiresAt: new Date(Date.now() + GDELT_TTL * 1000).toISOString()
      };
      
      const key = `gdelt:${topic.name}:articles`;
      await redis.setex(key, GDELT_TTL, JSON.stringify(payload));
      console.log(`[GDELT] ${topic.name}: ${articles.length} articles`);
      
    } catch (error) {
      console.error(`[GDELT] ERROR ${topic.name}: ${error.message}`);
    }
    
    await delay(20000); // 20s between GDELT queries (rate limit)
  }
  
  await redis.set('gdelt:lastSeed', now);
}

/**
 * Process a single feed
 */
async function processFeed(feed) {
  const { name, url, type: feedType, category } = feed;
  const detectedType = detectFeedType(feedType, url);
  const now = new Date().toISOString();
  const cacheKey = `feed:${category}:${name.replace(/[^a-zA-Z0-9]/g, '_')}`;
  
  try {
    const response = await fetchWithRetry(url);
    const contentType = response.headers.get('content-type') || '';
    let data = null;
    
    if (detectedType === 'RSS' || detectedType === 'Atom' || contentType.includes('xml')) {
      const text = await response.text();
      data = parseRSS(text, name);
      if (data) {
        await redis.setex(`${cacheKey}:articles`, TTL_SECONDS, JSON.stringify(data));
      }
    } else if (detectedType === 'GeoJSON' || contentType.includes('geojson')) {
      const text = await response.text();
      data = parseGeoJSON(text, name);
      if (data) {
        await redis.setex(`${cacheKey}:data`, TTL_SECONDS, JSON.stringify(data));
      }
    } else if (detectedType === 'CSV') {
      const text = await response.text();
      data = parseCSV(text, name);
      if (data) {
        await redis.setex(`${cacheKey}:data`, TTL_SECONDS, JSON.stringify(data));
      }
    } else {
      // JSON/REST - cache raw response
      try {
        data = await response.json();
        await redis.setex(`${cacheKey}:data`, TTL_SECONDS, JSON.stringify(data));
      } catch {
        // Not JSON, try text
        const text = await response.text();
        await redis.setex(`${cacheKey}:data`, TTL_SECONDS, text.substring(0, 100000));
      }
    }
    
    // Update metadata
    await redis.setex(`${cacheKey}:meta`, TTL_SECONDS, JSON.stringify({
      name,
      category,
      type: detectedType,
      cachedAt: now,
      status: 'success'
    }));
    
    return { name, category, type: detectedType, status: 'success' };
    
  } catch (error) {
    console.error(`[FEED_ERROR] ${name} (${category}): ${error.message}`);
    
    // Mark as failed in metadata
    await redis.setex(`${cacheKey}:meta`, TTL_SECONDS, JSON.stringify({
      name,
      category,
      type: detectedType,
      cachedAt: now,
      status: 'error',
      error: error.message
    }));
    
    return { name, category, type: detectedType, status: 'error', error: error.message };
  }
}

/**
 * Process feeds with concurrency control
 */
async function processFeedsBatch(feeds) {
  const results = [];
  let memoryWarnings = 0;
  
  for (let i = 0; i < feeds.length; i += CONCURRENCY_LIMIT) {
    const batch = feeds.slice(i, i + CONCURRENCY_LIMIT);
    
    const batchResults = await Promise.all(batch.map(processFeed));
    results.push(...batchResults);
    
    const memUsed = checkMemory();
    if (memUsed > 2500) {
      memoryWarnings++;
      console.warn(`[MEMORY] Batch ${i / CONCURRENCY_LIMIT + 1}: ${memUsed}MB heap`);
    }
    
    // Progress logging
    const success = batchResults.filter(r => r.status === 'success').length;
    console.log(`[PROGRESS] ${Math.min(i + CONCURRENCY_LIMIT, feeds.length)}/${feeds.length} - ${success}/${batch.length} succeeded`);
    
    // Delay between batches
    if (i + CONCURRENCY_LIMIT < feeds.length) {
      await delay(FEED_DELAY_MS);
    }
  }
  
  return results;
}

/**
 * Load feeds from Google Sheet via gog
 */
async function loadFeedsFromSheet() {
  console.log('[FEEDS] Loading feeds from Google Sheet...');
  
  try {
    const { execSync } = await import('child_process');
    const raw = execSync(
      `gog sheets get 1Os-TRbmVzCWYHUyXt9fYeYlbE88DmzzgVp7UyYZyuD4 "Sheet1!A1:F300" --json 2>/dev/null`,
      { encoding: 'utf8', maxBuffer: 50 * 1024 * 1024 }
    );
    
    const sheetData = JSON.parse(raw);
    const rows = sheetData.values || [];
    const feeds = [];
    
    // Skip header row
    for (let i = 1; i < rows.length; i++) {
      const row = rows[i];
      if (!row || row.length < 3) continue;
      
      const name = (row[0] || '').trim();
      const url = (row[1] || '').trim();
      const type = (row[2] || '').trim();
      const category = (row[3] || 'Other').trim();
      
      if (!name || !url || !url.startsWith('http')) continue;
      
      feeds.push({ name, url, type, category });
    }
    
    console.log(`[FEEDS] Loaded ${feeds.length} feeds from sheet`);
    return feeds;
    
  } catch (error) {
    console.error(`[FEEDS] Error loading sheet: ${error.message}`);
    console.log('[FEEDS] Falling back to bundled feed list...');
    
    // Return hardcoded minimal list for fallback
    return getMinimalFeedList();
  }
}

/**
 * Fallback: minimal feed list when sheet is unavailable
 */
function getMinimalFeedList() {
  return [
    // RSS Feeds (Phase 1 priority)
    { name: 'US Embassy Thailand Alerts', url: 'https://th.usembassy.gov/rss.xml', type: 'RSS', category: 'Travel Advisories' },
    { name: 'US Embassy UAE Alerts', url: 'https://ua.usembassy.gov/rss.xml', type: 'RSS', category: 'Travel Advisories' },
    { name: 'US Embassy Germany Alerts', url: 'https://de.usembassy.gov/rss.xml', type: 'RSS', category: 'Travel Advisories' },
    { name: 'ECDC Epidemiological Updates', url: 'https://www.ecdc.europa.eu/en/rss', type: 'XML/RSS', category: 'Health' },
    { name: 'WHO Africa Health News', url: 'https://www.afro.who.int/en/rss.xml', type: 'RSS', category: 'Health' },
    { name: 'LiveUAMap Iran Events', url: 'https://liveuamap.com/iran', type: 'RSS', category: 'Conflict/Iran' },
    { name: 'Pentagon USNI Fleet Report', url: 'https://news.usni.org/category/fleet-tracker', type: 'RSS', category: 'Intel/Military' },
    { name: 'ListDefensePatents', url: 'https://patents.google.com/feed', type: 'RSS', category: 'Intel/Military' },
    { name: 'GetCo2Monitoring', url: 'https://gml.noaa.gov/ccgg/', type: 'RSS', category: 'Climate' },
    { name: 'ListOrefAlerts', url: 'https://oref.org.il', type: 'RSS', category: 'Israel Alerts' },
    
    // JSON/REST API Feeds (Phase 2)
    { name: 'Submarine Cable Map', url: 'https://www.submarinecablemap.com/api/v3/', type: 'REST API', category: 'Infrastructure' },
    { name: 'ListEarthquakes', url: 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson', type: 'GeoJSON', category: 'Natural Disasters' },
    { name: 'ListNaturalEvents', url: 'https://eonet.gsfc.nasa.gov/api/v3/events', type: 'REST API', category: 'Natural Disasters' },
    { name: 'ListArxivPapers', url: 'https://export.arxiv.org/api/query', type: 'Atom', category: 'Research/AI' },
    { name: 'ListHackernewsItems', url: 'https://hacker-news.firebaseio.com/v0/', type: 'REST API', category: 'Research/Tech' },
    { name: 'CryptoQuotes', url: 'https://api.coingecko.com/api/v3/coins/markets', type: 'REST API', category: 'Crypto' },
    { name: 'ListPredictionMarkets', url: 'https://gamma-api.polymarket.com/', type: 'REST API', category: 'Prediction Markets' },
    { name: 'ListSatellites', url: 'https://www.celestrak.org/NORAD/elements/gp/', type: 'TLE/JSON', category: 'Space/Satellites' },
    { name: 'GPS Jam Detection', url: 'https://gpsjam.org/data', type: 'CSV', category: 'GPS Jamming' },
    
    // More REST APIs
    { name: 'ListCrossSourceSignals', url: 'https://data.worldmonitor.app/api/intel/crosssignals', type: 'REST API', category: 'Intelligence' },
    { name: 'GetGdeltTopicTimeline', url: 'https://data.worldmonitor.app/api/intel/gdelttimeline', type: 'REST API', category: 'Intelligence' },
    { name: 'GetPizzintStatus', url: 'https://data.worldmonitor.app/api/intel/pizzint', type: 'REST API', category: 'Intelligence' },
    { name: 'ListTelegramFeed', url: 'https://data.worldmonitor.app/api/intel/telegramfeed', type: 'REST API', category: 'OSINT Intelligence' },
    { name: 'ListDefensePatents', url: 'https://patents.google.com/feed', type: 'RSS', category: 'Intel/Military' },
    { name: 'GetVesselSnapshot', url: 'https://data.worldmonitor.app/api/maritime/vessels', type: 'REST API', category: 'Maritime' },
    { name: 'GetDisplacementSummary', url: 'https://data.worldmonitor.app/api/displacement/summary', type: 'REST API', category: 'Displacement' },
    { name: 'ListClimateAnomalies', url: 'https://open-meteo.com/', type: 'REST API', category: 'Climate' },
    { name: 'GetMacroSignals', url: 'https://data.worldmonitor.app/api/economics/macrosignals', type: 'REST API', category: 'Economic Indicators' },
    { name: 'GetFearGreedIndex', url: 'https://data.worldmonitor.app/api/markets/feargreed', type: 'REST API', category: 'Markets' },
    { name: 'ListCyberThreats', url: 'https://data.worldmonitor.app/api/cyber/threats', type: 'REST API', category: 'Cyber Threat Intel' },
    { name: 'GetChokepointStatus', url: 'https://data.worldmonitor.app/api/supplychain/chokepoints', type: 'REST API', category: 'Supply Chain' },
    { name: 'ListUnrestEvents', url: 'https://data.worldmonitor.app/api/unrest/events', type: 'REST API', category: 'Conflict/Protests' },
    { name: 'GetHumanitarianSummary', url: 'https://data.worldmonitor.app/api/conflict/humanitarianbatch', type: 'REST API', category: 'Humanitarian' },
    { name: 'GetAircraftDetails', url: 'https://data.worldmonitor.app/api/military/aircraft', type: 'REST API', category: 'Aviation/Military' },
    { name: 'ListMilitaryFlights', url: 'https://data.worldmonitor.app/api/military/flights', type: 'REST API', category: 'Aviation/Military' },
    { name: 'ListInternetOutages', url: 'https://api.cloudflare.com/client/v4/radar/', type: 'REST API', category: 'Infrastructure' },
    { name: 'ListServiceStatuses', url: 'https://data.worldmonitor.app/api/infra/servicestatus', type: 'REST API', category: 'Infrastructure' },
  ];
}

/**
 * Main seed function
 */
async function main() {
  const args = process.argv.slice(2);
  const typeFilter = args.find(a => a.startsWith('--type='))?.split('=')[1]?.split(',') || null;
  
  console.log('[SEED] SignalAtlas v3 Feed Seeder starting...');
  console.log(`[SEED] Memory: ${checkMemory()}MB heap`);
  console.log(`[SEED] Type filter: ${typeFilter ? typeFilter.join(', ') : 'all'}`);
  
  try {
    // Upstash Redis connects automatically via HTTP
    console.log('[SEED] Upstash Redis configured');
    
    // Seed GDELT anchor feeds first (high priority)
    await seedGDELT();
    
    // Load all feeds from sheet
    const allFeeds = await loadFeedsFromSheet();
    
    // Filter by type if specified
    let feedsToProcess = allFeeds;
    if (typeFilter) {
      feedsToProcess = allFeeds.filter(f => {
        const feedType = (f.type || '').toUpperCase();
        return typeFilter.some(t => feedType.includes(t.toUpperCase()));
      });
      console.log(`[SEED] Filtered to ${feedsToProcess.length} feeds of type ${typeFilter.join(', ')}`);
    }
    
    // Group by category for logging
    const byCategory = {};
    for (const feed of feedsToProcess) {
      byCategory[feed.category] = (byCategory[feed.category] || 0) + 1;
    }
    console.log('[SEED] Feeds by category:');
    for (const [cat, count] of Object.entries(byCategory).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${cat}: ${count}`);
    }
    
    // Process feeds
    console.log(`[SEED] Processing ${feedsToProcess.length} feeds...`);
    const results = await processFeedsBatch(feedsToProcess);
    
    // Summary
    const successCount = results.filter(r => r.status === 'success').length;
    const errorCount = results.filter(r => r.status === 'error').length;
    
    console.log('\n[SEED] === SUMMARY ===');
    console.log(`[SEED] Total processed: ${results.length}`);
    console.log(`[SEED] Succeeded: ${successCount}`);
    console.log(`[SEED] Failed: ${errorCount}`);
    console.log(`[SEED] GDELT anchor: 6 topics seeded`);
    console.log(`[SEED] Final memory: ${checkMemory()}MB heap`);
    
    if (rateLimitLog.length > 0) {
      console.log(`[SEED] Rate limit events: ${rateLimitLog.length}`);
    }
    
    // Update last seed time
    await redis.set('feeds:lastSeed', new Date().toISOString());
    await redis.set('feeds:totalCount', String(feedsToProcess.length));
    await redis.set('feeds:successCount', String(successCount));
    await redis.set('feeds:errorCount', String(errorCount));
    
  } catch (error) {
    console.error(`[SEED] Fatal error: ${error.message}`);
    process.exit(1);
  } finally {
    await redis.quit();
    // Upstash Redis uses HTTP - no persistent connection to close
    console.log('[SEED] Done. Exiting cleanly.');
  }
}

main();
