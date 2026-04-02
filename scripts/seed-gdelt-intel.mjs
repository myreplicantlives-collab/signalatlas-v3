/**
 * SignalAtlas v3 - GDELT Intel Seed Script
 * Fetches intelligence articles for 6 topics and caches them in Redis
 * Runs every 6 hours via launchd/cron
 * 
 * Memory safety: NODE_OPTIONS=--max-old-space-size=4096 enforced
 */

import { Redis } from '@upstash/redis';
import { setTimeout as sleep } from 'timers/promises';

// Configuration
const GDELT_BASE_URL = 'https://api.gdeltproject.org/api/v2/doc/doc';
const TTL_SECONDS = 24 * 60 * 60; // 24 hours
const TOPIC_DELAY_MS = 20000; // 20s between topic queries
const MAX_RETRIES = 3;
const RETRY_DELAYS = [60000, 120000, 240000]; // Exponential backoff

// 6 intelligence topics
const TOPICS = [
  {
    name: 'military',
    query: '(military exercise OR troop deployment OR airstrike OR "naval exercise" OR warplane OR missile test) sourcelang:eng'
  },
  {
    name: 'cyber',
    query: '(cyberattack OR ransomware OR hacking OR "data breach" OR APT OR "zero day") sourcelang:eng'
  },
  {
    name: 'nuclear',
    query: '(nuclear OR uranium enrichment OR IAEA OR "nuclear weapon" OR plutonium OR "ICBM") sourcelang:eng'
  },
  {
    name: 'sanctions',
    query: '(sanctions OR embargo OR "trade war" OR tariff OR "economic pressure" OR "trade restriction") sourcelang:eng'
  },
  {
    name: 'intelligence',
    query: '(espionage OR spy OR "intelligence agency" OR covert OR surveillance OR "covert operation") sourcelang:eng'
  },
  {
    name: 'maritime',
    query: '(naval blockade OR piracy OR "strait of hormuz" OR "south china sea" OR warship OR "carrier strike") sourcelang:eng'
  }
];

// Upstash Redis client
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || 'https://fleet-goose-76058.upstash.io',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || 'gQAAAAAAASkaAAIncDE5YWFjYjU5MjlmMzE0ZTNmODBkNjJkODA5ZDY1MTA2M3AxNzYwNTg',
});

/**
 * Build GDELT API URL for a topic
 */
function buildGDELTUrl(topic) {
  const params = new URLSearchParams({
    mode: 'artlist',
    format: 'json',
    maxrecords: '50',
    timespan: '24h',
    sort: 'Date',
    sourcelang: 'eng',
    query: topic.query
  });
  return `${GDELT_BASE_URL}?${params.toString()}`;
}

/**
 * Fetch with rate limit handling and exponential backoff retry
 */
async function fetchWithRetry(url, retries = MAX_RETRIES) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json'
        }
      });

      if (response.status === 429) {
        if (attempt < retries) {
          const delay = RETRY_DELAYS[attempt];
          console.log(`[RATE_LIMIT] 429 received. Waiting ${delay}ms before retry ${attempt + 1}/${retries}`);
          console.log(`[RATE_LIMIT] URL: ${url}`);
          await sleep(delay);
          continue;
        } else {
          throw new Error(`Rate limit exceeded after ${retries} retries`);
        }
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      return await response.json();
    } catch (error) {
      if (attempt < retries && (error.message.includes('429') || error.message.includes('fetch'))) {
        const delay = RETRY_DELAYS[attempt];
        console.log(`[RETRY] ${error.message}. Retrying in ${delay}ms (${attempt + 1}/${retries})`);
        await sleep(delay);
        continue;
      }
      throw error;
    }
  }
}

/**
 * Normalize GDELT article to our schema
 */
function normalizeArticle(article, topic) {
  return {
    title: article.title || 'No title',
    url: article.url || article.seenAt || '',
    source: article.domain || article.source || '',
    date: article.seenAt || article.date || new Date().toISOString(),
    tone: article.tone || 0,
    image: article.image || article.socialimage || ''
  };
}

/**
 * Seed a single topic
 */
async function seedTopic(topic) {
  const url = buildGDELTUrl(topic);
  console.log(`[SEED] Fetching ${topic.name}...`);

  const data = await fetchWithRetry(url);

  let articles = [];
  if (data && data.articles && Array.isArray(data.articles)) {
    articles = data.articles.map(a => normalizeArticle(a, topic.name));
  } else if (data && data.result && Array.isArray(data.result)) {
    articles = data.result.map(a => normalizeArticle(a, topic.name));
  }

  const cacheKey = `gdelt:${topic.name}:articles`;
  const now = new Date().toISOString();
  const expiresAt = new Date(Date.now() + TTL_SECONDS * 1000).toISOString();

  const payload = {
    topic: topic.name,
    count: articles.length,
    articles,
    cachedAt: now,
    expiresAt
  };

  // Store in Redis with TTL
  await redis.setex(cacheKey, TTL_SECONDS, JSON.stringify(payload));

  // Also store metadata for /topics endpoint
  const metaKey = `gdelt:${topic.name}:meta`;
  await redis.setex(metaKey, TTL_SECONDS, JSON.stringify({
    topic: topic.name,
    count: articles.length,
    cachedAt: now,
    expiresAt
  }));

  console.log(`[SEED] ${topic.name}: ${articles.length} articles cached`);

  // Update last seed time
  await redis.set('gdelt:lastSeed', now);

  return articles.length;
}

/**
 * Health check - verify GDELT API is reachable
 */
async function checkGDELTHealth() {
  try {
    const testUrl = `${GDELT_BASE_URL}?mode=artlist&format=json&maxrecords=1&timespan=1h&query=test`;
    const response = await fetch(testUrl);
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Main seed function
 */
async function main() {
  console.log('[SEED] SignalAtlas v3 GDELT Seed Script starting...');
  console.log(`[SEED] Memory limit: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB heap used`);

  try {
    // Upstash Redis connects automatically via HTTP
    console.log('[SEED] Upstash Redis configured');

    // Verify GDELT API is reachable
    const gdeltHealthy = await checkGDELTHealth();
    if (!gdeltHealthy) {
      console.warn('[SEED] WARNING: GDELT API may be unreachable');
    }

    let totalArticles = 0;

    // Process each topic with delay between them
    for (let i = 0; i < TOPICS.length; i++) {
      const topic = TOPICS[i];

      try {
        const count = await seedTopic(topic);
        totalArticles += count;
      } catch (error) {
        console.error(`[SEED] ERROR seeding ${topic.name}: ${error.message}`);
        // Continue with other topics even if one fails
      }

      // Delay between topics (except after last)
      if (i < TOPICS.length - 1) {
        console.log(`[SEED] Waiting ${TOPIC_DELAY_MS}ms before next topic...`);
        await sleep(TOPIC_DELAY_MS);
      }
    }

    console.log(`[SEED] Complete. Total articles: ${totalArticles}`);
    console.log(`[SEED] Final memory: ${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB heap used`);

  } catch (error) {
    console.error(`[SEED] Fatal error: ${error.message}`);
    process.exit(1);
  } finally {
    // Upstash Redis uses HTTP - no persistent connection to close
    console.log('[SEED] Done. Exiting cleanly.');
  }
}

// Run if called directly
main();
