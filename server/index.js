/**
 * SignalAtlas v3 - API Server
 * Serves cached feed data from Redis
 * 
 * Memory safety: 
 * - NODE_OPTIONS=--max-old-space-size=4096 enforced
 * - Connection pooling for Redis
 * - No persistent in-memory caches
 * - Soft memory limit at 3GB heap
 */

import Fastify from 'fastify';
import { Redis } from '@upstash/redis';

const PORT = process.env.PORT || 3047;

// Upstash Redis connection
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || 'https://fleet-goose-76058.upstash.io',
  token: process.env.UPSTASH_REDIS_REST_TOKEN || 'gQAAAAAAASkaAAIncDE5YWFjYjU5MjlmMzE0ZTNmODBkNjJkODA5ZDY1MTA2M3AxNzYwNTg',
});

// Categories from the feed sheet
const CATEGORIES = [
  'Geopolitical/World', 'MENA', 'Africa', 'Asia-Pacific', 'Europe/Eurasia',
  'Intelligence', 'Intel/Military', 'OSINT Intelligence', 'Cyber Threat Intel',
  'Markets', 'Markets/Gulf', 'Crypto', 'Prediction Markets',
  'Economic Indicators', 'Central Banks', 'Trade', 'Supply Chain',
  'Energy', 'Climate', 'Natural Disasters', 'Wildfires',
  'Health', 'Health Advisories', 'Displacement', 'Humanitarian',
  'Maritime', 'Aviation', 'Aviation/Military', 'Space/Satellites',
  'Infrastructure', 'GPS Jamming', 'Research/Tech', 'Research/AI',
  'News Media', 'Travel Advisories', 'Israel Alerts',
  'Conflict/Protests', 'Conflict/Iran', 'Military Infrastructure',
  'Development', 'Government Spending', 'Other'
];

// GDELT Topics
const GDELT_TOPICS = ['military', 'cyber', 'nuclear', 'sanctions', 'intelligence', 'maritime'];

/**
 * Check memory usage
 */
function checkMemory() {
  const heapUsed = process.memoryUsage().heapUsed;
  return {
    heapUsed: Math.round(heapUsed / 1024 / 1024),
    heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024),
    rss: Math.round(process.memoryUsage().rss / 1024 / 1024)
  };
}

// Create Fastify instance
const fastify = Fastify({
  logger: true,
  trustProxy: true
});

// Health check endpoint
fastify.get('/api/health', async (request, reply) => {
  let redisStatus = 'disconnected';
  let lastSeed = null;
  let totalFeeds = 0;
  let successCount = 0;
  let errorCount = 0;
  let gdeltLastSeed = null;

  try {
    const pong = await redis.ping();
    redisStatus = pong === 'PONG' ? 'connected' : 'error';

    lastSeed = await redis.get('feeds:lastSeed');
    gdeltLastSeed = await redis.get('gdelt:lastSeed');
    
    const totalStr = await redis.get('feeds:totalCount');
    totalFeeds = parseInt(totalStr || '0', 10);
    
    const successStr = await redis.get('feeds:successCount');
    successCount = parseInt(successStr || '0', 10);
    
    const errorStr = await redis.get('feeds:errorCount');
    errorCount = parseInt(errorStr || '0', 10);

  } catch (error) {
    console.error('[HEALTH] Error:', error.message);
  }

  const memory = checkMemory();

  return {
    status: redisStatus === 'connected' ? 'healthy' : 'degraded',
    redis: redisStatus,
    lastSeed,
    gdeltLastSeed,
    feedsTotal: totalFeeds,
    feedsSuccess: successCount,
    feedsErrors: errorCount,
    memory,
    timestamp: new Date().toISOString()
  };
});

// Categories list endpoint
fastify.get('/api/categories', async (request, reply) => {
  const categoryList = [];
  
  for (const category of CATEGORIES) {
    // Count feeds in this category by scanning keys
    const pattern = `feed:${category}:*`;
    const keys = await redis.keys(pattern);
    const feedCount = Math.floor(keys.length / 2); // Each feed has :articles/:data and :meta
    
    if (feedCount > 0) {
      // Get most recent cache time
      let cachedAt = null;
      for (const key of keys) {
        if (key.endsWith(':meta')) {
          const meta = await redis.get(key);
          if (meta) {
            const parsed = JSON.parse(meta);
            if (!cachedAt || parsed.cachedAt > cachedAt) {
              cachedAt = parsed.cachedAt;
            }
          }
        }
      }
      
      categoryList.push({
        category,
        feedCount,
        cachedAt,
        status: 'active'
      });
    }
  }
  
  return {
    categories: categoryList,
    totalCategories: categoryList.length,
    timestamp: new Date().toISOString()
  };
});

// GDELT Topics endpoint
fastify.get('/api/gdelt/topics', async (request, reply) => {
  const topics = [];
  
  for (const topic of GDELT_TOPICS) {
    const key = `gdelt:${topic}:articles`;
    const data = await redis.get(key);
    
    if (data) {
      const parsed = JSON.parse(data);
      topics.push({
        topic: parsed.topic,
        count: parsed.count,
        cachedAt: parsed.cachedAt,
        expiresAt: parsed.expiresAt
      });
    } else {
      topics.push({
        topic,
        count: 0,
        cachedAt: null,
        expiresAt: null,
        status: 'no_data'
      });
    }
  }
  
  return {
    topics,
    totalTopics: topics.length,
    timestamp: new Date().toISOString()
  };
});

// GDELT single topic
fastify.get('/api/gdelt/:topic', async (request, reply) => {
  const { topic } = request.params;
  
  if (!GDELT_TOPICS.includes(topic)) {
    reply.code(400);
    return {
      error: 'Invalid GDELT topic',
      validTopics: GDELT_TOPICS
    };
  }
  
  const key = `gdelt:${topic}:articles`;
  const data = await redis.get(key);
  
  if (!data) {
    reply.code(404);
    return {
      error: 'No cached data for this GDELT topic',
      topic,
      suggestion: 'Run seed script to populate GDELT data'
    };
  }
  
  return JSON.parse(data);
});

// Category feeds endpoint
fastify.get('/api/feeds/:category', async (request, reply) => {
  const { category } = request.params;
  const { hours = '24' } = request.query;
  
  const decodedCategory = decodeURIComponent(category);
  
  // Find all feeds in this category
  const pattern = `feed:${decodedCategory}:*:meta`;
  const keys = await redis.keys(pattern);
  
  if (keys.length === 0) {
    reply.code(404);
    return {
      error: 'No feeds found for category',
      category: decodedCategory,
      suggestion: 'Run seed script to populate feeds'
    };
  }
  
  const feeds = [];
  
  for (const key of keys) {
    const meta = await redis.get(key);
    if (!meta) continue;
    
    const parsed = JSON.parse(meta);
    
    // Get the actual data/articles
    const nameKey = key.replace(':meta', '');
    const articlesData = await redis.get(`${nameKey}:articles`);
    const rawData = await redis.get(`${nameKey}:data`);
    
    const feed = {
      name: parsed.name,
      category: parsed.category,
      type: parsed.type,
      cachedAt: parsed.cachedAt,
      status: parsed.status,
      error: parsed.error || null
    };
    
    if (articlesData) {
      const articles = JSON.parse(articlesData);
      feed.articles = articles.articles || [];
      feed.articleCount = feed.articles.length;
    }
    
    if (rawData) {
      try {
        feed.data = JSON.parse(rawData);
      } catch {
        feed.dataRaw = rawData.substring(0, 500);
      }
    }
    
    feeds.push(feed);
  }
  
  return {
    category: decodedCategory,
    feedCount: feeds.length,
    feeds,
    timestamp: new Date().toISOString()
  };
});

// Single feed endpoint
fastify.get('/api/feed/:category/:name', async (request, reply) => {
  const { category, name } = request.params;
  const decodedCategory = decodeURIComponent(category);
  const decodedName = decodeURIComponent(name).replace(/[^a-zA-Z0-9]/g, '_');
  
  const baseKey = `feed:${decodedCategory}:${decodedName}`;
  
  const meta = await redis.get(`${baseKey}:meta`);
  const articles = await redis.get(`${baseKey}:articles`);
  const data = await redis.get(`${baseKey}:data`);
  
  if (!meta && !articles && !data) {
    reply.code(404);
    return {
      error: 'Feed not found',
      category: decodedCategory,
      name: decodedName
    };
  }
  
  const response = {};
  
  if (meta) {
    response.meta = JSON.parse(meta);
  }
  
  if (articles) {
    response.articles = JSON.parse(articles);
  }
  
  if (data) {
    try {
      response.data = JSON.parse(data);
    } catch {
      response.dataRaw = data.substring(0, 1000);
    }
  }
  
  return response;
});

// All feeds summary (lightweight)
fastify.get('/api/feeds', async (request, reply) => {
  const { category } = request.query;
  
  let pattern = 'feed:*:meta';
  if (category) {
    pattern = `feed:${decodeURIComponent(category)}:*:meta`;
  }
  
  const keys = await redis.keys(pattern);
  const feeds = [];
  
  for (const key of keys.slice(0, 100)) { // Limit to 100 for performance
    const meta = await redis.get(key);
    if (!meta) continue;
    
    const parsed = JSON.parse(meta);
    feeds.push({
      name: parsed.name,
      category: parsed.category,
      type: parsed.type,
      status: parsed.status,
      cachedAt: parsed.cachedAt
    });
  }
  
  return {
    feeds,
    count: feeds.length,
    totalMatched: keys.length,
    timestamp: new Date().toISOString()
  };
});

// Startup
async function start() {
  try {
    // Upstash Redis connects automatically on first use
    console.log('[API] Upstash Redis configured');

    // Memory check interval (every 5 minutes)
    setInterval(() => {
      const mem = checkMemory();
      if (mem.heapUsed > 3000) {
        console.warn(`[API] High memory: ${mem.heapUsed}MB heap`);
      }
    }, 5 * 60 * 1000);

    await fastify.listen({ port: PORT, host: '0.0.0.0' });
    console.log(`[API] SignalAtlas v3 API running on port ${PORT}`);

  } catch (error) {
    console.error('[API] Startup error:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('[API] Shutting down...');
  await fastify.close();
  await redis.quit();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('[API] Shutting down...');
  await fastify.close();
  await redis.quit();
  process.exit(0);
});

start();
