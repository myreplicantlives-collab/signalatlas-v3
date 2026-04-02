# SignalAtlas v3 - Railway Deployment
FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --only=production

# Copy scripts
COPY scripts/ ./scripts/

# Run seed script (called by cron trigger)
CMD ["node", "scripts/seed-all.mjs"]
