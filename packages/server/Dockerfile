# Build from root context to include workspace dependencies
FROM node:20-alpine AS builder

# Install pnpm
RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy workspace root files
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
COPY .npmrc ./ 

# Copy all package manifests for workspace resolution
COPY packages/server/package.json ./packages/server/
COPY packages/client/package.json ./packages/client/
COPY packages/state/package.json ./packages/state/
COPY packages/server-conformance-tests/package.json ./packages/server-conformance-tests/

# Install dependencies
RUN pnpm install --frozen-lockfile

# Copy source code
COPY packages/server ./packages/server
COPY packages/client ./packages/client
COPY packages/state ./packages/state

# Build the server
RUN pnpm --filter @durable-streams/state --filter @durable-streams/client --filter @durable-streams/server build

# Production image
FROM node:20-alpine

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /app

# Copy workspace manifests
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/server/package.json ./packages/server/
COPY packages/client/package.json ./packages/client/
COPY packages/state/package.json ./packages/state/

# Install production dependencies only
RUN pnpm install --frozen-lockfile --prod --ignore-scripts

# Copy built artifacts
COPY --from=builder /app/packages/server/dist ./packages/server/dist
COPY --from=builder /app/packages/client/dist ./packages/client/dist
COPY --from=builder /app/packages/state/dist ./packages/state/dist

WORKDIR /app/packages/server

EXPOSE 3000

CMD ["node", "dist/start.js"]
