FROM node:22-slim

WORKDIR /app

# Prisma's query engine needs openssl on debian-slim.
RUN apt-get update -y && apt-get install -y --no-install-recommends openssl \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
RUN npm install

COPY prisma ./prisma
RUN npx prisma generate

COPY tsconfig.json tsconfig.build.json ./
COPY proto ./proto
COPY src ./src
RUN npm run build

# Default command; docker-compose overrides for the init service.
CMD ["node", "dist/main.js"]
