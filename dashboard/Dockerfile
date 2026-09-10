# Multi-stage build: compile the Vite/React/TS app, then serve the static
# bundle with a tiny production static file server.
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .

# Baked in at build time. Override with --build-arg for a non-default backend
# URL. For local docker compose, the browser talks to the backend at
# http://localhost:8000 (published port), not the in-network service name.
ARG VITE_API_BASE_URL=http://localhost:8000
ENV VITE_API_BASE_URL=$VITE_API_BASE_URL

RUN npm run build

FROM node:22-alpine AS serve
WORKDIR /app
RUN npm install -g serve@14
COPY --from=build /app/dist ./dist
EXPOSE 4173
CMD ["serve", "-s", "dist", "-l", "4173"]
