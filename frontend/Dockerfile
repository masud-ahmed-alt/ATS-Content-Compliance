# ------------------------------------------------------
# Stage 1: Base Node setup (used for both dev & build)
# ------------------------------------------------------
FROM node:20-alpine AS base
WORKDIR /app
COPY package*.json ./
RUN npm install

# ------------------------------------------------------
# Stage 2: Development mode (with Vite hot reload)
# ------------------------------------------------------
FROM base AS dev
ENV NODE_ENV=development
COPY . .
EXPOSE 5173
ENV CHOKIDAR_USEPOLLING=true
CMD ["npm", "run", "dev", "--", "--host"]

# ------------------------------------------------------
# Stage 3: Production build
# ------------------------------------------------------
FROM base AS build
ENV NODE_ENV=production
COPY . .
RUN npm run build

# ------------------------------------------------------
# Stage 4: Serve production build with Nginx
# ------------------------------------------------------
FROM nginx:alpine AS prod
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
