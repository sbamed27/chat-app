
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .

# PRODUCTION FIX 4: Security (Run as non-root user)
USER node 
EXPOSE 3000
CMD ["node", "server.js"]