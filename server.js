import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { createClient } from 'redis';
import { createAdapter } from '@socket.io/redis-adapter';

const app = express();
const httpServer = createServer(app);

// PRODUCTION FIX 1: Strict CORS policy via Environment Variables
const allowedOrigins = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(',')
    : ['http://localhost:3000']; // Fallback for local testing

const io = new Server(httpServer, {
    cors: {
        origin: allowedOrigins,
        methods: ["GET", "POST"]
    },
    transports: ["websocket"]
});

// PRODUCTION FIX 2: Explicit Error Handling for Redis
const redisUrl = process.env.REDIS_URL;
if (!redisUrl) throw new Error("FATAL: REDIS_URL environment variable is missing.");

const pubClient = createClient({ url: redisUrl });
const subClient = pubClient.duplicate();

pubClient.on('error', (err) => console.error('Redis Pub Error:', err));
subClient.on('error', (err) => console.error('Redis Sub Error:', err));

Promise.all([pubClient.connect(), subClient.connect()]).then(() => {
    io.adapter(createAdapter(pubClient, subClient));
    console.log('Redis Pub/Sub adapter connected');
});

io.on('connection', (socket) => {
    socket.on('chat_message', (msg) => {
        io.emit('chat_message', msg);
    });
});

const PORT = process.env.PORT || 3000;
const server = httpServer.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// PRODUCTION FIX 3: Graceful Shutdown
// When EasyPanel sends a redeploy command, this safely closes active connections
process.on('SIGTERM', () => {
    console.log('SIGTERM signal received: closing HTTP server');
    server.close(() => {
        console.log('HTTP server closed');
        Promise.all([pubClient.quit(), subClient.quit()]).then(() => {
            console.log('Redis clients disconnected');
            process.exit(0);
        });
    });
});