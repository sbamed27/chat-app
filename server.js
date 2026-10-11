import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { createClient } from 'redis';
import { createAdapter } from '@socket.io/redis-adapter';
import pkg from 'pg';
const { Pool } = pkg;
import os from 'os';

const app = express();
const httpServer = createServer(app);

// CORS configuration
const allowedOrigins = process.env.ALLOWED_ORIGINS
    ? process.env.ALLOWED_ORIGINS.split(',')
    : ['http://localhost:3000'];

const io = new Server(httpServer, {
    cors: {
        origin: allowedOrigins,
        methods: ["GET", "POST"]
    },
    transports: ["websocket"]
});

// PostgreSQL Pool Connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

// Initialize Database Table
async function initDB() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS messages (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('PostgreSQL: users and messages tables verified/created');
    } catch (err) {
        console.error('Database initialization error:', err);
    }
}
initDB();

// Redis Setup
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

io.on('connection', async (socket) => {
    console.log(`[Replica Host: ${os.hostname()}] Connected: ${socket.id}`);

    // Fetch chat history joining users and messages tables
    try {
        const { rows } = await pool.query(`
            SELECT u.username AS sender, m.content 
            FROM messages m 
            JOIN users u ON m.user_id = u.id 
            ORDER BY m.created_at ASC 
            LIMIT 50
        `);
        socket.emit('init_history', rows);
    } catch (err) {
        console.error('Error fetching history:', err);
    }

    socket.on('chat_message', async ({ sender, content }) => {
        try {
            // Upsert user: insert if new, or return existing id if username already exists
            const userResult = await pool.query(`
                INSERT INTO users (username) 
                VALUES ($1) 
                ON CONFLICT (username) 
                DO UPDATE SET username = EXCLUDED.username 
                RETURNING id
            `, [sender]);

            const userId = userResult.rows[0].id;

            // Insert message linked via foreign key user_id
            await pool.query(`
                INSERT INTO messages (user_id, content) 
                VALUES ($1, $2)
            `, [userId, content]);

            // Broadcast to other clients across replicas
            socket.broadcast.emit('chat_message', { sender, content });
        } catch (err) {
            console.error('Error saving message:', err);
        }
    });
});

const PORT = process.env.PORT || 3000;
const server = httpServer.listen(PORT, () => console.log(`Server running on port ${PORT}`));

process.on('SIGTERM', () => {
    server.close(async () => {
        await pool.end();
        await Promise.all([pubClient.quit(), subClient.quit()]);
        process.exit(0);
    });
});