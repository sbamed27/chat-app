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
                recipient_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);
        console.log('PostgreSQL: users and messages tables verified/created with DM support');
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

// Helper to create a consistent, unique room name using immutable User IDs
function getRoomName(id1, id2) {
    const sortedIds = [parseInt(id1), parseInt(id2)].sort((a, b) => a - b);
    return `dm_${sortedIds[0]}_${sortedIds[1]}`;
}

io.on('connection', async (socket) => {
    console.log(`[Replica Host: ${os.hostname()}] Connected: ${socket.id}`);

    // 1. Register or get user upon connection
    socket.on('register_user', async (username, callback) => {
        try {
            socket.username = username;

            // Upsert user and cache id on the socket instance
            const userResult = await pool.query(`
                INSERT INTO users (username) 
                VALUES ($1) 
                ON CONFLICT (username) 
                DO UPDATE SET username = EXCLUDED.username 
                RETURNING id
            `, [username]);

            socket.userId = userResult.rows[0].id;

            // Send back list of all registered users (except self)
            const { rows: users } = await pool.query('SELECT username FROM users WHERE username != $1', [username]);
            callback({ success: true, users: users.map(u => u.username) });
        } catch (err) {
            console.error('Registration error:', err);
            callback({ success: false });
        }
    });

    // 2. Join a private chat room using immutable IDs
    socket.on('join_private_chat', async ({ recipient }, callback) => {
        try {
            // Get recipient ID from database
            const recipientRes = await pool.query('SELECT id FROM users WHERE username = $1', [recipient]);
            if (recipientRes.rows.length === 0) return callback({ success: false });
            const recipientId = recipientRes.rows[0].id;

            // Generate stable room name using IDs
            const roomName = getRoomName(socket.userId, recipientId);
            socket.join(roomName);

            // Fetch chat history between the two IDs
            const { rows } = await pool.query(`
                SELECT u.username AS sender, m.content 
                FROM messages m 
                JOIN users u ON m.user_id = u.id 
                WHERE (m.user_id = $1 AND m.recipient_id = $2) 
                   OR (m.user_id = $2 AND m.recipient_id = $1)
                ORDER BY m.created_at ASC 
                LIMIT 50
            `, [socket.userId, recipientId]);

            callback({ success: true, history: rows });
        } catch (err) {
            console.error('Error joining private chat:', err);
            callback({ success: false });
        }
    });

    // 3. Handle incoming private messages using IDs
    socket.on('private_message', async ({ recipient, content }) => {
        try {
            const recipientRes = await pool.query('SELECT id FROM users WHERE username = $1', [recipient]);
            if (recipientRes.rows.length === 0) return;
            const recipientId = recipientRes.rows[0].id;

            // Save to PostgreSQL with foreign key IDs
            await pool.query(`
                INSERT INTO messages (user_id, recipient_id, content) 
                VALUES ($1, $2, $3)
            `, [socket.userId, recipientId, content]);

            const roomName = getRoomName(socket.userId, recipientId);

            // Broadcast to the ID-based room across replicas via Redis
            socket.to(roomName).emit('private_message', {
                sender: socket.username,
                content
            });
        } catch (err) {
            console.error('Error saving private message:', err);
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