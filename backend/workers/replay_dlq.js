// 1 Read one msg from dlq
// 2 add that msg to the order
// 3 if successfully added that msg, remove that from dlq


const { Pool } = require('pg');
require('dotenv').config();
const { createClient } = require('redis');

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: 5433,
    max: 20,
});

const redisClient = createClient({
    url: 'redis://localhost:6379'
});

redisClient.on('error', (error) => {
    console.error('Redis error:', error);
});

const event_id = process.argv[2]

async function process_failed_messages(){
    const event = await redisClient.xRange('order-dlq', event_id, event_id)
    
}