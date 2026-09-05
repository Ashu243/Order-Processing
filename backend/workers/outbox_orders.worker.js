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


async function publishEvents() {
    try {
        const records = await pool.query(`
            SELECT *
            FROM outbox_orders
            WHERE status = 'TO_BE_PUBLISHED'
        `);

        if (records.rows.length === 0) {
            console.log('No orders to publish');
            return;
        }

        const ids = [];

        for (const msg of records.rows) {
            await redisClient.xAdd(
                'order',
                '*',
                {
                    event: 'order.created',
                    orderID: msg.order_id.toString()
                }
            );

            ids.push(msg.id);
        }

        await pool.query(
            `
            UPDATE outbox_orders
            SET status = 'PUBLISHED'
            WHERE id = ANY($1::int[])
            `,
            [ids]
        );

        console.log(`Published ${ids.length} event(s)`);

    } catch (error) {
        console.error('Outbox publisher error:', error);
    }
}



async function start() {
    try {
        await pool.query('SELECT 1');

        console.log(
            'Postgres actually connected!'
        );

        await redisClient.connect();

        console.log(
            'Redis connected!'
        );

        setInterval(publishEvents, 5000)

    } catch (error) {
        console.error(
            'Failed to start worker:',
            error
        );

        process.exit(1);
    }
}

start();