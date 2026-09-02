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

const STREAM_NAME = 'order';
const GROUP_NAME = 'order-workers';
const consumerName = process.argv[2] || 'worker-1';

// Don't make this too small.
// If normal processing can take 2-3 seconds,
// 30 seconds gives you some breathing room.
const RECOVERY_IDLE_TIME = 30000;


// --------------------------------------------------
// PROCESS MESSAGE
// --------------------------------------------------

async function processMessage(message) {
    const messageID = message.id;
    const orderId = message.message.orderId;

    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Check whether this event has already been processed
        const processedEventResult = await client.query(
            `SELECT 1
             FROM processed_events
             WHERE event_id = $1`,
            [messageID]
        );

        if (processedEventResult.rows.length > 0) {
            console.log('Already processed:', messageID);

            await client.query('ROLLBACK');

            // Safe to ACK because DB says this event
            // was already successfully processed.
            await redisClient.xAck(
                STREAM_NAME,
                GROUP_NAME,
                messageID
            );

            return;
        }

        // Process the order
        const orderUpdate = await client.query(
            `UPDATE orders
             SET status = 'CONFIRMED'
             WHERE id = $1`,
            [orderId]
        );

        if (orderUpdate.rows.length === 0) {
            console.log('Order not found:', orderId);

            await client.query('ROLLBACK');

            // Don't ACK.
            // Message will remain pending and can be retried.
            return;
        }

        // Record successful processing
        await client.query(
            `INSERT INTO processed_events (event_id)
             VALUES ($1)`,
            [messageID]
        );

        // DB work is now atomic
        await client.query('COMMIT');

        // ACK only AFTER DB commit
        await redisClient.xAck(
            STREAM_NAME,
            GROUP_NAME,
            messageID
        );

        console.log(
            `Processed order ${orderId}, event ${messageID}`
        );

    } catch (error) {

        try {
            await client.query('ROLLBACK');
        } catch (rollbackError) {
            console.error(
                'Rollback failed:',
                rollbackError
            );
        }

        console.error(
            `Error processing message ${messageID}:`,
            error
        );

        // IMPORTANT:
        // Don't ACK here.
        //
        // The message stays pending.
        // Recovery can claim it later.

    } finally {
        client.release();
    }
}


// --------------------------------------------------
// NORMAL WORKER
// --------------------------------------------------

async function startWorker() {
    console.log(`Starting worker: ${consumerName}`);

    while (true) {
        try {
            const result = await redisClient.xReadGroup(
                GROUP_NAME,
                consumerName,
                {
                    key: STREAM_NAME,
                    id: '>'
                },
                {
                    COUNT: 1,
                    BLOCK: 5000 // if no message after 5 sec then return null
                }
            );

            // BLOCK can return null when it times out.
            if (!result) {
                continue;
            }

            const messages = result[0]?.messages || [];

            for (const message of messages) {
                await processMessage(message);
            }

        } catch (error) {
            console.error(
                'Worker error:',
                error
            );

            // Don't kill the worker because of
            // a temporary Redis/DB error.
            await new Promise(resolve =>
                setTimeout(resolve, 1000)
            );
        }
    }
}


// --------------------------------------------------
// RECOVER PENDING MESSAGES
// --------------------------------------------------

async function recoverPendingMessages() {
    try {

        const result = await redisClient.xAutoClaim(
            STREAM_NAME,
            GROUP_NAME,
            consumerName,
            RECOVERY_IDLE_TIME,
            '0-0',
            {
                COUNT: 10
            }
        );

        const claimedMessages = result.messages;

        if (claimedMessages.length === 0) {
            return;
        }

        console.log(
            `Recovered ${claimedMessages.length} message(s)`
        );

        for (const message of claimedMessages) {
            console.log(
                'Processing recovered message:',
                message.id
            );

            await processMessage(message);
        }

    } catch (error) {
        console.error(
            'Recovery error:',
            error
        );
    }
}


// --------------------------------------------------
// START APPLICATION
// --------------------------------------------------

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

        console.log(
            `Order Worker Started: ${consumerName}`
        );

        // Start normal worker
        startWorker();

        // Recovery loop
        setInterval(
            recoverPendingMessages,
            10000
        );

    } catch (error) {
        console.error(
            'Failed to start worker:',
            error
        );

        process.exit(1);
    }
}

start();
