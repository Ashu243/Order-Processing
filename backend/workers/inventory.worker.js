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
const GROUP_NAME = 'inventory-workers';
const consumerName = process.argv[2] || 'worker-1';

// Don't make this too small.
// If normal processing can take 2-3 seconds,
// 30 seconds gives you some breathing room.
const RECOVERY_IDLE_TIME = 30000;


// PROCESS MESSAGE

async function processMessage(message) {
    const messageID = message.id;
    const orderId = message.message.orderID;
    console.log(message)
    const event_id = `order-${orderId}`

    const client = await pool.connect();

    try {
        await client.query('BEGIN');

        // Check whether this event has already been processed
        const processedEventResult = await client.query(
            `SELECT 1
             FROM processed_events
             WHERE event_id = $1`,
            [event_id]
        );

        if (processedEventResult.rows.length > 0) {
            console.log('Already processed:', event_id);

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
        const orderDetails = await client.query(
            `select product_id, quantity from order_items where order_id = $1`, [orderId]
        );

        if (orderDetails.rows.length === 0) {
            console.log('Order not found:', orderId);

            await client.query('ROLLBACK');

            // Don't ACK.
            // Message will remain pending and can be retried.
            return;
        }
        let orderQuantity = orderDetails.rows[0].quantity
        let product_id = orderDetails.rows[0].product_id

        console.log("product_id: ", product_id)

        const totalQuantity = await client.query(
            `select quantity from inventory where product_id = $1`, [product_id]
        )

        if (totalQuantity.rows.length === 0) {
            console.log('Product not found:', product_id);

            await client.query('ROLLBACK');
            return;
        }

        const currentStock = totalQuantity.rows[0].quantity
        console.log(totalQuantity)

        if (currentStock - orderQuantity < 0) {
            console.log('Stock not available');

            await client.query('ROLLBACK');

            await redisClient.xAdd(
                'order',
                '*',
                {
                    event: 'inventory.failed',
                    orderID: orderId.toString()
                }
            );

            await redisClient.xAck(
                STREAM_NAME,
                GROUP_NAME,
                messageID
            );

            return;
        }

        await client.query('update inventory set quantity = quantity - $1 where product_id = $2', [orderQuantity, product_id])

        await redisClient.xAdd(
            'order',
            '*',
            {
                event: 'inventory.reserved',
                orderID: orderId.toString()
            }
        );



        // console.log(orderUpdate)

        // Record successful processing
        await client.query(
            `INSERT INTO processed_events (event_id)
             VALUES ($1)`,
            [event_id]
        );

        // await client.query('some randome bullshit')
        // DB work is now atomic
        await client.query('COMMIT');

        // await redisClient.hIncrBy('metrics', 'order_confirmed', 1)

        // await new Promise(resolve => setTimeout(resolve, 40000))
        // ACK only AFTER DB commit
        await redisClient.xAck(
            STREAM_NAME,
            GROUP_NAME,
            messageID
        );

        console.log(
            `inventory reserved for order: ${orderId}, event ${event_id}`
        );

    } catch (error) {

        try {
            await client.query('ROLLBACK');

            await client.query('BEGIN')

            await client.query(`INSERT INTO message_failures (
                event_id,
                retry_count,
                last_error,
                updated_at
                )
                VALUES ($1, 1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (event_id)
                DO UPDATE SET
                retry_count = message_failures.retry_count + 1,
                last_error = EXCLUDED.last_error,
                updated_at = CURRENT_TIMESTAMP;
                `, [event_id, error.message]) // If this event already exists, don't create another row. Update the existing row instead.

            const result = await client.query('select retry_count from message_failures where event_id = $1', [event_id])
            const retryCount = result.rows[0].retry_count

            await client.query("COMMIT")

            if (retryCount >= 3) {
                await redisClient.xAdd('order-dlq', '*', {
                    originalEventId: event_id,
                    orderID: message.message.orderID,
                    retryCount: retryCount.toString(),
                    error: error.message
                })

                await redisClient.xAck(
                    STREAM_NAME,
                    GROUP_NAME,
                    messageID
                )
            }
        } catch (rollbackError) {
            console.error(
                'Rollback failed:',
                rollbackError
            );
        }

        console.error(
            `Error processing message ${event_id}:`,
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


// NORMAL WORKER

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
                if (message.message.event !== 'order.created') {
                    await redisClient.xAck(
                        STREAM_NAME,
                        GROUP_NAME,
                        message.id
                    );
                    continue
                }
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


// RECOVER PENDING MESSAGES

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
            if (message.message.event !== 'order.created') {
                await redisClient.xAck(
                    STREAM_NAME,
                    GROUP_NAME,
                    message.id
                );
                continue;
            }
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


// START APPLICATION

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
            `Inventory Worker Started: ${consumerName}`
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
