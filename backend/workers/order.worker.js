const { Pool } = require('pg');
require('dotenv').config();
const { createClient } = require('redis')

const pool = new Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD,
    port: 5433,
    max: 20,
});

pool.connect()
    .then((client) => {
        console.log('Postgres actually connected!');
        client.release();
    })
    .catch((error) => {
        console.error('Database connection error:', error);
    });

const redisClient = createClient({
    url: 'redis://localhost:6379'
})

redisClient.on('error', (error) => {
    console.error('Redis error:', error)
})

const consumerName = process.argv[2] || 'worker-1'

async function startWorker() {
    await redisClient.connect()
    console.log('Order Worker Started!!')


    // let lastID = '0' // if the worker crashes and started again it will return the order which have id greater than '0', so that is the limitation
    while (true) {
        const result = await redisClient.xReadGroup(
            'order-workers',
            consumerName,
            {
                key: 'order',
                id: '>' // Give me new messages that have never been delivered to another consumer in this group.
            },
            {
                COUNT: 1,
                BLOCK: 0
            }
        ) // "Read messages from the orders stream, starting from ID lastID. Give me up to 1 message, and if there isn't one, wait until a message arrives."
        
        const messageID = result[0].messages[0].id
        
        const processedEventResult = await client.query(`select 1 from processed_events
            where event_id = $1`, [messageID])
            
            if (processedEventResult.rows.length > 0) {
                console.log('Already Processed', messageID)
                await redisClient.xAck(
                    'order',
                    'order-workers',
                    messageID
                )
                continue
            }
            const client = await pool.connect()
            try {
            const orderId = result[0].messages[0].message.orderId

            await client.query('BEGIN')

            const orderUpdate = await client.query(`update orders set status = 'CONFIRMED' where id = $1`, [orderId])

            if (orderUpdate.rows.length === 0) {
                console.log('Order not found:', orderId)
                await client.query('ROLLBACK')
                continue
            }

            await client.query(`insert into processed_events (event_id)
                values ($1)`, [messageID])
            console.log(JSON.stringify(result, null, 2))

            await client.query('COMMIT')

            await redisClient.xAck(
                'order',
                'order-workers',
                messageID
            )

        } catch (error) {
            await client.query('ROLLBACK')
            console.log('Error while processing orders', error)
        }
        finally {
            await client.release()
        }
    }
}

// recover the data if the idle time of data is more than our expected time
async function recoverPendingMessages() {
    const pending = await redisClient.xPendingRange(
        'order',
        'order-workers',
        '-',
        '+',
        10
    )

    for (const message of pending) {
        if (message.millisecondsSinceLastDelivery > 5000) {
            const claimed = await redisClient.xClaim(
                'order',
                'order-workers',
                consumerName,
                5000,
                [message.id]
            )

            console.log('Recovered:', claimed)
        }
    }
}

startWorker()

setInterval(() => {
    recoverPendingMessages()
}, 5000);