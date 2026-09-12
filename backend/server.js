const express = require('express')
const pool = require('./src/db/client')
const redisClient = require('./src/db/redis')
const app = express()

const PORT = process.env.PORT || 3000

app.use(express.json())
app.use(express.urlencoded())

app.get('/', (req, res) => {
    res.send('All Good')
})

app.get('/metrics', async(req, res)=>{
    // const response1 = await redisClient.hSet('metrics', {
    //     'order_created': 0,
    //     'order_confirmed': 0,
    //     'messages_failed': 0 
    // })
    // // console.log(response1)
    // return res.json({
    //     result: response1
    // })

    const result = await redisClient.hGetAll('metrics')
    const pendingMessages = await redisClient.xPending('order', 'order-workers')
    const dlq_messages = await redisClient.XLEN('order-dlq')
    return res.json({
        result,
        pendingMessages: pendingMessages.pending,
        dlq_messages
    })
})

app.get('/health', async (req, res) => {
    let isPostgreUp = 'down'
    let isRedisUp = 'down'
    try {
        await pool.query('SELECT 1')
        isPostgreUp = 'up'
    } catch (error) {
        isPostgreUp = 'down'
    }
    try {
        const redisResult = await redisClient.ping()

        if (redisResult === 'PONG') {
            isRedisUp = 'up'
        }
    } catch (error) {
        isRedisUp = 'down'
    }
    let status = 'unhealthy'
    let statuscode = 503

    if (isPostgreUp === 'up' && isRedisUp === 'up') {
        status = 'healthy'
        statuscode = 200
    }

    return res.status(statuscode)
    .json({
        status,
        services: {
            isPostgreUp,
            isRedisUp
        }
    })
})

app.post('/orders/:id', async (req, res) => {
    const { id } = req.params
    const {quantity} = req.body
    const user_id = 1
    const client = await pool.connect()
    try {
        await client.query('BEGIN')
        // Get Product
        const productResult = await client.query(`select price from products where id = $1`, [id])

        if (productResult.rows.length == 0) {
            await client.query('ROLLBACK')
            return res.status(404).json({
                message: 'Product not found'
            })
        }

        const price = productResult.rows[0].price
        const query = `insert into orders (user_id, total_amount)
        values
        ($1, $2)
        returning *
        `

        // create order
        const orderResult = await client.query(query, [user_id, price])
        const order = orderResult.rows[0]

        // create order item
        await client.query(`insert into order_items (order_id, product_id, quantity, price)
            values ($1, $2, $3, $4)
            `, [order.id, id, quantity, price])

        // 
        await client.query(`insert into outbox_orders (order_id) values ($1)`, [order.id])

        await client.query('COMMIT')
        await redisClient.hIncrBy('metrics', 'order_created', 1)
        // add the order to redis streams
        // redisClient.xAdd('order', '*', {event: 'order.created', orderID: order.id.toString()})

        return res.status(201).json({
            message: 'Order Created',
            order
        })

    } catch (error) {
        await client.query('ROLLBACK')
        console.error(error);

        res.status(500).json({
            message: 'Failed to create order'
        });

        await redisClient.hIncrBy('metrics', 'messages_failed', 1)
    }
    finally {
        client.release()
    }
})

app.listen(PORT, () => {
    console.log(`server is listening on port http://localhost:${PORT}`)
})