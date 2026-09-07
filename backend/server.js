const express = require('express')
const pool = require('./src/db/client')
const redisClient = require('./src/db/redis')
const app = express()

const PORT = process.env.PORT || 3000


app.get('/', (req, res) => {
    res.send('All Good')
})

app.post('/orders/:id', async (req, res) => {
    const { id } = req.params
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
            `, [order.id, id, 1, price])
        
        // 
        await client.query(`insert into outbox_orders (order_id) values ($1)`, [order.id])
        
        await client.query('COMMIT')

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
    }
    finally {
        client.release()
    }
})

app.listen(PORT, () => {
    console.log(`server is listening on port http://localhost:${PORT}`)
})