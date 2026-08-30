const { createClient } = require('redis')

const redisClient = createClient({
    url: 'redis://localhost:6379'
})

redisClient.on('error', (error) => {
    console.error('Redis error:', error)
})

const consumerName = process.argv[2] || 'worker-1'

async function startWorker(){
    await redisClient.connect()
    console.log('Order Worker Started!!')


    // let lastID = '0' // if the worker crashes and started again it will return the order which have id greater than '0', so that is the limitation
    while (true){
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
        console.log(JSON.stringify(result, null, 2))

        await redisClient.xAck(
            'order', 
            'order-workers',
            messageID
        )
    }
}

async function recoverPendingMessages() {
    const pending = await redisClient.xPendingRange(
        'order',
        'order-workers',
        '-',
        '+',
        10
    )

    for (const message of pending){
        if (message.millisecondsSinceLastDelivery > 5000){
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