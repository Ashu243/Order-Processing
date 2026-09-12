// 1 Read one msg from dlq
// 2 add that msg to the order
// 3 if successfully added that msg, remove that from dlq

const { createClient } = require('redis');


const redisClient = createClient({
    url: 'redis://localhost:6379'
});

redisClient.on('error', (error) => {
    console.error('Redis error:', error);
});

const event_id = process.argv[2]

async function process_failed_messages() {
    const messages = await redisClient.xRange('order-dlq', event_id, event_id)

    if (messages.length === 0) {
        console.log(`Event ${event_id} not found in DLQ`);
        return;
    }

    const message = messages[0];

    try {
        const newId = await redisClient.xAdd(
            'order',
            '*',
            {
                event: 'order.created',
                orderID: message.message.orderID
            }
        );

        console.log(`Requeued ${message.id} as ${newId}`)

        await redisClient.xDel('order-dlq', message.id)

        console.log(
            `Removed ${message.id} from DLQ`
        );
    } catch (error) {
        console.log('Error while Requeing the message', error)
    }

}


async function start() {
    try {
        await redisClient.connect();

        console.log(
            'Redis connected!'
        );

        // Start normal worker
        process_failed_messages();


    } catch (error) {
        console.error(
            'Failed to start worker:',
            error
        );

        process.exit(1);
    }
}

start();
