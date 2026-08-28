const express = require('express')
const pool = require('./src/db/client')
const redisClient = require('./src/db/redis')
const app = express()

const PORT = process.env.PORT || 3000


app.get('/',  (req, res)=>{
    res.send('All Good')
})

app.post('/orders/id', async (req, res)=>{
    const {id} = req.params
})

app.listen(PORT, ()=>{
    console.log(`server is listening on port http://localhost:${PORT}`)
})