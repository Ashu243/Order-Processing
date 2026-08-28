const {Pool} = require('pg')
require('dotenv').config()

try {
    const pool = new Pool({
        user: process.env.DB_USER,
        host: process.env.DB_HOST,
        database: process.env.DB_NAME,
        password: process.env.DB_PASSWORD,
        port: 5433,
        max: 20, // max clients in pool
    })

    console.log('Postgres connected!')
    module.exports = pool
} catch (error) {
    console.log('Database Error', error)
}

