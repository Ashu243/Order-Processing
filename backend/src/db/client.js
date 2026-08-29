const { Pool } = require('pg');
require('dotenv').config();

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

module.exports = pool;