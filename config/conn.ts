import mysql from 'mysql2/promise'

export const poolConn = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
})

export const poolConn2 = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME2,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
})

export const poolConn3 = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME3,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
})

export const poolConn4 = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME4,
});

export const poolConn5 = mysql.createPool({
    host: process.env.DB_HOST2,
    user: process.env.DB_USERNAME2,
    password: process.env.DB_PASSWORD2,
    database: process.env.DB_NAME5,
})