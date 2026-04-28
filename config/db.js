const mysql = require("mysql2");

const db = mysql.createPool({
  host: process.env.DB_HOST || "127.0.0.1",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "#Shiv@2002",
  database: process.env.DB_NAME || "fleet",
  waitForConnections: true,
  connectionLimit: Number(process.env.DB_CONNECTION_LIMIT || 20),
  queueLimit: Number(process.env.DB_QUEUE_LIMIT || 0),
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

module.exports = db;
