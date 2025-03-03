const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const databasesFilePath = path.join(__dirname, "databases.json");

const databaseNames = JSON.parse(fs.readFileSync(databasesFilePath, "utf8"));

const getDatabaseNames = async () => {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS
  });

  const [rows] = await connection.query("SHOW DATABASES LIKE 'db_practice_%';");
  connection.end();

  return rows.map(row => Object.values(row)[0]); 
};

const getDatabaseConnection = async (dbName) => {
  return mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: dbName,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
  });
};

module.exports = { getDatabaseConnection, databaseNames, getDatabaseNames };
