var http = require('http');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database("nyc.db");

db.exec(`PRAGMA synchronous = OFF;
        PRAGMA journal_mode = MEMORY;`);

module.exports = db;
