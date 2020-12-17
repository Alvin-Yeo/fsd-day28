// load libraries
const express = require('express');
const morgan = require('morgan');
const { MongoClient } = require('mongodb');
const mysql = require('mysql2/promise');


// environment configuration
require('dotenv').config();
const PORT = parseInt(process.argv[2]) || parseInt(process.env.PORT) || 3000;


/* MySQL Configuration */
const pool = mysql.createPool({
    host: process.env.MYSQL_SERVER,
    port: process.env.MYSQL_SERVER_PORT,
    user: process.env.MYSQL_USERNAME,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    connectionLimit: process.env.MYSQL_CONN_LIMIT
});

const mkQuery = (sql, pool) => {
    return (async(args) => {
        const conn = await pool.getConnection();

        try {
            const results = await conn.query(sql, args || []);
            return results[0];
        } catch(err) {
            console.error(`[ERROR] Failed to execute sql query.`);
            console.error(`[ERROR] Error message: `, err);
            return err;
        } finally {
            conn.release();
        }
    })
}

// sql statements
const SQL_GET_GAME_BY_ID = 'select name, year, url, image from game where gid = ?';

// sql functions
const getGameById = mkQuery(SQL_GET_GAME_BY_ID, pool);


/* MongoDB Configuration */
const MONGO_URL = 'mongodb://localhost:27017';
const MONGO_DATABASE = 'bgg';
const MONGO_COLLECTION = 'reviews';

const mongoClient = new MongoClient(MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});


// create an instance of express
const app = express();

/* resources */

// logging all requests with morgan
app.use(morgan('combined'));

// GET /game/:id
app.get('/game/:id', async(req, res) => {
    const id = parseInt(req.params['id']);

    const sqlResponse = await getGameById([ id ]);

    if(sqlResponse.length > 0) {
        const mongoResponse = await mongoClient.db(MONGO_DATABASE)
            .collection(MONGO_COLLECTION)
            .aggregate([
                {
                    $match: {
                        ID: id
                    }
                },
                {
                    $group: {
                        _id: '$ID',
                        comment_ids: {
                            $push: '$_id'
                        },
                        avg_rating: {
                            $avg: '$rating'
                        }
                    }
                }
            ])
            .toArray();

        res.status(200);
        res.type('application/json');
        res.json({ 
            name: sqlResponse[0]['name'],
            year: sqlResponse[0]['year'],
            url: sqlResponse[0]['url'],
            image: sqlResponse[0]['image'],
            reviews: mongoResponse[0]['comment_ids'],
            'average_rating': mongoResponse[0]['avg_rating']
         });
    } else {
        res.status(404);
        res.type('application/json');
        res.json({ status: 'Error 404. No record found.' });
    }
})


// start server
const startApp = (app, pool, mongoClient) => {
    const p0 = (async() => {
        const conn = await pool.getConnection();
        
        console.info(`[INFO] Pinging database...`);
        await conn.ping();
        
        console.info(`[INFO] Ping database successfully.`);
        conn.release();
        
        return true;
    })();
    
    const p1 = (async() => {
        await mongoClient.connect();
        return true;
    })();

    Promise.all([ p0, p1 ])
        .then((result) => {
            app.listen(PORT, () => {
                console.info(`[INFO] Server started on port ${PORT} at ${new Date()}`);
            })
        })
        .catch((err) => {
            console.error(`[ERROR] Failed to start server.`);
            console.error(`[ERROR] Error message: `, err);
        })
}
startApp(app, pool, mongoClient);