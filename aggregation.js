// load libraries
const { MongoClient } = require('mongodb');

// connection string
const MONGO_URL = 'mongodb://localhost:27017';
const MONGO_DB = 'airbnb';
const MONGO_COLLECTION = 'listingsAndReviews';


// mongodbClient
const mongoClient = new MongoClient(MONGO_URL, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

// function
const avgCleanliness = async(propertyType, mongoClient) => {
    const results = await mongoClient.db(MONGO_DB)
            .collection(MONGO_COLLECTION)
            .aggregate([
                {
                    $match: {
                        property_type: propertyType 
                    }
                },
                {
                    $group: {
                        _id: '$address.country',
                        count: { $sum: 1 },
                        cleanliness: {
                            $push: '$review_scores.review_scores_cleanliness'
                        }
                    }
                },
                {
                    $project: {
                        count: 1,
                        avg_cleanliness: {
                            $avg: '$cleanliness'
                        }
                    }
                },
                {
                    $sort: {
                        avg_cleanliness: 1
                    }
                }
            ])
            .toArray();

    // list the results
    console.log('Result: ', results);

    // stop the client
    mongoClient.close();
} 

mongoClient.connect()
    .then(() => {
        avgCleanliness('Apartment', mongoClient);
    })