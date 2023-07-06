const express = require("express");
const app = express();
const {MongoClient} = require("mongodb");
const {PubSub} = require('@google-cloud/pubsub');
const fs = require('fs');
const avro = require('avro-js');
var path = require("path");
const dotenv = require("dotenv")
dotenv.config()
let mongodbClient;
const port = process.env.PORT;

const connectDb = async () => {
    try {
        mongodbClient = new MongoClient(process.env.CONNECTION_STRING);
        console.log("Database connected ");
        await monitorCollectionForInserts(mongodbClient, 'Uber_NYC', 'UberData');
    } catch (err) {
        console.log(err);
        process.exit(1);
    }
}
connectDb()

async function monitorCollectionForInserts(client, databaseName, collectionName) {
    const collection = client.db(databaseName).collection(collectionName);
    // An aggregation pipeline that matches on new documents in the collection.
    const pipeline = [ { $match: { operationType: 'update' } } ];
    const changeStream = collection.watch([], { fullDocument: 'updateLookup' });
    console.log("Pub/Sub connected");
    changeStream.on('change', event => {
        const document = event.fullDocument;
        publishDocumentAsMessage(document,  process.env.PUB_SUB_TOPIC);
    });
    // await closeChangeStream(60000, changeStream);
 }
  
//  function closeChangeStream(timeInMs, changeStream) {
//     return new Promise((resolve) => {
//         setTimeout(() => {
//             console.log('Closing the change stream');
//             changeStream.close();
//             resolve();
//         }, timeInMs)
//     })
//  };

 async function publishDocumentAsMessage(document, topicName) {
    const pubSubClient = new PubSub();
    const topic = pubSubClient.topic(topicName);
    const configDirectory = path.resolve(process.cwd(), "config");
    const file = fs.readFileSync(
        path.join(configDirectory, "chang-stream-schema.avsc"),
        "utf8"
      );
    const definition = file.toString();
    const type = avro.parse(definition);
    console.log(typeof document.pickup_datetime)
    const message = {
        id: JSON.stringify(document._id),
        pickup_datetime: document.pickup_datetime.getTime(),
        dropoff_datetime: document.dropoff_datetime.getTime(),
        pickup_lat:document.pickup_lat.toString(),
        pickup_lat:document.pickup_lat.toString(),
        pickup_long:document.pickup_long.toString(),
        dropoff_lat:document.dropoff_lat.toString(),
        dropoff_long:document.dropoff_long.toString(),
        Hvfhs_license_num: document.Hvfhs_license_num.toString(),
    };
    console.log(message)
    const dataBuffer = Buffer.from(type.toString(message));
    try {
        const messageId = await topic.publishMessage({ data: dataBuffer });
        console.log(`Avro record ${messageId} published.`);
    } catch(error) {
        console.error(error);
    }
 }
