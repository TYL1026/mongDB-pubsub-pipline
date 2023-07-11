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
const pubSubClient = new PubSub();
const topic = pubSubClient.topic(process.env.PUB_SUB_TOPIC);
const configDirectory = path.resolve(process.cwd(), "config");
const file = fs.readFileSync(
    path.join(configDirectory, "chang-stream-schema.avsc"),
    "utf8"
  );
const definition = file.toString();
const type = avro.parse(definition);

const server = app.listen(port, () => {
    console.log(`Server Run on ${port} `)
})

const connectDb = async () => {
    try {
        mongodbClient = new MongoClient(process.env.CONNECTION_STRING);
        console.log("Database connected ");
        await monitorCollectionForInserts(mongodbClient, 'Uber_NYC', 'UberData');
    } catch (err) {
        console.log(err);
    }
}

async function monitorCollectionForInserts(client, databaseName, collectionName) {
    const collection = client.db(databaseName).collection(collectionName);
    // An aggregation pipeline that matches on new documents in the collection.
    const changeStream = collection.watch([], { fullDocument: 'updateLookup' });
    changeStream.on('change', event => {
        const document = event.fullDocument;
        if(event.operationType != 'delete'){
            console.log("New Row added")
            publishDocumentAsMessage(document );
        }else{
            console.log("Row deleted")
        }
       
    });
    await closeChangeStream(6000, changeStream);
 }
 
 function closeChangeStream(timeInMs, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log('Closing the change stream');
            changeStream.close();
            resolve();
        }, timeInMs)
    })
 };

 async function publishDocumentAsMessage(document) {

    const message = {
        id: JSON.stringify(document._id),
        pickup_datetime: document.pickup_datetime.getTime(),
        dropoff_datetime: document.dropoff_datetime.getTime(),
        pickup_lat:document.pickup_lat,
        pickup_long:document.pickup_long,
        dropoff_lat:document.dropoff_lat,
        dropoff_long:document.dropoff_long,
        Hvfhs_license_num: document.Hvfhs_license_num.toString(),
        density: document.density
    };
    const dataBuffer = Buffer.from(type.toString(message));
    try {
        const messageId = await topic.publishMessage({ data: dataBuffer });
        console.log(`Avro record ${messageId} published.`);
    } catch(error) {
        console.error(error);
    }
 }

 connectDb()