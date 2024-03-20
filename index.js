import express from "express";
import {createServer} from "http";
import { Server } from "socket.io";
import { Kafka } from "kafkajs";

import dotenv from 'dotenv';
dotenv.config();

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, {
    cors: {
      origin: process.env.FRONT,
      methods: ["GET", "POST"]
    }
  });

  app.get('/', (req, res) => {
    res.send("hello")
  });

const kafka = new Kafka({
    clientId: 'my-dashboard',
    brokers: [process.env.KAFKA_BROKER],
    sasl: {
      mechanism: "scram-sha-512",
      username: process.env.UPSTASH_KAFKA_USERNAME,
      password: process.env.UPSTASH_KAFKA_PASSWORD,
    },
    ssl:true,
  });

  const consumer = kafka.consumer({ groupId: 'test-group-1' });

  const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'mytopic', fromBeginning: true });
    await consumer.subscribe({ topic: 'ProductViewedCountsTopic', fromBeginning: true });
    await consumer.subscribe({ topic: 'ProductPurchasedCountsTopic', fromBeginning: true });
    await consumer.subscribe({ topic: 'ProductFavoritedCountsTopic', fromBeginning: true });
    
  
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const key = message.key ? message.key.toString() : null;
        const value = JSON.parse(message.value.toString());
        
        /*console.log({
            topic,
            key,
            value
        });*/

        io.emit('kafka-message', {
            topic,
            key,
            value
        });
      },
    });
  };

  run().catch(console.error);

io.on('connection', (socket) => {
  console.log('a user connected');
  socket.on('disconnect', () => {
    console.log('user disconnected');
  });
});

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});