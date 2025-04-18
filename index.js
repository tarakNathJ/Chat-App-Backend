import express from 'express';
import { WebSocketServer } from 'ws';
import { createClient } from "redis";
import { produseMessage ,ConsumeMessages } from './Kafka/ProduserAndConsumer.js';

const app = express();
const httpServer = app.listen(8080);

const WSS = new WebSocketServer({ server: httpServer });
const Users = {};

const redisClient = createClient({
    url: 'redis://localhost:6379' // Adjust if your Redis is elsewhere
});

const redisSubscriber = redisClient.duplicate(); // Separate client for subscribing

redisClient.on('error', err => console.log('Redis Publisher Error', err));
redisClient.on('connect', () => console.log('Connected to Redis (Publisher)'));
redisClient.connect().catch(console.error);

redisSubscriber.on('error', err => console.log('Redis Subscriber Error', err));
redisSubscriber.on('connect', () => console.log('Connected to Redis (Subscriber)'));
redisSubscriber.connect().catch(console.error);

const subscribeToMessages = async () => {
    await redisSubscriber.subscribe("MESSAGE", async(message) => {
        try {
            const data = JSON.parse(message);
            const { roomId, userId, message: msg } = data;

            for (const wsID in Users) {
                const user = Users[wsID];
                if (user.roomId === roomId && user.userId !== userId) {
                    user.ws.send(JSON.stringify({ type: "message", payload: { message: msg, roomId, userId } }));
                }
            }
            await produseMessage({ message: msg, roomId, userId});
        } catch (error) {
            console.error("Error processing Redis message:", error);
        }
    });
};

subscribeToMessages();
ConsumeMessages();

let counter = 0;

WSS.on('connection', (ws) => {
    const wsID = counter++;

    ws.on("message", async (message) => {
        try {
            const data = JSON.parse(message.toString());

            switch (data.type) {
                case "join":
                    Users[wsID] = {
                        roomId: data.payload.roomId,
                        userId: data.payload.userId,
                        ws
                    };
                    ws.send(JSON.stringify({ type: "joined", payload: { roomId: data.payload.roomId } }));
                    break;

                case "message":
                    const { roomId, userId, message: msg } = data.payload;
                    console.log({ roomId, userId, message: msg });

                    if (redisClient.isReady) {
                        await redisClient.publish("MESSAGE", JSON.stringify({ roomId, userId, message: msg }));
                    } else {
                        console.error('Redis publisher client is not ready.');
                        ws.send(JSON.stringify({ type: "error", payload: { message: "Failed to send message due to Redis connection issue." } }));
                    }
                    break;

                case "leave":
                    const { roomId: leaveRoomId } = data.payload;
                    delete Users[wsID];
                    ws.send(JSON.stringify({ type: "left", payload: { roomId: leaveRoomId } }));
                    break;

                case "ping":
                    ws.send(JSON.stringify({ type: "pong", payload: {} }));
                    break;

                case "close":
                    ws.close();
                    delete Users[wsID]; // Clean up user on close
                    break;

                case "error":
                    ws.send(JSON.stringify({ type: "error", payload: { message: "Error" } }));
                    break;

                default:
                    ws.send(JSON.stringify({ type: "error", payload: { message: "Unknown message type" } }));
                    break;
            }
        } catch (error) {
            console.error("Error processing WebSocket message:", error);
            ws.send(JSON.stringify({ type: "error", payload: { message: "Failed to process message." } }));
        }
    });

    ws.on('close', () => {
        console.log(`WebSocket client ${wsID} disconnected.`);
        delete Users[wsID]; // Ensure user is removed on disconnect
    });

    ws.on('error', (error) => {
        console.error(`WebSocket error for client ${wsID}:`, error);
        delete Users[wsID]; // Clean up on error as well
    });
});

console.log('WebSocket server started on port 8080');