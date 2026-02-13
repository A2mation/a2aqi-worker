import "dotenv/config";
import { Worker } from "bullmq";

import { prisma } from './libs/prisma'
import { connection } from "./libs/redis";
import { roundToDay } from "./utils/roundToDay";
import { roundToHour } from "./utils/roundToHour";


type SensorJob = {
    deviceId: string;
    timestamp: string;

    aqi: number;
    pm25?: number;
    pm10?: number;
    no2?: number;
    so2?: number;
    o3?: number;
    co?: number;

    temperature?: number;
    humidity?: number;
};

const worker = new Worker("sensor-queue", async (job) => {  
    
 }, {
    connection,
});

worker.on("completed", (job) => {
    console.log(`✅ Job completed: ${job.id}`);
});

worker.on("failed", (job, err) => {
    console.error(`❌ Job failed: ${job?.id}`, err);
});

console.log("🚀 Worker running (Hourly + Daily Aggregation in 1 call)...");
