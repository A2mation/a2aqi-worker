import "dotenv/config";
import { Worker } from "bullmq";

import { prisma } from './libs/prisma'
import { connection } from "./libs/redis";
import { roundToDay } from "./utils/roundToDay";
import { roundToHour } from "./utils/roundToHour";
import { SensorJob } from "../types/sensor";
import { storeRawSensorReading } from "./services/db/sensorReadings";



const worker = new Worker(
    "sensor-queue",
    async (job) => {
        const data = job.data as SensorJob;

        const deviceId = data.deviceId;
        const timestamp = new Date(data.measuredAt);

        await storeRawSensorReading(data);

        const hourStart = roundToHour(timestamp);
        const dayStart = roundToDay(timestamp);


        const incFields: Record<string, number> = { count: 1 }
        const minFields: Record<string, number> = {};
        const maxFields: Record<string, number> = {};

        function addField(value: number | undefined, key: string) {
            if (value === undefined) {
                return;
            }
            incFields[`sum${key}`] = Math.round(value);
            minFields[`min${key}`] = Math.round(value);
            maxFields[`max${key}`] = Math.round(value);
        }

        for (const [key, value] of Object.entries(data)) {
            if (
                value === undefined ||
                key === "deviceId" ||
                key === "measuredAt"
            ) continue;

            if (typeof value !== "number") continue;

            const formattedKey =
                key.charAt(0).toUpperCase() + key.slice(1);

            addField(value, formattedKey);
        }
        
        // ----------------------------- 
        //      HOURLY Aggregation 
        // -----------------------------

        await prisma.$runCommandRaw({
            update: "HourlyAggregateReading",
            updates: [
                {
                    q: {
                        deviceId: { $oid: deviceId },
                        hourStart: { $date: hourStart.toISOString() }
                    },
                    u: {
                        $setOnInsert: {
                            deviceId: { $oid: deviceId },
                            hourStart: { $date: hourStart.toISOString() },
                            createdAt: { $date: new Date().toISOString() },
                        },
                        $inc: incFields,
                        ...(Object.keys(minFields).length > 0 ? {
                            $min: minFields
                        } : {}),

                        ...(Object.keys(maxFields).length > 0 ? {
                            $max: maxFields
                        } : {}),
                    },
                    upsert: true,
                },
            ]
        });


        // -----------------------------
        // DAILY Aggregation
        // -----------------------------

        await prisma.$runCommandRaw({
            update: "DailyAggregateReading",
            updates: [
                {
                    q: {
                        deviceId: { $oid: deviceId },
                        dayStart: { $date: dayStart.toISOString() },
                    },
                    u: {
                        $setOnInsert: {
                            deviceId: { $oid: deviceId },
                            dayStart: { $date: dayStart.toISOString() },
                            createdAt: { $date: new Date().toISOString() },
                        },
                        $inc: incFields,
                        ...(Object.keys(minFields).length > 0 ? { $min: minFields } : {}),
                        ...(Object.keys(maxFields).length > 0 ? { $max: maxFields } : {}),
                    },
                    upsert: true,
                },
            ],
        });



        return { ok: true };
    },
    {
        connection,
        concurrency: 20,
    }
);

worker.on("completed", (job) => {
    console.log(`✅ Job completed: ${job.id}`);
});

process.on("SIGTERM", async () => {
    console.log("SIGTERM received...");
    await worker.close();
    process.exit(0);
});

worker.on("failed", (job, err) => {
    console.error(`❌ Job failed: ${job?.id}`, err);
});

console.log("🚀 Worker running (Hourly + Daily Aggregation in 1 call)...");
