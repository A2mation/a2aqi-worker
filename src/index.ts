import "dotenv/config";
import { Worker } from "bullmq";

import { prisma } from './libs/prisma'
import { connection } from "./libs/redis";
import { roundToDay } from "./utils/roundToDay";
import { roundToHour } from "./utils/roundToHour";
import { SensorJob } from "../types/sensor";



const worker = new Worker(
    "sensor-queue",
    async (job) => {
        const data = job.data as SensorJob;

        const deviceId = data.deviceId;
        const timestamp = new Date(data.measuredAt);

        const hourStart = roundToHour(timestamp);
        const dayStart = roundToDay(timestamp);


        const incFields: Record<string, number> = { count: 1 }
        const minFields: Record<string, number> = {};
        const maxFields: Record<string, number> = {};

        function addField(value: number | undefined, key: string) {
            if (value === undefined) {
                return;
            }
            incFields[`sum${key}`] = value;
            minFields[`min${key}`] = value;
            maxFields[`max${key}`] = value;
        }

        addField(data.aqi, "Aqi");
        addField(data.pm10, "Pm10");
        addField(data.pm25, "Pm25");
        addField(data.so2, "So2");
        addField(data.no2, "No2");
        addField(data.co2, "Co2");
        addField(data.co, "Co");
        addField(data.o3, "O3");
        addField(data.noise, "Noise");
        addField(data.pm1, "PM1");
        addField(data.tvoc, "Tvoc");
        addField(data.smoke, "Smoke");
        addField(data.methane, "Methane");
        addField(data.h2, "H2");
        addField(data.ammonia, "Ammonia");
        addField(data.h2s, "H2s");

        addField(data.temperature, "Temperature");
        addField(data.humidity, "Humidity");

        // ----------------------------- 
        //      HOURLY Aggregation 
        // -----------------------------

        await prisma.$runCommandRaw({
            update: "HourlyAggregateReading",
            updates: [
                {
                    q: {
                        deviceId,
                        hourStart: { $date: hourStart.toISOString() }
                    },
                    u: {
                        $setOnInsert: {
                            deviceId,
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
                        deviceId, dayStart
                    },
                    u: {
                        $setOnInsert: {
                            deviceId,
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

worker.on("failed", (job, err) => {
    console.error(`❌ Job failed: ${job?.id}`, err);
});

console.log("🚀 Worker running (Hourly + Daily Aggregation in 1 call)...");
