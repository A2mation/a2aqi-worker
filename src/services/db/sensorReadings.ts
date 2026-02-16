import { prisma } from "../../libs/prisma";

export async function storeRawSensorReading(data: {
    deviceId: string,
    measuredAt: Date;

    // Pollution Data
    aqi?: number;
    pm10?: number;
    pm25?: number;
    so2?: number;
    no2?: number;
    co2?: number;
    co?: number;
    o3?: number;
    noise?: number;
    pm1?: number;
    tvoc?: number;
    smoke?: number;
    methane?: number;
    h2?: number;
    ammonia?: number;
    h2s?: number;

    // Weather Data
    temperature?: number;
    humidity?: number;
}) {
    // console.log(data)

    const measuredAt = data.measuredAt ? new Date(data.measuredAt) : new Date();

    

        return prisma.sensorReading.create({
            data: {
                deviceId: data.deviceId,
                measuredAt: measuredAt,

                // Pollution Data
                aqi: data.aqi ?? null,
                pm10: data.pm10 ?? null,
                pm25: data.pm25 ?? null,
                so2: data.so2 ?? null,
                no2: data.no2 ?? null,
                co2: data.co2 ?? null,
                co: data.co ?? null,
                o3: data.o3 ?? null,
                noise: data.noise ?? null,
                pm1: data.pm1 ?? null,
                tvoc: data.tvoc ?? null,
                smoke: data.smoke ?? null,
                methane: data.methane ?? null,
                h2: data.h2 ?? null,
                ammonia: data.ammonia ?? null,
                h2s: data.h2s ?? null,

                // Weather Data
                temperature: data.temperature ?? null,
                humidity: data.humidity ?? null,
            },
        });
    

}
