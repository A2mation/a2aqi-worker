import { prisma } from "../../libs/prisma";

export async function storeRawSensorReading(data: {
    deviceId: string
    measuredAt?: Date

    aqi: number
    pm10?: number
    pm25?: number
    so2?: number
    no2?: number
    co2?: number
    co?: number
    o3?: number
    noise?: number
    pm1?: number
    tvoc?: number
    smoke?: number
    methane?: number
    h2?: number
    ammonia?: number
    h2s?: number

    temperature?: number
    humidity?: number
}) {
    const measuredAt = data.measuredAt
        ? new Date(data.measuredAt)
        : new Date()

    const sensorPayload = {
        deviceId: data.deviceId,
        measuredAt,

        aqi: data.aqi ,
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

        temperature: data.temperature ?? null,
        humidity: data.humidity ?? null,
    }

    return await prisma.$transaction(async (tx) => {
        // Store historical record
        const historical = await tx.sensorReading.create({
            data: sensorPayload,
        })

        // Update latest record
        await tx.latestSensorReading.upsert({
            where: { deviceId: sensorPayload.deviceId },
            update: {
                ...sensorPayload,
            },
            create: {
                ...sensorPayload,
            },
        })

        return historical
    })
}