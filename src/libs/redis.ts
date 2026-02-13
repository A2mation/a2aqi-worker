import "dotenv/config";
import { ConnectionOptions } from "bullmq";

if (!process.env.REDIS_URL) {
  throw new Error("❌ REDIS_URL missing");
}

const url = new URL(process.env.REDIS_URL);

export const connection: ConnectionOptions = {
  host: url.hostname,
  port: Number(url.port),
  username: url.username,
  password: url.password,

  tls: {
    rejectUnauthorized: false,
  },

  maxRetriesPerRequest: null,
  enableReadyCheck: false,
};
