import { createPaypyrKv } from "../mod.ts";
import { stringify } from "@std/csv/stringify";

type MyPaypyrKvSchema = {
  "example-topic": {
    message: string;
    timestamp: string; // ISO date string
  };
};

const kv = await Deno.openKv("http://localhost:4512");
const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-publisher");

const log = await papyrKv.getLogs();
const csvHeader = [
  "timestamp",
  "clientId",
  "action",
  "memo",
];

const csvData = log.map((entry) => [
  entry.timestamp,
  entry.clientId,
  entry.action,
  entry.memo,
]);

Deno.writeTextFileSync(
  "./tmp/log.csv",
  await stringify([csvHeader, ...csvData]),
);
