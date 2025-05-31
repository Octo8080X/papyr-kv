import { createPaypyrKv } from "../mod.ts";
import { delay } from "@std/async/delay";

type MyPaypyrKvSchema = {
  "example-topic": {
    message: string;
    timestamp: string; // ISO date string
  };
};

const kv = await Deno.openKv("tmp/kv.db");
const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-subscriber");
await papyrKv.subscribe("example-topic", (message) => {
  console.log("Received message:", message);
});

console.log(
  "clientId = ",
  papyrKv.clientId,
  " Subscribed to example-topic. Waiting for messages...",
);
//
//setTimeout(() => {
//    console.log("Unsubscribing");
//    papyrKv.unsubscribe("example-topic");
//}, 10000);
//
//await delay(20000)
//
//console.log("Ending subscriber after 20 seconds...");
