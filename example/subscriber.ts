import { createPaypyrKv } from "../mod.ts";
import type { MyPaypyrKvSchema } from "./types.ts";

const kv = await Deno.openKv("http://localhost:4512");

const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-subscriber");
await papyrKv.subscribe("example-topic", (message) => {
  console.log("Received message:", message);
}, 100);

console.log(
  "clientId = ",
  papyrKv.clientId,
  " Subscribed to example-topic. Waiting for messages...",
);
