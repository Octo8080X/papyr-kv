import { createPaypyrKv } from "../mod.ts";
import { delay } from "@std/async/delay";
type MyPaypyrKvSchema = {
  "example-topic": {
    message: string;
    timestamp: string; // ISO date string
  };
};

const kv = await Deno.openKv("tmp/kv.db");
const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-publisher");
await papyrKv.publish("example-topic", {
  message: "Hello, world!",
  timestamp: new Date().toISOString(),
});

console.log(
  "clientId = ",
  papyrKv.clientId,
  "Published message to example-topic.",
);

while (true) {
  await delay(100); // Keep the process alive to receive messages
  console.log("Waiting for messages...");
  const messages = {
    message: "Hello, world!",
    timestamp: new Date().toISOString(),
  };
  console.log("publishing message to example-topic:", messages);
  await papyrKv.publish("example-topic", messages);
}
