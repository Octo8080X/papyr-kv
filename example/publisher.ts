import { createPaypyrKv } from "../mod.ts";
import type { MyPaypyrKvSchema } from "./types.ts";

const kv = await Deno.openKv("http://localhost:4512");
const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-publisher");

setInterval(() => {
  const messages = {
    message: "Hello, world!",
    timestamp: new Date().toISOString(),
  };
  console.log("publishing message to example-topic:", messages);
  papyrKv.publish("example-topic", messages);
}, 500);

console.log(
  "clientId = ",
  papyrKv.clientId,
  "Published message to example-topic.",
);
