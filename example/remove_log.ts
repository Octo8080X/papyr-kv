import { createPaypyrKv } from "../mod.ts";

type MyPaypyrKvSchema = {
  "example-topic": {
    message: string;
    timestamp: string; // ISO date string
  };
};

const kv = await Deno.openKv("http://localhost:4512");
const papyrKv = createPaypyrKv<MyPaypyrKvSchema>(kv, "example-publisher");

await papyrKv.removeLogs(
  //  "2025-05-30T18:42:41.271Z",
  //  "2025-05-31T18:43:16.019Z"
);
