import { delay } from "@std/async/delay";
import { monotonicUlid } from "@std/ulid";
import type { PaypyrKvSubscribedMessage } from "./types.ts";
import { format } from "jsr:@std/internal@^1.0.6/format";

const PAYPYR_KV_PREFIX = "paypyr-kv" as const;
const PAYPYR_KV_MESSAGE_PREFIX = "paypyr-message" as const;
const PAYPYR_KV_MESSAGE_BODY_PREFIX = "paypyr-message-body" as const;
const PAYPYR_KV_SUBSCRIBER_PREFIX = "paypyr-subscriber" as const;
const PAYPYR_KV_LOG_PREFIX = "paypyr-log" as const;

const PAYPYR_KV_LOG_ACTION = [
  "PUBLISH",
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "DELIVER",
  "DELETE",
] as const;

export function createPaypyrKv<S extends object>(
  kv: Deno.Kv,
  clientPrefix?: string,
) {
  const clientId = getClientId(clientPrefix);
  const subscriberTopics = new Set<keyof S & string>();

  function getClientId(clientPrefix?: string) {
    if (typeof clientPrefix !== "string") {
      return crypto.randomUUID();
    }
    return `${clientPrefix}-${crypto.randomUUID()}`;
  }

  function errorMessage(message: string) {
    return `[PaypyrKv Error][clientId=${clientId}]: ${message}`;
  }

  function getMessagesKey<K extends keyof S>(topic: K) {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_PREFIX, topic];
  }
  function getMessageKey<K extends keyof S>(topic: K, messageId: string) {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_PREFIX, topic, messageId];
  }

  function getMessageBodyKey<K extends keyof S>(topic: K, messageId: string) {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_BODY_PREFIX, topic, messageId];
  }

  function getPaypyrKvSubscriberKey<K extends keyof S>(
    topic: K,
    subscriberId: string,
  ) {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_SUBSCRIBER_PREFIX, topic, subscriberId];
  }

  function getLogKey(date: Date) {
    return [...getLogsPrefix(), date.toISOString()];
  }

  function getLogsPrefix() {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_LOG_PREFIX];
  }

  function getLogBody(
    clientId: string,
    date: Date,
    action: typeof PAYPYR_KV_LOG_ACTION[number],
    ...memo: any[]
  ): LogValue {
    return {
      clientId,
      timestamp: new Date().toISOString(),
      action,
      date: date.toISOString(),
      memo: { ...memo },
    };
  }

  interface LogValue {
    clientId: string;
    timestamp: string; // ISO date string
    action: typeof PAYPYR_KV_LOG_ACTION[number];
    date: string; // ISO date string
    memo: Record<string, any>;
  }

  async function registerSubscriberKv<K extends keyof S & string>(
    topic: K,
  ) {
    const subscriberKey = getPaypyrKvSubscriberKey(topic, clientId);

    const atomic = kv.atomic();
    atomic.check({ key: subscriberKey, versionstamp: null });
    atomic.set(subscriberKey, { topic, subscriberId: clientId });
    const res = await atomic.commit();
    if (!res.ok) {
      throw new Error(
        errorMessage(
          `Client could not be registered as a subscriber for topic ${topic}. It may already be registered or another error occurred.`,
        ),
      );
    }
  }

  async function publish<K extends keyof S & string>(
    topic: K,
    value: S[K],
  ): Promise<string> {
    const messageId = monotonicUlid();
    const now = new Date();
    const atomic = kv.atomic();

    const message = { from: clientId, ...(value as object) } as S[K] & {
      from: string;
    };

    atomic.check({
      key: getMessageKey(topic, messageId),
      versionstamp: null,
    });
    atomic.set(
      getMessageKey(topic, messageId),
      0,
    );
    atomic.set(
      getMessageBodyKey(topic, messageId),
      message,
    );
    atomic.set(
      getLogKey(now),
      getLogBody(clientId, now, "PUBLISH", { topic, messageId, message }),
    );
    const res = await atomic.commit();
    if (!res.ok) {
      throw new Error(
        errorMessage(
          `Client could not publish message to topic ${topic}. It may not be registered or another error occurred.`,
        ),
      );
    }
    return messageId;
  }

  async function subscribe<K extends keyof S & string>(
    topic: K,
    callback: (
      value: PaypyrKvSubscribedMessage<S[K]>,
    ) => Deno.AtomicOperation | void,
    interval: number = 1000,
  ) {
    await registerSubscriberKv(topic);

    subscriberTopics.add(topic);

    (async () => {
      while (true) {
        if (!subscriberTopics.has(topic)) {
          console.log(
            `Client (clientId = ${clientId}) unsubscribed from topic ${topic}. Stopping polling.`,
          );
          break;
        }
        await delay(interval);

        const entriesIterator = kv.list<number>({
          prefix: getMessagesKey(topic),
        }, {
          limit: 1,
        });

        const entries = [];
        for await (const entry of entriesIterator) {
          entries.push(entry);
        }

        if (entries.length === 0) {
          console.log(
            `Client (clientId = ${clientId}) found no new messages for topic ${topic}.`,
          );
          continue;
        }

        console.log(`messages found for topic ${topic}:`, entries.length);

        for await (const entry of entries) {
          if (entry.value === 1) {
            continue;
          }

          const now = new Date();
          const readRes = await kv
            .atomic()
            .check({
              key: entry.key,
              versionstamp: entry.versionstamp,
            })
            .set(entry.key, 1)
            .set(
              getLogKey(now),
              getLogBody(clientId, now, "DELIVER", {
                topic,
                messageId: String(entry.key[3]),
              }),
            )
            .commit();
          if (!readRes.ok) {
            console.error(
              errorMessage(
                `Client (clientId = ${clientId}) could not read message with key ${entry.key}. It may have been deleted or another error occurred.`,
              ),
            );
            break;
          }

          const messageBody = await kv.get(
            getMessageBodyKey(topic, String(entry.key[3])),
          );

          callback({
            topic: topic,
            messageId: String(entry.key[3]),
            data: messageBody.value as S[K] & { from: string },
          });

          const nowDelete = new Date();
          const deleteRes = await kv.atomic().delete(
            getMessageKey(topic, String(entry.key[3])),
          ).delete(
            getMessageBodyKey(topic, String(entry.key[3])),
          ).set(
            getLogKey(nowDelete),
            getLogBody(clientId, nowDelete, "DELETE", {
              key: getMessageKey(topic, String(entry.key[3])),
            }),
          )
            .set(
              getLogKey(nowDelete),
              getLogBody(clientId, nowDelete, "DELETE", {
                key: getMessageBodyKey(topic, String(entry.key[3])),
                messageBody: messageBody.value,
              }),
            )
            .commit();

          if (!deleteRes.ok) {
            console.error(
              errorMessage(
                `Client could not delete message with key ${entry.key}. It may have been deleted or another error occurred.`,
              ),
            );
          }
        }
      }
    })();
  }
  async function unsubscribeKv<K extends keyof S & string>(
    topic: K,
  ) {
    const subscriberKey = getPaypyrKvSubscriberKey(topic, clientId);

    const getRes = await kv.get(subscriberKey);
    if (!getRes.value) {
      throw new Error(
        `1 Client (clientId = ${clientId}) is not subscribed to topic ${topic}. It may not be registered or another error occurred.`,
      );
    }

    const now = new Date();
    const res = await kv
      .atomic()
      .check({
        key: subscriberKey,
        versionstamp: getRes.versionstamp,
      })
      .delete(subscriberKey)
      .set(
        getLogKey(now),
        getLogBody(clientId, now, "UNSUBSCRIBE", { topic }),
      )
      .commit();
    if (!res.ok) {
      throw new Error(
        `Client (clientId = ${clientId}) could not be unsubscribed from topic ${topic}. It may not be registered or another error occurred.`,
      );
    }
  }

  async function unsubscribe<K extends keyof S & string>(
    topic: K,
  ) {
    if (!subscriberTopics.has(topic)) {
      throw new Error(
        `Client (clientId = ${clientId}) is not subscribed to topic ${topic}.`,
      );
    }
    await unsubscribeKv(topic);

    subscriberTopics.delete(topic);
  }

  // ログを出力用に加工

  interface FormattedLogEntry {
    timestamp: string; // ISO date string
    clientId: string;
    action: typeof PAYPYR_KV_LOG_ACTION[number];
    memo: string; // Optional, only for PUBLISH and DELIVER actions
  }

  function formatLogEntry(entryValue: LogValue): FormattedLogEntry {
    const { clientId, timestamp, action, ...memo } = entryValue;

    return {
      timestamp,
      clientId,
      action,
      memo: JSON.stringify(memo),
    };
  }

  async function getLogs() {
    const logEntries = kv.list<LogValue>({
      prefix: getLogsPrefix(),
    });

    const buffer: LogValue[] = [];
    for await (const entry of logEntries) {
      buffer.push(entry.value);
    }
    return buffer.map((b) => formatLogEntry(b));
  }

  async function removeLogs(start?: string, end?: string) {
    if (!start || !end) {
      const logEntries = kv.list<LogValue>({
        prefix: getLogsPrefix(),
      });

      for await (const entry of logEntries) {
        kv.delete(entry.key);
      }
      return;
    }

    const logEntries = kv.list<LogValue>({
      start: [...getLogsPrefix(), start],
      end: [...getLogsPrefix(), end],
    });

    for await (const entry of logEntries) {
      kv.delete(entry.key);
    }
  }

  return {
    clientId,
    publish,
    subscribe,
    unsubscribe,
    unsubscribeKv,
    getLogs,
    removeLogs,
  };
}
