import { delay } from "@std/async/delay";

//const kv = await Deno.openKv("aa");

interface PaypyrKvSubscribedMessage<T> {
  topic: string;
  messageId: string;
  data: T & {
    from: string; // Client ID of the publisher
  };
}

const PAYPYR_KV_PREFIX = "paypyr-kv";
const PAYPYR_KV_MESSAGE_PREFIX = "paypyr-message";
const PAYPYR_KV_MESSAGE_BODY_PREFIX = "paypyr-message-body";

function getMessagesKey(topic: string) {
  return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_PREFIX, topic];
}
function getMessageKey(topic: string, messageId: string) {
  return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_PREFIX, topic, messageId];
}

function getMessageBodyKey(topic: string, messageId: string) {
  return [PAYPYR_KV_PREFIX, PAYPYR_KV_MESSAGE_BODY_PREFIX, topic, messageId];
}

const PAYPYR_KV_SUBSCRIBER_PREFIX = "paypyr-subscriber";

//interface PaypyrKvList<T> {
//  key: [string, string, string, string]; // [PAYPYR_KV_PREFIX, topic, messageId]
//  value: T;
//  versionstamp: string;
//}

export function createPaypyrKv<S extends object>(
  kv: Deno.Kv,
  clientPrefix?: string,
) {
  const clientId = (clientPrefix ? clientPrefix : "") + "-" +
    crypto.randomUUID();
  function getPaypyrKvSubscriberKey<K extends keyof S>(
    topic: K,
    subscriberId: string,
  ) {
    return [PAYPYR_KV_PREFIX, PAYPYR_KV_SUBSCRIBER_PREFIX, topic, subscriberId];
  }

  async function registerSubscriber<K extends keyof S & string>(
    topic: K,
  ) {
    const subscriberKey = getPaypyrKvSubscriberKey(topic, clientId);

    const atomic = kv.atomic();
    atomic.check({ key: subscriberKey, versionstamp: null });
    atomic.set(subscriberKey, { topic, subscriberId: clientId });
    const res = await atomic.commit();
    if (!res.ok) {
      throw new Error(
        "Client (clientId = " + clientId +
          ") could not be registered as a subscriber for topic " + topic +
          ". It may already be registered or another error occurred.",
      );
    }
  }

  const subscriberTopics = new Set<keyof S & string>();

  return {
    publish: async <K extends keyof S & string>(topic: K, value: S[K]) => {
      const messageId = crypto.randomUUID();
      const atomic = kv.atomic();

      atomic.check({
        key: getMessageKey(topic as string, messageId),
        versionstamp: null,
      });
      atomic.set(
        getMessageKey(topic as string, messageId),
        0,
      );
      atomic.set(
        getMessageBodyKey(topic as string, messageId),
        { from: clientId, ...(value as object) } as S[K] & { from: string },
      );
      const res = await atomic.commit();
      if (!res.ok) {
        throw new Error(
          "Client (clientId = " + clientId +
            ") could not publish message to topic " + topic +
            ". It may not be registered or another error occurred.",
        );
      }
      console.log(
        `Client (clientId = ${clientId}) published message to topic ${topic} with ID ${messageId}.`,
      );
      return messageId;
    },
    subscribe: async <K extends keyof S & string>(
      topic: K,
      callback: (
        value: PaypyrKvSubscribedMessage<S[K]>,
      ) => Deno.AtomicOperation | void,
      interval: number = 1000,
    ) => {
      await registerSubscriber(topic);

      subscriberTopics.add(topic);

      (async () => {
        // Polling for new messages
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
            await delay(100);
            if (entry.value === 1) {
              continue; // Skip if the value is null
            }

            try {
              const readAtomic = kv.atomic();
              readAtomic.check({
                key: entry.key,
                versionstamp: entry.versionstamp,
              });
              readAtomic.set(entry.key, 1);
              const res = await readAtomic.commit();
              if (!res.ok) {
                console.log(
                  `Client (clientId = ${clientId}) could not read message with key ${entry.key}. It may have been deleted or another error occurred.`,
                );
                break;
              }
            } catch (e) {
              console.error(e);
              break;
            }

            const messageBody = await kv.get(
              getMessageBodyKey(topic, String(entry.key[3])),
            );

            const atomic = callback({
              topic: topic,
              messageId: String(entry.key[3]),
              data: messageBody.value as S[K] & { from: string },
            });

            if (atomic instanceof Deno.AtomicOperation) {
              atomic.delete(
                getMessageKey(topic, String(entry.key[3])),
              );
              await atomic.commit();
              continue;
            }
            kv.delete(
              getMessageKey(topic, String(entry.key[3])),
            );
          }
        }
      })();
    },
    unsubscribe: async <K extends keyof S & string>(
      topic: K,
    ) => {
      if (!subscriberTopics.has(topic)) {
        throw new Error(
          `1 Client (clientId = ${clientId}) is not subscribed to topic ${topic}.`,
        );
      }
      const subscriberKey = getPaypyrKvSubscriberKey(topic, clientId);

      const getRes = await kv.get(subscriberKey);
      if (!getRes.value) {
        throw new Error(
          `1 Client (clientId = ${clientId}) is not subscribed to topic ${topic}. It may not be registered or another error occurred.`,
        );
      }

      const atomic = kv.atomic();
      atomic.check({ key: subscriberKey, versionstamp: getRes.versionstamp });
      atomic.delete(subscriberKey);
      const res = await atomic.commit();
      if (!res.ok) {
        throw new Error(
          `2 Client (clientId = ${clientId}) could not be unsubscribed from topic ${topic}. It may not be registered or another error occurred.`,
        );
      }

      subscriberTopics.delete(topic);
    },
    clientId,
  };
}
