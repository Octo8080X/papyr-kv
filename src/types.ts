export interface PaypyrKvSubscribedMessage<T> {
  topic: string;
  messageId: string;
  data: T & {
    from: string; // Client ID of the publisher
  };
}
