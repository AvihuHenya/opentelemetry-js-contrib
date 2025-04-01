// consume extra attributes is an attributes object that is attached to each message in the batch.
// it stores interesting information about the message that is not available in the message object itself at time of span creation.

import { Attributes } from "@opentelemetry/api";
import { KafkaMessage } from "kafkajs";

// for example: broker info, consumer group info.
const ConsumeMessageExtraAttributes = Symbol(
  "opentelemetry.instrumentation_kafkajs.consume_message_extra_attributes"
);

export type PatchedKafkaMessage = KafkaMessage & {
  [ConsumeMessageExtraAttributes]: Attributes;
};

// this function will create a side effect on the message object,
// creating or updating the additional attributes object it stores with the provided attributes.
// the value can later be read from the message object using the getMessagesAttributes function.
export const createOrAddMessageAttributes = (
  message: PatchedKafkaMessage,
  attributes: Attributes
) => {
  if (message[ConsumeMessageExtraAttributes]) {
    Object.assign(message[ConsumeMessageExtraAttributes], attributes);
  } else {
    Object.defineProperty(message, ConsumeMessageExtraAttributes, {
      value: attributes,
      enumerable: false, // hide from JSON.stringify, console.log, etc to have minimal impact on existing code
    });
  }
};

export const getMessagesAttributes = (
  message: PatchedKafkaMessage
): Attributes | undefined => {
  return message[ConsumeMessageExtraAttributes];
};
