/*
 * Copyright The OpenTelemetry Authors, Aspecto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type * as KafkaJSTypes from 'kafkajs';

export const EVENT_LISTENERS_SET = Symbol(
  'opentelemetry.instrumentation.kafkajs.eventListenersSet'
);

export interface Partition {
  partition: number;
  firstSequence: number;
  messages: Array<any>;
}

export interface Topic {
  topic: string;
  partitions: Array<Partition>;
}

export interface ConsumerExtended extends KafkaJSTypes.Consumer {
  [EVENT_LISTENERS_SET]?: boolean; // flag to identify if the event listeners for instrumentation have been set
}

export interface ProducerExtended extends KafkaJSTypes.Producer {
  [EVENT_LISTENERS_SET]?: boolean; // flag to identify if the event listeners for instrumentation have been set
}

export type TopicData = Array<Topic>;