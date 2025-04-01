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

import {
  Attributes,
  Context,
  context,
  Counter,
  Histogram,
  Link,
  propagation,
  ROOT_CONTEXT,
  Span,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition,
  InstrumentationNodeModuleFile,
  isWrapped,
  safeExecuteInTheMiddle,
} from '@opentelemetry/instrumentation';
import {
  ATTR_ERROR_TYPE,
  ATTR_SERVER_ADDRESS,
  ATTR_SERVER_PORT,
  ERROR_TYPE_VALUE_OTHER,
} from '@opentelemetry/semantic-conventions';
import type * as kafkaJs from 'kafkajs';
import type {
  Consumer,
  ConsumerRunConfig,
  EachBatchHandler,
  EachMessageHandler,
  KafkaMessage,
  Message,
  Producer,
  RecordMetadata,
} from 'kafkajs';
import { EVENT_LISTENERS_SET, TopicData } from './internal-types';
import { bufferTextMapGetter } from './propagator';
import {
  ATTR_MESSAGING_BATCH_MESSAGE_COUNT,
  ATTR_MESSAGING_DESTINATION_NAME,
  ATTR_MESSAGING_DESTINATION_PARTITION_ID,
  ATTR_MESSAGING_KAFKA_MESSAGE_KEY,
  ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE,
  ATTR_MESSAGING_KAFKA_OFFSET,
  ATTR_MESSAGING_OPERATION_NAME,
  ATTR_MESSAGING_OPERATION_TYPE,
  ATTR_MESSAGING_SYSTEM,
  MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
  MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
  MESSAGING_OPERATION_TYPE_VALUE_SEND,
  MESSAGING_SYSTEM_VALUE_KAFKA,
  METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES,
  METRIC_MESSAGING_CLIENT_OPERATION_DURATION,
  METRIC_MESSAGING_CLIENT_SENT_MESSAGES,
  METRIC_MESSAGING_PROCESS_DURATION,
} from './semconv';
import { KafkaJsInstrumentationConfig } from './types';
/** @knipignore */
import { PACKAGE_NAME, PACKAGE_VERSION } from './version';

interface ConsumerSpanOptions {
  topic: string;
  message: KafkaMessage | undefined;
  operationType: string;
  attributes: Attributes;
  ctx?: Context | undefined;
  link?: Link;
}
// This interface acts as a strict subset of the KafkaJS Consumer and
// Producer interfaces (just for the event we're needing)
interface KafkaEventEmitter {
  on(
    eventName:
      | kafkaJs.ConsumerEvents['REQUEST']
      | kafkaJs.ProducerEvents['REQUEST'],
    listener: (event: kafkaJs.RequestEvent) => void
  ): void;
  events: {
    REQUEST:
      | kafkaJs.ConsumerEvents['REQUEST']
      | kafkaJs.ProducerEvents['REQUEST'];
  };
  [EVENT_LISTENERS_SET]?: boolean;
}

interface StandardAttributes<OP extends string = string> extends Attributes {
  [ATTR_MESSAGING_SYSTEM]: string;
  [ATTR_MESSAGING_OPERATION_NAME]: OP;
  [ATTR_ERROR_TYPE]?: string;
}
interface TopicAttributes {
  [ATTR_MESSAGING_DESTINATION_NAME]: string;
  [ATTR_MESSAGING_DESTINATION_PARTITION_ID]?: string;
}

interface ClientDurationAttributes
  extends StandardAttributes,
    Partial<TopicAttributes> {
  [ATTR_SERVER_ADDRESS]: string;
  [ATTR_SERVER_PORT]: number;
  [ATTR_MESSAGING_OPERATION_TYPE]?: string;
}
interface SentMessagesAttributes
  extends StandardAttributes<'send'>,
    TopicAttributes {
  [ATTR_ERROR_TYPE]?: string;
}
type ConsumedMessagesAttributes = StandardAttributes<'receive' | 'process'>;
interface MessageProcessDurationAttributes
  extends StandardAttributes<'process'>,
    TopicAttributes {
  [ATTR_MESSAGING_SYSTEM]: string;
  [ATTR_MESSAGING_OPERATION_NAME]: 'process';
  [ATTR_ERROR_TYPE]?: string;
}
type RecordPendingMetric = (errorType?: string | undefined) => void;

// for example: broker info, consumer group info.
const ConsumeMessageExtraAttributes = Symbol(
  "opentelemetry.instrumentation_kafkajs.consume_message_extra_attributes"
);

export type PatchedKafkaMessage = KafkaMessage & {
  [ConsumeMessageExtraAttributes]: Attributes;
};

function prepareCounter<T extends Attributes>(
  meter: Counter<T>,
  value: number,
  attributes: T
): RecordPendingMetric {
  return (errorType?: string | undefined) => {
    meter.add(value, {
      ...attributes,
      ...(errorType ? { [ATTR_ERROR_TYPE]: errorType } : {}),
    });
  };
}

function prepareDurationHistogram<T extends Attributes>(
  meter: Histogram<T>,
  value: number,
  attributes: T
): RecordPendingMetric {
  return (errorType?: string | undefined) => {
    meter.record((Date.now() - value) / 1000, {
      ...attributes,
      ...(errorType ? { [ATTR_ERROR_TYPE]: errorType } : {}),
    });
  };
}

const HISTOGRAM_BUCKET_BOUNDARIES = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];
export class KafkaJsInstrumentation extends InstrumentationBase<KafkaJsInstrumentationConfig> {
  private _clientDuration!: Histogram<ClientDurationAttributes>;
  private _sentMessages!: Counter<SentMessagesAttributes>;
  private _consumedMessages!: Counter<ConsumedMessagesAttributes>;
  private _processDuration!: Histogram<MessageProcessDurationAttributes>;

  constructor(config: KafkaJsInstrumentationConfig = {}) {
    super(PACKAGE_NAME, PACKAGE_VERSION, config);
  }

  override _updateMetricInstruments() {
    this._clientDuration = this.meter.createHistogram(
      METRIC_MESSAGING_CLIENT_OPERATION_DURATION,
      { advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES } }
    );
    this._sentMessages = this.meter.createCounter(
      METRIC_MESSAGING_CLIENT_SENT_MESSAGES
    );
    this._consumedMessages = this.meter.createCounter(
      METRIC_MESSAGING_CLIENT_CONSUMED_MESSAGES
    );
    this._processDuration = this.meter.createHistogram(
      METRIC_MESSAGING_PROCESS_DURATION,
      { advice: { explicitBucketBoundaries: HISTOGRAM_BUCKET_BOUNDARIES } }
    );
  }

  protected init() { 
    const instrumentation = this;

    const unpatchBrokerMethod = (moduleExports: any) => { 
      if (moduleExports?.prototype?.produce?.__wrapped) {
        instrumentation._unwrap(moduleExports.prototype, "produce");
      }
    };

    const unpatchKafkaModule = (moduleExports: typeof kafkaJs) => {
      if (isWrapped(moduleExports?.Kafka?.prototype.consumer)) {
        this._unwrap(moduleExports.Kafka.prototype, 'consumer');
      }
    };

    const brokerFileInstrumentation = new InstrumentationNodeModuleFile(
      "kafkajs/src/broker/index.js",
      [">=0.3.0 <3"],
      (moduleExports: any) => {
        this._wrap(
          moduleExports.prototype,
          "produce",
          this.getBrokerProducePatch()
        );
        return moduleExports;
      },
      unpatchBrokerMethod
    );

    const module = new InstrumentationNodeModuleDefinition(
      'kafkajs',
      ['>=0.3.0 <3'],
      (moduleExports: typeof kafkaJs) => {
        this._wrap(
          moduleExports?.Kafka?.prototype,
          'consumer',
          this._getConsumerPatch()
        );
        return moduleExports;
      },
      unpatchKafkaModule,
      [
        brokerFileInstrumentation
      ]
    );
    return module;
  }

  private getBrokerProducePatch() {
    const instrumentation = this;
    return (original: any) => {
      return function produce(this: any, produceArgs: any) {
        // The broker object is the caller, and JavaScript binds this in that function call to the broker instance.
        const brokerAddress: string = this.brokerAddress;
        const topicData: TopicData = produceArgs.topicData;
        
        const spans = topicData.flatMap((t) => {
          const topic = t.topic;
          return t.partitions.flatMap((p) => {
            const partition = p.partition; 
            return p.messages.map((m: Message) => {
              const singleMessageSpan = instrumentation.tracer.startSpan(
                topic,
                {
                  kind: SpanKind.PRODUCER,
                  startTime: Number(m.timestamp), 
                  attributes: {
                    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                    [ATTR_MESSAGING_OPERATION_NAME]: "produce",
                    [ATTR_MESSAGING_DESTINATION_NAME]: topic,
                    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: partition,
                    [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]:
                      m.key?.toString("base64"),
                    [ATTR_SERVER_ADDRESS]: brokerAddress,
                  },
                }
              );
              // active span?
              // record the span context in each message headers for context propagation to downstream spans
              m.headers = m.headers ?? {}; // NOTICE - changed via side effect
              propagation.inject(
                trace.setSpan(context.active(), singleMessageSpan),
                m.headers
              );

              m.headers = { ...m.headers };
              return singleMessageSpan;
            });
          });
        });

        try {
          return original.apply(this, arguments);
        } finally {
          spans.forEach((span) => {
            span.end();
          });
        }
      };
    };
  }

  private _getConsumerPatch() {
    const instrumentation = this;
    return (original: kafkaJs.Kafka['consumer']) => {
      return function consumer(
        this: kafkaJs.Kafka,
        ...args: Parameters<kafkaJs.Kafka['consumer']>
      ) {
        const newConsumer: Consumer = original.apply(this, args);

        if (isWrapped(newConsumer.run)) {
          instrumentation._unwrap(newConsumer, 'run');
        }

        instrumentation._wrap(
          newConsumer,
          'run',
          instrumentation._getConsumerRunPatch()
        );

        instrumentation._setKafkaEventListeners(newConsumer);

        return newConsumer;
      };
    };
  }

  private _setKafkaEventListeners(kafkaObj: KafkaEventEmitter) {
    if (kafkaObj[EVENT_LISTENERS_SET]) return;

    kafkaObj.on(
      kafkaObj.events.REQUEST,
      this._recordClientDurationMetric.bind(this)
    );

    kafkaObj[EVENT_LISTENERS_SET] = true;
  }

  private _recordClientDurationMetric(
    event: Pick<kafkaJs.RequestEvent, 'payload'>
  ) {
    const [address, port] = event.payload.broker.split(':');
    this._clientDuration.record(event.payload.duration / 1000, {
      [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
      [ATTR_MESSAGING_OPERATION_NAME]: `${event.payload.apiName}`, // potentially suffix with @v${event.payload.apiVersion}?
      [ATTR_SERVER_ADDRESS]: address,
      [ATTR_SERVER_PORT]: Number.parseInt(port, 10),
    });
  }

  private _getConsumerRunPatch() {
    const instrumentation = this;
    return (original: Consumer['run']) => {
      return function run(
        this: Consumer,
        ...args: Parameters<Consumer['run']>
      ): ReturnType<Consumer['run']> {
        const config = args[0];
        if (config?.eachMessage) {
          if (isWrapped(config.eachMessage)) {
            instrumentation._unwrap(config, 'eachMessage');
          }
          instrumentation._wrap(
            config,
            'eachMessage',
            instrumentation._getConsumerEachMessagePatch()
          );
        }
        if (config?.eachBatch) {
          if (isWrapped(config.eachBatch)) {
            instrumentation._unwrap(config, 'eachBatch');
          }
          instrumentation._wrap(
            config,
            'eachBatch',
            instrumentation._getConsumerEachBatchPatch()
          );
        }
        return original.call(this, config);
      };
    };
  }

  private _getConsumerEachMessagePatch() {
    const instrumentation = this;
    return (original: ConsumerRunConfig['eachMessage']) => {
      return function eachMessage(
        this: unknown,
        ...args: Parameters<EachMessageHandler>
      ): Promise<void> {
        const payload = args[0];
        const propagatedContext: Context = propagation.extract(
          ROOT_CONTEXT,
          payload.message.headers,
          bufferTextMapGetter
        );
        const span = instrumentation._startConsumerSpan({
          topic: payload.topic,
          message: payload.message,
          operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
          ctx: propagatedContext,
          attributes: {
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
              payload.partition
            ),
          },
        });

        const pendingMetrics: RecordPendingMetric[] = [
          prepareDurationHistogram(
            instrumentation._processDuration,
            Date.now(),
            {
              [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
              [ATTR_MESSAGING_OPERATION_NAME]: 'process',
              [ATTR_MESSAGING_DESTINATION_NAME]: payload.topic,
              [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
                payload.partition
              ),
            }
          ),
          prepareCounter(instrumentation._consumedMessages, 1, {
            [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
            [ATTR_MESSAGING_OPERATION_NAME]: 'process',
            [ATTR_MESSAGING_DESTINATION_NAME]: payload.topic,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
              payload.partition
            ),
          }),
        ];

        const eachMessagePromise = context.with(
          trace.setSpan(propagatedContext, span),
          () => {
            return original!.apply(this, args);
          }
        );
        return instrumentation._endSpansOnPromise(
          [span],
          pendingMetrics,
          eachMessagePromise
        );
      };
    };
  }

  private _getConsumerEachBatchPatch() {
    return (original: ConsumerRunConfig['eachBatch']) => {
      const instrumentation = this;
      return function eachBatch(
        this: unknown,
        ...args: Parameters<EachBatchHandler>
      ): Promise<void> {
        const payload = args[0];
        // https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/trace/semantic_conventions/messaging.md#topic-with-multiple-consumers
        const receivingSpan = instrumentation._startConsumerSpan({
          topic: payload.batch.topic,
          message: undefined,
          operationType: MESSAGING_OPERATION_TYPE_VALUE_RECEIVE,
          ctx: ROOT_CONTEXT,
          attributes: {
            [ATTR_MESSAGING_BATCH_MESSAGE_COUNT]: payload.batch.messages.length,
            [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
              payload.batch.partition
            ),
          },
        });
        return context.with(
          trace.setSpan(context.active(), receivingSpan),
          () => {
            const startTime = Date.now();
            const spans: Span[] = [];
            const pendingMetrics: RecordPendingMetric[] = [
              prepareCounter(
                instrumentation._consumedMessages,
                payload.batch.messages.length,
                {
                  [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                  [ATTR_MESSAGING_OPERATION_NAME]: 'process',
                  [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
                  [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
                    payload.batch.partition
                  ),
                }
              ),
            ];
            payload.batch.messages.forEach(message => {
              const propagatedContext: Context = propagation.extract(
                ROOT_CONTEXT,
                message.headers,
                bufferTextMapGetter
              );
              const spanContext = trace
                .getSpan(propagatedContext)
                ?.spanContext();
              let origSpanLink: Link | undefined;
              if (spanContext) {
                origSpanLink = {
                  context: spanContext,
                };
              }
              spans.push(
                instrumentation._startConsumerSpan({
                  topic: payload.batch.topic,
                  message,
                  operationType: MESSAGING_OPERATION_TYPE_VALUE_PROCESS,
                  link: origSpanLink,
                  attributes: {
                    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
                      payload.batch.partition
                    ),
                  },
                })
              );
              pendingMetrics.push(
                prepareDurationHistogram(
                  instrumentation._processDuration,
                  startTime,
                  {
                    [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
                    [ATTR_MESSAGING_OPERATION_NAME]: 'process',
                    [ATTR_MESSAGING_DESTINATION_NAME]: payload.batch.topic,
                    [ATTR_MESSAGING_DESTINATION_PARTITION_ID]: String(
                      payload.batch.partition
                    ),
                  }
                )
              );
            });
            const batchMessagePromise: Promise<void> = original!.apply(
              this,
              args
            );
            spans.unshift(receivingSpan);
            return instrumentation._endSpansOnPromise(
              spans,
              pendingMetrics,
              batchMessagePromise
            );
          }
        );
      };
    };
  }

  private _endSpansOnPromise<T>(
    spans: Span[],
    pendingMetrics: RecordPendingMetric[],
    sendPromise: Promise<T>
  ): Promise<T> {
    return Promise.resolve(sendPromise)
      .then(result => {
        pendingMetrics.forEach(m => m());
        return result;
      })
      .catch(reason => {
        let errorMessage: string | undefined;
        let errorType: string = ERROR_TYPE_VALUE_OTHER;
        if (typeof reason === 'string' || reason === undefined) {
          errorMessage = reason;
        } else if (
          typeof reason === 'object' &&
          Object.prototype.hasOwnProperty.call(reason, 'message')
        ) {
          errorMessage = reason.message;
          errorType = reason.constructor.name;
        }
        pendingMetrics.forEach(m => m(errorType));

        spans.forEach(span => {
          span.setAttribute(ATTR_ERROR_TYPE, errorType);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message: errorMessage,
          });
        });

        throw reason;
      })
      .finally(() => {
        spans.forEach(span => span.end());
      });
  }

  private _startConsumerSpan({
    topic,
    message,
    operationType,
    ctx,
    link,
    attributes,
  }: ConsumerSpanOptions) {
    const operationName =
      operationType === MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
        ? 'poll' // for batch processing spans
        : operationType; // for individual message processing spans

    const span = this.tracer.startSpan(
      `${operationName} ${topic}`,
      {
        kind:
          operationType === MESSAGING_OPERATION_TYPE_VALUE_RECEIVE
            ? SpanKind.CLIENT
            : SpanKind.CONSUMER,
        attributes: {
          ...attributes,
          [ATTR_MESSAGING_SYSTEM]: MESSAGING_SYSTEM_VALUE_KAFKA,
          [ATTR_MESSAGING_DESTINATION_NAME]: topic,
          [ATTR_MESSAGING_OPERATION_TYPE]: operationType,
          [ATTR_MESSAGING_OPERATION_NAME]: operationName,
          [ATTR_MESSAGING_KAFKA_MESSAGE_KEY]: message?.key
            ? String(message.key)
            : undefined,
          [ATTR_MESSAGING_KAFKA_MESSAGE_TOMBSTONE]:
            message?.key && message.value === null ? true : undefined,
          [ATTR_MESSAGING_KAFKA_OFFSET]: message?.offset,
        },
        links: link ? [link] : [],
      },
      ctx
    );

    const { consumerHook } = this.getConfig();
    if (consumerHook && message) {
      safeExecuteInTheMiddle(
        () => consumerHook(span, { topic, message }),
        e => {
          if (e) this._diag.error('consumerHook error', e);
        },
        true
      );
    }

    return span;
  }

}
