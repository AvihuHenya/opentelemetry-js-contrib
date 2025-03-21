/*
 * Copyright The OpenTelemetry Authors
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

import { Meter, MeterProvider, diag, metrics } from '@opentelemetry/api';

/** @knipignore */
import { PACKAGE_NAME, PACKAGE_VERSION } from './version';

/**
 * Metrics Collector Configuration
 */
export interface MetricsCollectorConfig {
  // Meter Provider
  meterProvider?: MeterProvider;
  // Name of component
  name?: string;
}

const DEFAULT_NAME = PACKAGE_NAME;

/**
 * Base Class for metrics
 */
export abstract class BaseMetrics {
  protected _logger = diag;
  protected _meter: Meter;
  private _name: string;

  constructor(config?: MetricsCollectorConfig) {
    // Do not use `??` operator to allow falling back to default when the
    // specified name is an empty string.
    this._name = config?.name || DEFAULT_NAME;
    const meterProvider = config?.meterProvider ?? metrics.getMeterProvider();
    this._meter = meterProvider.getMeter(this._name, PACKAGE_VERSION);
  }

  /**
   * Creates metrics
   */
  protected abstract _createMetrics(): void;

  /**
   * Starts collecting stats
   */
  public abstract start(): void;
}
