/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.common.configurations;

public final class ConfValues {

  private ConfValues() {

  }

  public enum SourceType {
    KAFKA,
    NETTY,
    MQTT
  }

  public enum OperatorType {
    MAP,
    FLAT_MAP,
    FILTER,
    APPLY_STATEFUL,
    STATE_TRANSITION,
    CEP,
    REDUCE_BY_KEY,
    UNION,
    TIME_WINDOW,
    COUNT_WINDOW,
    SESSION_WINDOW,
    JOIN,
    AGGREGATE_WINDOW,
    APPLY_STATEFUL_WINDOW
  }

  public enum SinkType {
    NETTY,
    MQTT
  }

  public enum EventGeneratorType {
    PERIODIC_EVENT_GEN,
    PUNCTUATED_EVENT_GEN
  }
}
