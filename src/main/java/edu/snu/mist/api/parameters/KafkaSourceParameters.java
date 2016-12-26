/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api.parameters;

/**
 * This class contains the list of necessary or optional parameters for KafkaSourceConfiguration.
 */
public final class KafkaSourceParameters {

  private KafkaSourceParameters() {
    // Not called.
  }

  /**
   * The kafka topic to monitor.
   */
  public static final String KAFKA_TOPIC = "Kafka topic to monitor";

  /**
   * The configuration for the kafka consumer to use.
   */
  public static final String KAFKA_CONSUMER_CONFIG = "Kafka consumer config";
}