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
package edu.snu.mist.api.sink.parameters;

/**
 * This class contains the list of necessary parameters for TextKafkaSinkConfiguration.
 */
public final class TextKafkaSinkParameters {

  private TextKafkaSinkParameters() {
    // Not called.
  }

  /**
   * The host address of the target KAFKA sink.
   */
  public static final String KAFKA_HOST_ADDRESS = "KafkaHostAddress";

  /**
   * The port of the target KAFKA sink.
   */
  public static final String KAFKA_HOST_PORT = "KafkaHostPort";

  /**
   * The topic name of target KAFKA sink.
   */
  public static final String KAFKA_TOPIC_NAME = "KafkaTopicName";

  /**
   * The number of partitions of target KAFKA sink. This will use the same number of threads in MISTTask.
   */
  public static final String KAFKA_NUM_PARTITION = "KafkaNumPartition";
}
