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
package edu.snu.mist.client.datastreams.configurations;

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;
import edu.snu.mist.common.functions.MISTFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the kafka source configuration.
 */
public final class KafkaSourceConfiguration {

  private KafkaSourceConfiguration() {
    // do nothing
  }

  /**
   * Gets the builder for Configuration construction.
   * @return the builder
   */
  public static KafkaSourceConfigurationBuilder newBuilder() {
    return new KafkaSourceConfigurationBuilder();
  }

  /**
   * This class builds a KafkaSourceConfiguration for KafkaSourceStream.
   */
  public static final class KafkaSourceConfigurationBuilder {

    private String kafkaTopic;
    private HashMap<String, Object> kafkaConfig;
    private MISTFunction<ConsumerRecord, Tuple<ConsumerRecord, Long>> extractFunc;

    /**
     * Tests that required parameters are set and builds the KafkaSourceConfiguration.
     * @return the configuration
     */
    public SourceConfiguration build() {

      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.SourceConf.SOURCE_TYPE.name(), ConfValues.SourceType.KAFKA.name());
      confMap.put(ConfKeys.KafkaSourceConf.KAFKA_TOPIC.name(), kafkaTopic);

      try {
        confMap.put(ConfKeys.KafkaSourceConf.KAFKA_CONSUMER_CONFIG.name(),
            SerializeUtils.serializeToString(kafkaConfig));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }

      try {
        if (extractFunc == null) {
          // No extract function is set
        } else {
          // Lambda object is set
          confMap.put(ConfKeys.SourceConf.TIMESTAMP_EXTRACT_FUNC.name(),
              SerializeUtils.serializeToString(extractFunc));
        }
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return new SourceConfiguration(confMap);
    }

    /**
     * Sets the configuration for the kafka topic to the given topic.
     * @param topic the kafka topic given by user to monitor
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder setTopic(final String topic) {
      kafkaTopic = topic;
      return this;
    }

    /**
     * Sets the configuration for the kafka consumer to the given HashMap.
     * KafkaSource would create a KafkaConsumer according to this configuration.
     * This configuration should follows the ConsumerConfig policy defined in kafka.
     * @param consumerConfig the kafka consumer configuration HashMap given by users
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder setConsumerConfig(final HashMap<String, Object> consumerConfig) {
      // configurability check
      final Set<String> configurable = ConsumerConfig.configNames();
      for (final Map.Entry<String, Object> entry : consumerConfig.entrySet()) {
        if (!configurable.contains(entry.getKey())) {
          throw new IllegalArgumentException(
              "Attempts to add an undefined kafka consumer configuration " + entry.getKey());
        }
      }
      kafkaConfig = consumerConfig;
      return this;
    }

    /**
     * Sets the configuration for the extracting timestamp in event-time input data function to the given function.
     * This is an optional setting for event-time processing.
     * @param function the function given by users which they want to set
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder setTimestampExtractionFunction(
        final MISTFunction<ConsumerRecord, Tuple<ConsumerRecord, Long>> function) {
      extractFunc = function;
      return this;
    }
  }
}