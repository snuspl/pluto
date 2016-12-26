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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.parameters.KafkaSourceParameters;
import edu.snu.mist.api.parameters.SourceParameters;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the kafka source configuration.
 * @param <K> the type of kafka record's key
 * @param <V> the type of kafka record's value
 */
public final class KafkaSourceConfiguration<K, V> extends SourceConfiguration<ConsumerRecord<K, V>> {
  private KafkaSourceConfiguration(final Map<String, Object> configMap) {
    super(configMap);
  }

  /**
   * Gets the builder for Configuration construction.
   * @param <K> the type of kafka record's key that the target configuration will have
   * @param <V> the type of kafka record's value that the target configuration will have
   * @return the builder
   */
  public static <K, V> KafkaSourceConfigurationBuilder<K, V> newBuilder() {
    return new KafkaSourceConfigurationBuilder<>();
  }

  /**
   * This class builds a KafkaSourceConfiguration for KafkaSourceStream.
   */
  public static final class KafkaSourceConfigurationBuilder<K, V> extends MISTConfigurationBuilderImpl {

    /**
     * Required parameters for KafkaSourceStream.
     */
    private final String[] kafkaSourceParameters = {
        KafkaSourceParameters.KAFKA_TOPIC,
        KafkaSourceParameters.KAFKA_CONSUMER_CONFIG
    };

    /**
     * Optional parameters for KafkaSourceStream.
     */
    private final String[] kafkaSourceOptionalParameters = {
        SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION
    };

    private KafkaSourceConfigurationBuilder() {
      requiredParameters.addAll(Arrays.asList(kafkaSourceParameters));
      optionalParameters.addAll(Arrays.asList(kafkaSourceOptionalParameters));
    }

    /**
     * Tests that required parameters are set and builds the KafkaSourceConfiguration.
     * @return the configuration
     */
    public KafkaSourceConfiguration<K, V> build() {
      readyToBuild();
      return new KafkaSourceConfiguration<>(configMap);
    }

    /**
     * Sets the configuration for the kafka topic to the given topic.
     * @param topic the kafka topic given by user to monitor
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder<K, V> setTopic(final String topic) {
      set(KafkaSourceParameters.KAFKA_TOPIC, topic);
      return this;
    }

    /**
     * Sets the configuration for the kafka consumer to the given HashMap.
     * KafkaSource would create a KafkaConsumer according to this configuration.
     * This configuration should follows the ConsumerConfig policy defined in kafka.
     * @param consumerConfig the kafka consumer configuration HashMap given by users
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder<K, V> setConsumerConfig(final HashMap<String, Object> consumerConfig) {
      // configurability check
      final Set<String> configurable = ConsumerConfig.configNames();
      for (final Map.Entry<String, Object> entry : consumerConfig.entrySet()) {
        if (!configurable.contains(entry.getKey())) {
          throw new IllegalArgumentException(
              "Attempts to add an undefined kafka consumer configuration " + entry.getKey());
        }
      }

      set(KafkaSourceParameters.KAFKA_CONSUMER_CONFIG, consumerConfig);
      return this;
    }

    /**
     * Sets the configuration for the extracting timestamp in event-time input data function to the given function.
     * This is an optional setting for event-time processing.
     * @param function the function given by users which they want to set
     * @param <Vb> the type of kafka record value before timestamp extraction
     * @return the configured SourceBuilder
     */
    public <Vb> KafkaSourceConfigurationBuilder<K, V> setTimestampExtractionFunction(
        final MISTFunction<ConsumerRecord<K, Vb>, Tuple<ConsumerRecord<K, V>, Long>> function) {
      set(SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION, function);
      return this;
    }
  }
}