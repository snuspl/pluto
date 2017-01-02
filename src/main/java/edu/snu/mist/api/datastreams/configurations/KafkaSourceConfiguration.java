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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.KafkaTopic;
import edu.snu.mist.common.parameters.SerializedKafkaConfig;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.KafkaDataGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.OptionalParameter;
import org.apache.reef.tang.formats.RequiredParameter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the kafka source configuration.
 */
public final class KafkaSourceConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> KAFKA_TOPIC = new RequiredParameter<>();
  public static final RequiredParameter<String> KAFKA_CONSUMER_CONFIG = new RequiredParameter<>();
  public static final OptionalParameter<String> TIMESTAMP_EXTRACT_OBJECT = new OptionalParameter<>();

  public static final ConfigurationModule CONF = new KafkaSourceConfiguration()
      .bindNamedParameter(KafkaTopic.class, KAFKA_TOPIC)
      .bindNamedParameter(SerializedKafkaConfig.class, KAFKA_CONSUMER_CONFIG)
      .bindNamedParameter(SerializedTimestampExtractUdf.class, TIMESTAMP_EXTRACT_OBJECT)
      .bindImplementation(DataGenerator.class, KafkaDataGenerator.class)
      .build();

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
      try {
        return new SourceConfiguration(CONF
            .set(KAFKA_TOPIC, kafkaTopic)
            .set(KAFKA_CONSUMER_CONFIG, SerializeUtils.serializeToString(kafkaConfig))
            .set(TIMESTAMP_EXTRACT_OBJECT, SerializeUtils.serializeToString(extractFunc))
            .build(), SourceConfiguration.SourceType.KAFKA);
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
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