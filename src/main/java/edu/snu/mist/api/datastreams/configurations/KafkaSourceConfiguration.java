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
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the kafka source configuration.
 */
public final class KafkaSourceConfiguration extends ConfigurationModuleBuilder {

  /**
   * The kafka topic.
   */
  public static final RequiredParameter<String> KAFKA_TOPIC = new RequiredParameter<>();

  /**
   * The configuration of the kafka consumer.
   */
  public static final RequiredParameter<String> KAFKA_CONSUMER_CONFIG = new RequiredParameter<>();

  /**
   * The parameter for timestamp extract object.
   */
  public static final OptionalParameter<String> TIMESTAMP_EXTRACT_OBJECT = new OptionalParameter<>();

  /**
   * The parameter for timestamp extract function.
   */
  public static final OptionalImpl<MISTFunction> TIMESTAMP_EXTRACT_FUNC = new OptionalImpl<>();

  private static final ConfigurationModule CONF = new KafkaSourceConfiguration()
      .bindNamedParameter(KafkaTopic.class, KAFKA_TOPIC)
      .bindNamedParameter(SerializedKafkaConfig.class, KAFKA_CONSUMER_CONFIG)
      .bindNamedParameter(SerializedTimestampExtractUdf.class, TIMESTAMP_EXTRACT_OBJECT)
      .bindImplementation(MISTFunction.class, TIMESTAMP_EXTRACT_FUNC)
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
    private Class<? extends MISTFunction<ConsumerRecord, Tuple<ConsumerRecord, Long>>> extractFuncClass;
    private Configuration extractFuncConf;

    /**
     * Tests that required parameters are set and builds the KafkaSourceConfiguration.
     * @return the configuration
     */
    public SourceConfiguration build() {

      if (extractFunc != null && extractFuncClass != null) {
        throw new IllegalArgumentException("Cannot bind both extractFunc and extractFuncClass");
      }

      try {
        if (extractFunc == null && extractFuncClass == null) {
          // No extract function is set
          return new SourceConfiguration(CONF
              .set(KAFKA_TOPIC, kafkaTopic)
              .set(KAFKA_CONSUMER_CONFIG, SerializeUtils.serializeToString(kafkaConfig))
              .build(), SourceConfiguration.SourceType.KAFKA);
        } else if (extractFunc != null) {
          // Lambda object is set
          return new SourceConfiguration(CONF
              .set(KAFKA_TOPIC, kafkaTopic)
              .set(KAFKA_CONSUMER_CONFIG, SerializeUtils.serializeToString(kafkaConfig))
              .set(TIMESTAMP_EXTRACT_OBJECT, SerializeUtils.serializeToString(extractFunc))
              .build(), SourceConfiguration.SourceType.KAFKA);
        } else {
          // UDF class is set
          return new SourceConfiguration(Configurations.merge(CONF
              .set(KAFKA_TOPIC, kafkaTopic)
              .set(KAFKA_CONSUMER_CONFIG, SerializeUtils.serializeToString(kafkaConfig))
              .set(TIMESTAMP_EXTRACT_FUNC, extractFuncClass)
              .build(), extractFuncConf), SourceConfiguration.SourceType.KAFKA);
        }
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

    /**
     * Sets the timestamp extract function by binding classes and its configuration.
     * This is an optional setting for event-time processing.
     * @param functionClass the class of the timestamp extract function
     * @param funcConf the configuration of the function
     * @return the configured SourceBuilder
     */
    public KafkaSourceConfigurationBuilder setTimestampExtractionFunction(
        final Class<? extends MISTFunction<ConsumerRecord, Tuple<ConsumerRecord, Long>>> functionClass,
        final Configuration funcConf) {
      extractFuncClass = functionClass;
      extractFuncConf = funcConf;
      return this;
    }
  }
}