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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * The test class for KafkaSourceConfiguration.
 */
public class KafkaSourceConfigurationTest {

  /**
   * Test for KafkaSource configuration builder.
   */
  @Test
  public void testKafkaSourceConfBuilder() {
    /**
     * Configuration values for KafkaSource.
     */
    final String topic = "kafkaTopic";
    final int pollTimeout = 10;
    final HashMap<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put("bootstrap.servers", "localhost:8888");
    consumerConfig.put("group.id", "test_group");
    final MISTFunction<ConsumerRecord<Integer, String>, Tuple<ConsumerRecord<Integer, String>, Long>>
        timestampExtractionFunction =
        input -> {
          final String[] split = (input.value()).split(":");
          return new Tuple<>(
              new ConsumerRecord<>(input.topic(), input.partition(), input.offset(), input.key(), split[0]),
              Long.parseLong(split[1]));
        };

    final KafkaSourceConfiguration<Integer, String> kafkaSourceConfiguration =
        KafkaSourceConfiguration.<Integer, String>newBuilder()
        .setTopic(topic)
        .setConsumerConfig(consumerConfig)
        .setTimestampExtractionFunction(timestampExtractionFunction)
        .build();

    Assert.assertEquals(topic,
        kafkaSourceConfiguration.getConfigurationValue(KafkaSourceParameters.KAFKA_TOPIC));
    Assert.assertEquals(consumerConfig,
        kafkaSourceConfiguration.getConfigurationValue(KafkaSourceParameters.KAFKA_CONSUMER_CONFIG));
    Assert.assertEquals(timestampExtractionFunction,
        kafkaSourceConfiguration.getConfigurationValue(SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION));
  }
}
