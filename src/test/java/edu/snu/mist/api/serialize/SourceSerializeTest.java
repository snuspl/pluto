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
package edu.snu.mist.api.serialize;

import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.sources.KafkaSourceStream;
import edu.snu.mist.api.sources.builder.KafkaSourceConfiguration;
import edu.snu.mist.api.sources.parameters.KafkaSourceParameters;
import edu.snu.mist.api.sources.parameters.PeriodicWatermarkParameters;
import edu.snu.mist.formats.avro.*;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static edu.snu.mist.api.serialize.utils.SerializationTestUtils.deserializeByteBuffer;

/**
 * This is the test class for serializing source into avro vertex.
 */
public class SourceSerializeTest {

  /**
   * This method tests a serialization of KafkaSourceStream.
   */
  @Test
  public void testKafkaSourceStreamSerialization() {
    final String topic = "kafkaTopic";
    final String serverKey = "bootstrap.servers";
    final String serverValue = "localhost:8888";
    final String groupKey = "group.id";
    final String groupValue = "test_group";
    final String valueDeserializerKey = "value.deserializer";
    final String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    final HashMap<String, Object> consumerConfig = new HashMap<>();
    consumerConfig.put(serverKey, serverValue);
    consumerConfig.put(groupKey, groupValue);
    consumerConfig.put(valueDeserializerKey, valueDeserializer);

    final KafkaSourceConfiguration<Integer, String> kafkaSourceConfiguration =
        KafkaSourceConfiguration.<Integer, String>newBuilder()
            .setTopic(topic)
            .setConsumerConfig(consumerConfig)
            .build();

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final KafkaSourceStream<Integer, String> kafkaSourceStream =
        queryBuilder.kafkaStream(kafkaSourceConfiguration);

    final Vertex serializedVertex = kafkaSourceStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.SOURCE);
    final SourceInfo sourceInfo = (SourceInfo) serializedVertex.getAttributes();
    Assert.assertEquals(sourceInfo.getSourceType(), SourceTypeEnum.KAFKA_SOURCE);
    Assert.assertEquals(sourceInfo.getWatermarkType(), WatermarkTypeEnum.PERIODIC);

    // Check about source configuration
    final Map<String, Object> sourceConf = sourceInfo.getSourceConfiguration();
    Assert.assertEquals(sourceConf.get(KafkaSourceParameters.KAFKA_TOPIC), topic);
    final Map<String, Object> kafkaConsumerConf =
        deserializeByteBuffer((ByteBuffer) sourceConf.get(KafkaSourceParameters.KAFKA_CONSUMER_CONFIG));
    Assert.assertEquals(kafkaConsumerConf.get(serverKey), serverValue);
    Assert.assertEquals(kafkaConsumerConf.get(groupKey), groupValue);
    Assert.assertEquals(kafkaConsumerConf.get(valueDeserializerKey), valueDeserializer);

    // Check about watermark configuration
    final Map<String, Object> watermarkConf = sourceInfo.getWatermarkConfiguration();
    Assert.assertEquals(watermarkConf.get(PeriodicWatermarkParameters.PERIOD), 100);
    Assert.assertEquals(watermarkConf.get(PeriodicWatermarkParameters.EXPECTED_DELAY), 0);
  }
}
