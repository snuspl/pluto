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
package edu.snu.mist.client;

import edu.snu.mist.client.datastreams.ContinuousStream;
import edu.snu.mist.client.datastreams.configurations.KafkaSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.client.utils.TestParameters;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.functions.MISTFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This is the test class for MISTQueryBuilder.
 */
public class MISTQueryBuilderTest {

  /**
   * Test whether the serialization of the netty text source is correct.
   */
  @Test
  public void testNettyTextSourceSerializationWithoutUdf() {
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);

    final ContinuousStream<String> stream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    // check
    final Map<String, String> conf = stream.getConfiguration();
    Assert.assertEquals(TestParameters.HOST, conf.get(ConfKeys.NettySourceConf.SOURCE_ADDR.name()));
    Assert.assertEquals(String.valueOf(TestParameters.SERVER_PORT),
        conf.get(ConfKeys.NettySourceConf.SOURCE_PORT.name()));
  }

  /**
   * Test whether the serialization of the netty text source with udf is correct.
   */
  @Test
  public void testNettyTextSourceSerializationWithUdf() throws IOException {
    final MISTFunction<String, Tuple<String, Long>> timestampExtFunc = s -> new Tuple<>(s, 1L);
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);

    final ContinuousStream<String> stream =
        queryBuilder.socketTextStream(TextSocketSourceConfiguration.newBuilder()
            .setHostAddress(TestParameters.HOST)
            .setHostPort(TestParameters.SERVER_PORT)
            .setTimestampExtractionFunction(timestampExtFunc)
            .build());
    // check
    final Map<String, String> conf = stream.getConfiguration();
    Assert.assertEquals(TestParameters.HOST, conf.get(ConfKeys.NettySourceConf.SOURCE_ADDR.name()));
    Assert.assertEquals(String.valueOf(TestParameters.SERVER_PORT),
        conf.get(ConfKeys.NettySourceConf.SOURCE_PORT.name()));
    Assert.assertEquals(SerializeUtils.serializeToString(timestampExtFunc),
        conf.get(ConfKeys.SourceConf.TIMESTAMP_EXTRACT_FUNC.name()));
  }

  /**
   * This method tests a serialization of KafkaSourceStream.
   */
  @Test
  public void testKafkaSourceStreamSerialization()
      throws InjectionException, IOException, ClassNotFoundException {
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

    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);

    final ContinuousStream<ConsumerRecord<Integer, String>> kafkaSourceStream =
        queryBuilder.kafkaStream(KafkaSourceConfiguration.newBuilder()
            .setTopic(topic)
            .setConsumerConfig(consumerConfig)
            .build());

    // Check about source configuration
    final Map<String, String> conf = kafkaSourceStream.getConfiguration();
    Assert.assertEquals(topic, conf.get(ConfKeys.KafkaSourceConf.KAFKA_TOPIC.name()));
    Assert.assertEquals(consumerConfig, SerializeUtils.deserializeFromString(
        conf.get(ConfKeys.KafkaSourceConf.KAFKA_CONSUMER_CONFIG.name())));

    // Check about watermark configuration
    Assert.assertEquals("1000", conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_PERIOD.name()));
    Assert.assertEquals("0", conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_DELAY.name()));
  }

  @Test
  public void testMQTTSourceSerialization()
    throws InjectionException, IOException, ClassNotFoundException {
    final String expectedBrokerURI = "tcp://192.168.0.1:3386";
    final String expectedTopic = "region/system/subsystem/device/sensor";

    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);

    final ContinuousStream<MqttMessage> mqttSourceStream =
        queryBuilder.mqttStream(MQTTSourceConfiguration.newBuilder()
            .setBrokerURI(expectedBrokerURI)
            .setTopic(expectedTopic)
            .build());

    // Check source configuration
    final Map<String, String> conf = mqttSourceStream.getConfiguration();
    Assert.assertEquals(expectedBrokerURI, conf.get(ConfKeys.MQTTSourceConf.MQTT_SRC_BROKER_URI.name()));
    Assert.assertEquals(expectedTopic, conf.get(ConfKeys.MQTTSourceConf.MQTT_SRC_TOPIC.name()));

    // Check watermark configuration
    Assert.assertEquals("1000", conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_PERIOD.name()));
    Assert.assertEquals("0", conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_DELAY.name()));
  }
}
