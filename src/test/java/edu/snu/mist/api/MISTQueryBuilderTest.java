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
package edu.snu.mist.api;

import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.KafkaSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.*;
import edu.snu.mist.utils.OperatorTestUtils;
import edu.snu.mist.utils.TestParameters;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
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
  public void testNettyTextSourceSerializationWithoutUdf() throws InjectionException {
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder(TestParameters.SUPER_GROUP_ID, TestParameters.SUB_GROUP_ID);
    final ContinuousStream<String> stream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    // check
    final Injector injector = Tang.Factory.getTang().newInjector(stream.getConfiguration());
    final String deHost = injector.getNamedInstance(SocketServerIp.class);
    final int dePort = injector.getNamedInstance(SocketServerPort.class);
    Assert.assertEquals(TestParameters.HOST, deHost);
    Assert.assertEquals(TestParameters.SERVER_PORT, dePort);
  }

  /**
   * Test whether the serialization of the netty text source with udf is correct.
   */
  @Test
  public void testNettyTextSourceSerializationWithUdf()
      throws InjectionException, IOException, ClassNotFoundException {
    final MISTFunction<String, Tuple<String, Long>> timestampExtFunc = s -> new Tuple<>(s, 1L);
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder(TestParameters.SUPER_GROUP_ID, TestParameters.SUB_GROUP_ID);
    final ContinuousStream<String> stream =
        queryBuilder.socketTextStream(TextSocketSourceConfiguration.newBuilder()
            .setHostAddress(TestParameters.HOST)
            .setHostPort(TestParameters.SERVER_PORT)
            .setTimestampExtractionFunction(timestampExtFunc)
            .build());
    // check
    final Injector injector = Tang.Factory.getTang().newInjector(stream.getConfiguration());
    final String deHost = injector.getNamedInstance(SocketServerIp.class);
    final int dePort = injector.getNamedInstance(SocketServerPort.class);
    final String seTimeFunc = injector.getNamedInstance(SerializedTimestampExtractUdf.class);
    Assert.assertEquals(TestParameters.HOST, deHost);
    Assert.assertEquals(TestParameters.SERVER_PORT, dePort);
    Assert.assertEquals(SerializeUtils.serializeToString(timestampExtFunc), seTimeFunc);
  }

  /**
   * Test whether the serialization of the netty text source with binding the udf class is correct.
   */
  @Test
  public void testNettyTextSourceSerializationWithUdfClassBinding()
      throws InjectionException, IOException, ClassNotFoundException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final MISTFunction<String, Tuple<String, Long>> timestampExtFunc = s -> new Tuple<>(s, 1L);
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder(TestParameters.SUPER_GROUP_ID, TestParameters.SUB_GROUP_ID);
    final ContinuousStream<String> stream =
        queryBuilder.socketTextStream(TextSocketSourceConfiguration.newBuilder()
            .setHostAddress(TestParameters.HOST)
            .setHostPort(TestParameters.SERVER_PORT)
            .setTimestampExtractionFunction(OperatorTestUtils.TestNettyTimestampExtractFunc.class, funcConf)
            .build());
    // check
    final Injector injector = Tang.Factory.getTang().newInjector(stream.getConfiguration());
    final String deHost = injector.getNamedInstance(SocketServerIp.class);
    final int dePort = injector.getNamedInstance(SocketServerPort.class);
    final MISTFunction func = injector.getInstance(MISTFunction.class);
    Assert.assertEquals(TestParameters.HOST, deHost);
    Assert.assertEquals(TestParameters.SERVER_PORT, dePort);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestNettyTimestampExtractFunc);
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
        new MISTQueryBuilder(TestParameters.SUPER_GROUP_ID, TestParameters.SUB_GROUP_ID);
    final ContinuousStream<ConsumerRecord<Integer, String>> kafkaSourceStream =
        queryBuilder.kafkaStream(KafkaSourceConfiguration.newBuilder()
            .setTopic(topic)
            .setConsumerConfig(consumerConfig)
            .build());

    // Check about source configuration
    final Injector injector = Tang.Factory.getTang().newInjector(kafkaSourceStream.getConfiguration());
    final String desTopic = injector.getNamedInstance(KafkaTopic.class);
    final String seConsumerConfig = injector.getNamedInstance(SerializedKafkaConfig.class);
    final Map<String, Object> deConsumerConfig = SerializeUtils.deserializeFromString(seConsumerConfig);
    Assert.assertEquals(topic, desTopic);
    Assert.assertEquals(consumerConfig, deConsumerConfig);

    // Check about watermark configuration
    final long period = injector.getNamedInstance(PeriodicWatermarkPeriod.class);
    final long delay = injector.getNamedInstance(PeriodicWatermarkDelay.class);
    Assert.assertEquals(1000, period);
    Assert.assertEquals(0, delay);
  }

  @Test
  public void testMQTTSourceSerialization()
    throws InjectionException, IOException, ClassNotFoundException {
    final String expectedBrokerURI = "tcp://192.168.0.1:3386";
    final String expectedTopic = "region/system/subsystem/device/sensor";

    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder(TestParameters.SUPER_GROUP_ID, TestParameters.SUB_GROUP_ID);
    final ContinuousStream<MqttMessage> mqttSourceStream =
        queryBuilder.mqttStream(MQTTSourceConfiguration.newBuilder()
            .setBrokerURI(expectedBrokerURI)
            .setTopic(expectedTopic)
            .build());

    // Check source configuration
    final Injector injector = Tang.Factory.getTang().newInjector(mqttSourceStream.getConfiguration());
    final String resultBrokerAddress = injector.getNamedInstance(MQTTBrokerURI.class);
    final String resultTopic = injector.getNamedInstance(MQTTTopic.class);
    Assert.assertEquals(expectedBrokerURI, resultBrokerAddress);
    Assert.assertEquals(expectedTopic, resultTopic);

    // Check watermark configuration
    final long period = injector.getNamedInstance(PeriodicWatermarkPeriod.class);
    final long delay = injector.getNamedInstance(PeriodicWatermarkDelay.class);
    Assert.assertEquals(1000, period);
    Assert.assertEquals(0, delay);
  }
}
