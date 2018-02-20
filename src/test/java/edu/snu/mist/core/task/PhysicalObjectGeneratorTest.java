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
package edu.snu.mist.core.task;

import edu.snu.mist.api.datastreams.configurations.*;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.sinks.MqttSink;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.*;
import edu.snu.mist.common.utils.MqttUtils;
import edu.snu.mist.utils.OperatorTestUtils;
import io.moquette.server.Server;
import junit.framework.Assert;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.URL;
import java.util.HashMap;

public final class PhysicalObjectGeneratorTest {
  private final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
  private final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
  private final URL[] urls = new URL[0];
  private PhysicalObjectGenerator generator;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    generator = injector.getInstance(PhysicalObjectGenerator.class);
  }

  @After
  public void tearDown() throws Exception {
    generator.close();
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected = InjectionException.class)
  public void testInjectionExceptionOfDataGenerator() throws IOException, InjectionException {
    final Configuration conf = UnionOperatorConfiguration.CONF.build();
    generator.newDataGenerator(conf, classLoader);
  }

  @Test
  public void testSuccessOfNettyTextDataGenerator() throws Exception {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    final DataGenerator<String> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof NettyTextDataGenerator);
  }

  @Test
  public void testSuccessOfNettyTextDataGeneratorWithClassBinding() throws Exception {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .setTimestampExtractionFunction(OperatorTestUtils.TestNettyTimestampExtractFunc.class, funcConf)
        .build().getConfiguration();
    final DataGenerator<String> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof NettyTextDataGenerator);
  }

  @Test
  public void testSuccessOfKafkaDataGenerator() throws Exception {
    final HashMap<String, Object> kafkaConsumerConf = new HashMap<>();
    final Configuration conf = KafkaSourceConfiguration.newBuilder()
        .setTopic("localhost")
        .setConsumerConfig(kafkaConsumerConf)
        .build().getConfiguration();
    final DataGenerator dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof KafkaDataGenerator);
  }

  @Test
  public void testSuccessOfKafkaDataGeneratorWithClassBinding() throws Exception {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final HashMap<String, Object> kafkaConsumerConf = new HashMap<>();
    final Configuration conf = KafkaSourceConfiguration.newBuilder()
        .setTopic("localhost")
        .setConsumerConfig(kafkaConsumerConf)
        .setTimestampExtractionFunction(OperatorTestUtils.TestKafkaTimestampExtractFunc.class, funcConf)
        .build().getConfiguration();
    final DataGenerator dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof KafkaDataGenerator);
  }

  @Test
  public void testSuccessOfMQTTDataGenerator() throws Exception {
    final Configuration conf = MQTTSourceConfiguration.newBuilder()
        .setBrokerURI("tcp://localhost:12345")
        .setTopic("topic")
        .build().getConfiguration();
    final DataGenerator<MqttMessage> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof MQTTDataGenerator);

  }

  @Test
  public void testSuccessOfMQTTDataGeneratorWithClassBinding() throws Exception {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final Configuration conf = MQTTSourceConfiguration.newBuilder()
        .setBrokerURI("tcp://localhost:12345")
        .setTopic("topic")
        .setTimestampExtractionFunction(OperatorTestUtils.TestMQTTTimestampExtractFunc.class, funcConf)
        .build().getConfiguration();
    final DataGenerator<MqttMessage> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof MQTTDataGenerator);
  }

  @Test(expected = InjectionException.class)
  public void testInjectionExceptionOfEventGenerator() throws IOException, InjectionException {
    final Configuration conf = UnionOperatorConfiguration.CONF.build();
    generator.newEventGenerator(conf, classLoader);
  }

  @Test
  public void testSuccessOfPeriodicEventGenerator() throws IOException, InjectionException {
    final Configuration conf = PeriodicWatermarkConfiguration.newBuilder()
        .setExpectedDelay(10)
        .setWatermarkPeriod(10)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(conf, classLoader);
    Assert.assertTrue(eg instanceof PeriodicEventGenerator);
  }

  @Test
  public void testSuccessOfPunctuatedEventGenerator() throws IOException, InjectionException {
    final Configuration conf = PunctuatedWatermarkConfiguration.newBuilder()
        .setParsingWatermarkFunction(input -> 1L)
        .setWatermarkPredicate(input -> true)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(conf, classLoader);
    Assert.assertTrue(eg instanceof PunctuatedEventGenerator);
  }

  @Test
  public void testSuccessOfPunctuatedEventGeneratorWithClassBinding() throws IOException, InjectionException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final Configuration watermarkConf = PunctuatedWatermarkConfiguration.<String>newBuilder()
        .setParsingWatermarkFunction(OperatorTestUtils.TestWatermarkTimestampExtractFunc.class, funcConf)
        .setWatermarkPredicate(OperatorTestUtils.TestPredicate.class, funcConf)
        .build().getConfiguration();
    final Configuration srcConf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .setTimestampExtractionFunction(OperatorTestUtils.TestNettyTimestampExtractFunc.class, funcConf)
        .build().getConfiguration();
    final Configuration conf = Configurations.merge(watermarkConf, srcConf);
    final EventGenerator eg = generator.newEventGenerator(conf, classLoader);
    Assert.assertTrue(eg instanceof PunctuatedEventGenerator);
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected = InjectionException.class)
  public void testInjectionExceptionOfOperator() throws IOException, InjectionException {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    generator.newOperator(conf, classLoader);
  }

  /**
   * Get an operator from the configuration.
   * @param conf configuration
   * @return operator
   */
  private Operator getOperator(final Configuration conf) throws IOException, InjectionException {
    return generator.newOperator(conf, classLoader);
  }

  /**
   * Get an operator that has a single udf function.
   * @param operatorClass operator class
   * @param obj udf object
   * @return operator
   */
  private Operator getSingleUdfOperator(final Class<? extends Operator> operatorClass,
                                        final Serializable obj) throws IOException, InjectionException {
    final Configuration conf = OperatorUDFConfiguration.CONF
        .set(OperatorUDFConfiguration.OPERATOR, operatorClass)
        .set(OperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(obj))
        .build();
    return getOperator(conf);
  }

  /**
   * Get a window operator.
   * @param operatorClass widow operator class
   * @return window operator
   */
  private Operator getWindowOperator(final Class<? extends Operator> operatorClass)
      throws IOException, InjectionException {
    final Configuration conf = WindowOperatorConfiguration.CONF
        .set(WindowOperatorConfiguration.OPERATOR, operatorClass)
        .set(WindowOperatorConfiguration.WINDOW_SIZE, 10)
        .set(WindowOperatorConfiguration.WINDOW_INTERVAL, 2)
        .build();
    return getOperator(conf);
  }

  @Test
  public void testSuccessOfApplyStatefulOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ApplyStatefulOperator.class,
        new OperatorTestUtils.TestApplyStatefulFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulOperator);
  }

  @Test
  public void testSuccessOfApplyStatefulOperatorWithClassBinding() throws Exception {
    final Configuration conf = ApplyStatefulOperatorConfiguration.CONF
        .set(ApplyStatefulOperatorConfiguration.OPERATOR, ApplyStatefulOperator.class)
        .set(ApplyStatefulOperatorConfiguration.UDF, OperatorTestUtils.TestApplyStatefulFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof ApplyStatefulOperator);
  }

  @Test
  public void testSuccessOfApplyStatefulWindowOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ApplyStatefulWindowOperator.class,
        new OperatorTestUtils.TestApplyStatefulFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulWindowOperator);
  }

  @Test
  public void testSuccessOfApplyStatefulWindowOperatorWithClassBinding() throws Exception {
    final Configuration conf = ApplyStatefulOperatorConfiguration.CONF
        .set(ApplyStatefulOperatorConfiguration.OPERATOR, ApplyStatefulWindowOperator.class)
        .set(ApplyStatefulOperatorConfiguration.UDF, OperatorTestUtils.TestApplyStatefulFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof ApplyStatefulWindowOperator);
  }

  @Test
  public void testSuccessOfAggregateWindowOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(AggregateWindowOperator.class, p);
    Assert.assertTrue(operator instanceof AggregateWindowOperator);
  }

  @Test
  public void testSuccessOfAggregateWindowOperatorWithClassBinding() throws Exception {
    final Configuration conf = MISTFuncOperatorConfiguration.CONF
        .set(MISTFuncOperatorConfiguration.OPERATOR, AggregateWindowOperator.class)
        .set(MISTFuncOperatorConfiguration.UDF, OperatorTestUtils.TestFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof AggregateWindowOperator);
  }

  @Test
  public void testSuccessOfFilterOperator() throws Exception {
    final MISTPredicate<String> p = input -> true;
    final Operator operator = getSingleUdfOperator(FilterOperator.class, p);
    Assert.assertTrue(operator instanceof FilterOperator);
  }

  @Test
  public void testSuccessOfFilterOperatorWithClassBinding() throws Exception {
    final Configuration conf = FilterOperatorConfiguration.CONF
        .set(FilterOperatorConfiguration.MIST_PREDICATE, OperatorTestUtils.TestPredicate.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof FilterOperator);
  }

  @Test
  public void testSuccessOfFlatMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(FlatMapOperator.class, p);
    Assert.assertTrue(operator instanceof FlatMapOperator);
  }

  @Test
  public void testSuccessOfFlatMapOperatorWithClassBinding() throws Exception {
    final Configuration conf = MISTFuncOperatorConfiguration.CONF
        .set(MISTFuncOperatorConfiguration.OPERATOR, FlatMapOperator.class)
        .set(MISTFuncOperatorConfiguration.UDF, OperatorTestUtils.TestFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof FlatMapOperator);
  }

  @Test
  public void testSuccessOfMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(MapOperator.class, p);
    Assert.assertTrue(operator instanceof MapOperator);
  }

  @Test
  public void testSuccessOfMapOperatorWithClassBinding() throws Exception {
    final Configuration conf = MISTFuncOperatorConfiguration.CONF
        .set(MISTFuncOperatorConfiguration.OPERATOR, MapOperator.class)
        .set(MISTFuncOperatorConfiguration.UDF, OperatorTestUtils.TestFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof MapOperator);
  }

  @Test
  public void testSuccessOfReduceByKeyOperator() throws Exception {
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final Configuration conf = ReduceByKeyOperatorUDFConfiguration.CONF
        .set(ReduceByKeyOperatorUDFConfiguration.KEY_INDEX, 0)
        .set(ReduceByKeyOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(wordCountFunc))
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof ReduceByKeyOperator);
  }

  @Test
  public void testSuccessOfReduceByKeyOperatorWithClassBinding() throws Exception {
    final Configuration conf = ReduceByKeyOperatorConfiguration.CONF
        .set(ReduceByKeyOperatorConfiguration.KEY_INDEX, 0)
        .set(ReduceByKeyOperatorConfiguration.MIST_BI_FUNC, OperatorTestUtils.TestBiFunction.class)
        .build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof ReduceByKeyOperator);
  }

  @Test
  public void testUnionOperator() throws Exception {
    final Configuration conf = UnionOperatorConfiguration.CONF.build();
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof UnionOperator);
  }

  @Test
  public void testSuccessOfCountWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(CountWindowOperator.class);
    Assert.assertTrue(operator instanceof CountWindowOperator);
  }

  @Test
  public void testSuccessOfTimeWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(TimeWindowOperator.class);
    Assert.assertTrue(operator instanceof TimeWindowOperator);
  }

  @Test
  public void testSuccessOfSessionWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(SessionWindowOperator.class);
    Assert.assertTrue(operator instanceof SessionWindowOperator);
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected = InjectionException.class)
  public void testInjectionExceptionOfSink() throws IOException, InjectionException {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    generator.newSink(conf, classLoader);
  }

  @Test
  public void testSuccessOfSink() throws IOException, InjectionException {
    final int port = 13667;
    final ServerSocket socket = new ServerSocket(port);
    final Configuration conf = TextSocketSinkConfiguration.CONF
        .set(TextSocketSinkConfiguration.SOCKET_HOST_ADDRESS, "localhost")
        .set(TextSocketSinkConfiguration.SOCKET_HOST_PORT, port)
        .build();
    final Sink sink = generator.newSink(conf, classLoader);
    Assert.assertTrue(sink instanceof NettyTextSink);
    socket.close();
  }

  /**
   * Test if the mqtt sink is created successfully.
   */
  @Test
  public void testSuccessOfMqttSink() throws IOException, InjectionException {
    final Server mqttBroker = MqttUtils.createMqttBroker();
    final String topic = "mqttTest";
    final Configuration conf = MqttSinkConfiguration.CONF
        .set(MqttSinkConfiguration.MQTT_BROKER_URI, MqttUtils.BROKER_URI)
        .set(MqttSinkConfiguration.MQTT_TOPIC, topic)
        .build();
    final Sink sink = generator.newSink(conf, classLoader);
    Assert.assertTrue(sink instanceof MqttSink);
    mqttBroker.stopServer();
  }
}
