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

import edu.snu.mist.client.datastreams.configurations.*;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.core.operators.*;
import edu.snu.mist.core.operators.window.*;
import edu.snu.mist.core.parameters.TaskHostname;
import edu.snu.mist.core.sinks.MqttSink;
import edu.snu.mist.core.sinks.NettyTextSink;
import edu.snu.mist.core.sinks.Sink;
import edu.snu.mist.core.sources.*;
import edu.snu.mist.core.utils.MqttUtils;
import edu.snu.mist.core.utils.UDFTestUtils;
import io.moquette.server.Server;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
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
import java.util.Map;

public final class PhysicalObjectGeneratorTest {
  private final AvroConfigurationSerializer serializer = new AvroConfigurationSerializer();
  private final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
  private final URL[] urls = new URL[0];
  private PhysicalObjectGenerator generator;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(TaskHostname.class, "127.0.0.1");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
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
  @Test(expected = Exception.class)
  public void testInjectionExceptionOfDataGenerator() throws IOException, ClassNotFoundException {
    final Map<String, String> map = new HashMap<>();
    generator.newDataGenerator(map, classLoader);
  }

  @Test
  public void testSuccessOfNettyTextDataGenerator() throws Exception {
    final Map<String, String> conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    final DataGenerator<String> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof NettyTextDataGenerator);
  }

  @Test
  public void testSuccessOfKafkaDataGenerator() throws Exception {
    final HashMap<String, Object> kafkaConsumerConf = new HashMap<>();
    final Map<String, String> conf = KafkaSourceConfiguration.newBuilder()
        .setTopic("localhost")
        .setConsumerConfig(kafkaConsumerConf)
        .build().getConfiguration();
    final DataGenerator dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof KafkaDataGenerator);
  }

  @Test
  public void testSuccessOfMQTTDataGenerator() throws Exception {
    final Map<String, String> conf = MQTTSourceConfiguration.newBuilder()
        .setBrokerURI("tcp://localhost:12345")
        .setTopic("topic")
        .build().getConfiguration();
    final DataGenerator<MqttMessage> dataGenerator =
        generator.newDataGenerator(conf, classLoader);
    Assert.assertTrue(dataGenerator instanceof MQTTDataGenerator);

  }

  @Test(expected = Exception.class)
  public void testInjectionExceptionOfEventGenerator() throws IOException, ClassNotFoundException {
    generator.newEventGenerator(new HashMap<>(), classLoader);
  }

  @Test
  public void testSuccessOfPeriodicEventGenerator() throws IOException, ClassNotFoundException {
    final Map<String, String> conf = PeriodicWatermarkConfiguration.newBuilder()
        .setExpectedDelay(10)
        .setWatermarkPeriod(10)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(conf, classLoader);
    Assert.assertTrue(eg instanceof PeriodicEventGenerator);
  }

  @Test
  public void testSuccessOfPunctuatedEventGenerator() throws IOException, ClassNotFoundException {
    final Map<String, String> conf = PunctuatedWatermarkConfiguration.newBuilder()
        .setParsingWatermarkFunction(input -> 1L)
        .setWatermarkPredicate(input -> true)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(conf, classLoader);
    Assert.assertTrue(eg instanceof PunctuatedEventGenerator);
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected = Exception.class)
  public void testInjectionExceptionOfOperator() throws IOException, ClassNotFoundException {
    final Map<String, String> conf = TextSocketSourceConfiguration.newBuilder()
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
  private Operator getOperator(final Map<String, String> conf) throws IOException, ClassNotFoundException {
    return generator.newOperator(conf, classLoader);
  }

  /**
   * Get an operator that has a single udf function.
   * @param operatorType operator class
   * @param obj udf object
   * @return operator
   */
  private Operator getSingleUdfOperator(final ConfValues.OperatorType operatorType,
                                        final Serializable obj) throws IOException, ClassNotFoundException {
    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.OperatorConf.OP_TYPE.name(), operatorType.name());
        put(ConfKeys.OperatorConf.UDF_STRING.name(), SerializeUtils.serializeToString(obj));
      }
    };
    return getOperator(conf);
  }

  /**
   * Get a window operator.
   * @param operatorType widow operator class
   * @return window operator
   */
  private Operator getWindowOperator(final ConfValues.OperatorType operatorType)
      throws IOException, ClassNotFoundException {
    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.OperatorConf.OP_TYPE.name(), operatorType.name());
        put(ConfKeys.WindowOperator.WINDOW_SIZE.name(), "10");
        put(ConfKeys.WindowOperator.WINDOW_INTERVAL.name(), "2");
      }
    };
    return getOperator(conf);
  }

  @Test
  public void testSuccessOfApplyStatefulOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.APPLY_STATEFUL,
        new UDFTestUtils.TestApplyStatefulFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulOperator);
  }

  @Test
  public void testSuccessOfApplyStatefulWindowOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.APPLY_STATEFUL_WINDOW,
        new UDFTestUtils.TestApplyStatefulFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulWindowOperator);
  }

  @Test
  public void testSuccessOfAggregateWindowOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.AGGREGATE_WINDOW, p);
    Assert.assertTrue(operator instanceof AggregateWindowOperator);
  }

  @Test
  public void testSuccessOfFilterOperator() throws Exception {
    final MISTPredicate<String> p = input -> true;
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.FILTER, p);
    Assert.assertTrue(operator instanceof FilterOperator);
  }

  @Test
  public void testSuccessOfFlatMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.FLAT_MAP, p);
    Assert.assertTrue(operator instanceof FlatMapOperator);
  }

  @Test
  public void testSuccessOfMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(ConfValues.OperatorType.MAP, p);
    Assert.assertTrue(operator instanceof MapOperator);
  }

  @Test
  public void testSuccessOfReduceByKeyOperator() throws Exception {
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.REDUCE_BY_KEY.name());
        put(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name(), "0");
        put(ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name(), SerializeUtils.serializeToString(wordCountFunc));
      }
    };
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof ReduceByKeyOperator);
  }

  @Test
  public void testUnionOperator() throws Exception {
    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.UNION.name());
      }
    };
    final Operator operator = getOperator(conf);
    Assert.assertTrue(operator instanceof UnionOperator);
  }

  @Test
  public void testSuccessOfCountWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(ConfValues.OperatorType.COUNT_WINDOW);
    Assert.assertTrue(operator instanceof CountWindowOperator);
  }

  @Test
  public void testSuccessOfTimeWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(ConfValues.OperatorType.TIME_WINDOW);
    Assert.assertTrue(operator instanceof TimeWindowOperator);
  }

  @Test
  public void testSuccessOfSessionWindowOperator() throws Exception {
    final Operator operator = getWindowOperator(ConfValues.OperatorType.SESSION_WINDOW);
    Assert.assertTrue(operator instanceof SessionWindowOperator);
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected = Exception.class)
  public void testInjectionExceptionOfSink() throws IOException {
    final Map<String, String> conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    generator.newSink(conf, classLoader);
  }

  @Test
  public void testSuccessOfSink() throws IOException, InjectionException {
    final int port = 13667;
    final ServerSocket socket = new ServerSocket(port);

    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.SinkConf.SINK_TYPE.name(), ConfValues.SinkType.NETTY.name());
        put(ConfKeys.NettySink.SINK_ADDRESS.name(), "localhost");
        put(ConfKeys.NettySink.SINK_PORT.name(), String.valueOf(port));
      }
    };

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

    final Map<String, String> conf = new HashMap<String, String>() {
      {
        put(ConfKeys.SinkConf.SINK_TYPE.name(), ConfValues.SinkType.MQTT.name());
        put(ConfKeys.MqttSink.MQTT_SINK_BROKER_URI.name(), MqttUtils.BROKER_URI);
        put(ConfKeys.MqttSink.MQTT_SINK_TOPIC.name(), topic);
      }
    };

    final Sink sink = generator.newSink(conf, classLoader);
    Assert.assertTrue(sink instanceof MqttSink);
    mqttBroker.stopServer();
  }
}
