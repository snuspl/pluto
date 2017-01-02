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
package edu.snu.mist.core.task;

import edu.snu.mist.api.datastreams.configurations.*;
import edu.snu.mist.api.datastreams.utils.CountStringFunction;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.*;
import junit.framework.Assert;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
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
  @Test(expected=InjectionException.class)
  public void testInjectionExceptionOfDataGenerator() throws IOException, InjectionException {
    final Configuration conf = UnionOperatorConfiguration.CONF.build();
    generator.newDataGenerator(serializer.toString(conf), classLoader, urls);
  }

  @Test
  public void testSuccessOfNettyTextDataGenerator() throws Exception {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    final DataGenerator<String> dataGenerator =
        generator.newDataGenerator(serializer.toString(conf), classLoader, urls);
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
        generator.newDataGenerator(serializer.toString(conf), classLoader, urls);
    Assert.assertTrue(dataGenerator instanceof KafkaDataGenerator);
  }

  @Test(expected=InjectionException.class)
  public void testInjectionExceptionOfEventGenerator() throws IOException, InjectionException {
    final Configuration conf = UnionOperatorConfiguration.CONF.build();
    generator.newEventGenerator(serializer.toString(conf), classLoader, urls);
  }

  @Test
  public void testSuccessOfPeriodicEventGenerator() throws IOException, InjectionException {
    final Configuration conf = PeriodicWatermarkConfiguration.newBuilder()
        .setExpectedDelay(10)
        .setWatermarkPeriod(10)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(serializer.toString(conf), classLoader, urls);
    Assert.assertTrue(eg instanceof PeriodicEventGenerator);
  }

  @Test
  public void testSuccessOfPunctuatedEventGenerator() throws IOException, InjectionException {
    final Configuration conf = PunctuatedWatermarkConfiguration.newBuilder()
        .setParsingWatermarkFunction(input -> 1L)
        .setWatermarkPredicate(input -> true)
        .build().getConfiguration();
    final EventGenerator eg = generator.newEventGenerator(serializer.toString(conf), classLoader, urls);
    Assert.assertTrue(eg instanceof PunctuatedEventGenerator);
  }

  /**
   * Test if the physical object generator throws an InjectionException
   * when a wrong configuration is injected.
   */
  @Test(expected=InjectionException.class)
  public void testInjectionExceptionOfOperator() throws IOException, InjectionException {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    generator.newOperator("test", serializer.toString(conf), classLoader, urls);
  }

  /**
   * Get an operator from the configuration.
   * @param conf configuration
   * @return operator
   */
  private Operator getOperator(final Configuration conf) throws IOException, InjectionException {
    return generator.newOperator("test", serializer.toString(conf), classLoader, urls);
  }

  /**
   * Get an operator that has a single udf function.
   * @param operatorClass operator class
   * @param obj udf object
   * @return operator
   */
  private Operator getSingleUdfOperator(final Class<? extends Operator> operatorClass,
                                        final Serializable obj) throws IOException, InjectionException {
    final Configuration conf = SingleInputOperatorUDFConfiguration.CONF
        .set(SingleInputOperatorUDFConfiguration.OPERATOR, operatorClass)
        .set(SingleInputOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(obj))
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
  public void testSuccessOfApplystatefulOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ApplyStatefulOperator.class, new CountStringFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulOperator);
  }

  @Test
  public void testSuccessOfApplyStatefulWindowOperator() throws Exception {
    final Operator operator = getSingleUdfOperator(ApplyStatefulWindowOperator.class, new CountStringFunction());
    Assert.assertTrue(operator instanceof ApplyStatefulWindowOperator);
  }

  @Test
  public void testSuccessOfAggregateWindowOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(AggregateWindowOperator.class, p);
    Assert.assertTrue(operator instanceof AggregateWindowOperator);
  }

  @Test
  public void testSuccessOfFilterOperator() throws Exception {
    final MISTPredicate<String> p = input -> true;
    final Operator operator = getSingleUdfOperator(FilterOperator.class, p);
    Assert.assertTrue(operator instanceof FilterOperator);
  }

  @Test
  public void testSuccessOfFlatMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(FlatMapOperator.class, p);
    Assert.assertTrue(operator instanceof FlatMapOperator);
  }

  @Test
  public void testSuccessOfMapOperator() throws Exception {
    final MISTFunction<String, String> p = input -> input;
    final Operator operator = getSingleUdfOperator(MapOperator.class, p);
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
  @Test(expected=InjectionException.class)
  public void testInjectionExceptionOfSink() throws IOException, InjectionException {
    final Configuration conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(13666)
        .build().getConfiguration();
    generator.newSink("test", serializer.toString(conf), classLoader, urls);
  }

  @Test
  public void testSucessOfSink() throws IOException, InjectionException {
    final int port = 13667;
    final ServerSocket socket = new ServerSocket(port);
    final Configuration conf = TextSocketSinkConfiguration.CONF
        .set(TextSocketSinkConfiguration.SOCKET_HOST_ADDRESS, "localhost")
        .set(TextSocketSinkConfiguration.SOCKET_HOST_PORT, port)
        .build();
    final Sink sink = generator.newSink("test", serializer.toString(conf), classLoader, urls);
    Assert.assertTrue(sink instanceof NettyTextSink);
    socket.close();
  }
}
