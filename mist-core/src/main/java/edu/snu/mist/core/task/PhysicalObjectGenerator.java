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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.cep.CepEventPattern;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.functions.WatermarkTimestampFunction;
import edu.snu.mist.core.operators.*;
import edu.snu.mist.core.sources.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.core.operators.window.*;
import edu.snu.mist.core.shared.KafkaSharedResource;
import edu.snu.mist.core.shared.MQTTResource;
import edu.snu.mist.core.shared.NettySharedResource;
import edu.snu.mist.core.sinks.MqttSink;
import edu.snu.mist.core.sinks.NettyTextSink;
import edu.snu.mist.core.sinks.Sink;
import edu.snu.mist.core.sources.*;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.MqttException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a helper class that creates physical objects (sources, operators, sinks)
 * from serialized configurations.
 */
public final class PhysicalObjectGenerator implements AutoCloseable {

  /**
   * Scheduled executor for event generators.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * Time unit for watermarks.
   */
  private final TimeUnit watermarkTimeUnit = TimeUnit.MILLISECONDS;

  /**
   * Kafka shared resources.
   */
  private final KafkaSharedResource kafkaSharedResource;

  /**
   * Netty shared resources.
   */
  private final NettySharedResource nettySharedResource;

  /**
   * MQTT shared resources.
   */
  private final MQTTResource mqttSharedResource;

  /**
   * The checkpoint period.
   */
  private final long checkpointPeriod;

  /**
   * Identifier factory.
   */
  private final StringIdentifierFactory identifierFactory;

  @Inject
  private PhysicalObjectGenerator(final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final KafkaSharedResource kafkaSharedResource,
                                  final NettySharedResource nettySharedResource,
                                  final MQTTResource mqttSharedResource,
                                  @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                                  final StringIdentifierFactory identifierFactory) {
    this.scheduler = schedulerWrapper.getScheduler();
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
    this.mqttSharedResource = mqttSharedResource;
    this.checkpointPeriod = checkpointPeriod;
    this.identifierFactory = identifierFactory;
  }

  /**
   * Get a new event generator.
   * @param conf configuration
   * @param classLoader external class loader
   * @param <T> event type
   * @return event generator
   */
  @SuppressWarnings("unchecked")
  public <T> EventGenerator<T> newEventGenerator(
      final Map<String, String> conf,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final String type = conf.get(ConfKeys.Watermark.EVENT_GENERATOR.name());

    final String tefString = conf.get(ConfKeys.SourceConf.TIMESTAMP_EXTRACT_FUNC.name());
    final MISTFunction timestampExtractFunc;
    if (tefString == null) {
      timestampExtractFunc = null;
    } else {
      timestampExtractFunc = SerializeUtils.deserializeFromString(
          conf.get(ConfKeys.SourceConf.TIMESTAMP_EXTRACT_FUNC.name()), classLoader);
    }

    if (type.equals(ConfValues.EventGeneratorType.PERIODIC_EVENT_GEN.name())) {
      // periodic event generator
      final long period = Long.valueOf(conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_PERIOD.name()));
      final long delay = Long.valueOf(conf.get(ConfKeys.Watermark.PERIODIC_WATERMARK_DELAY.name()));
      return new PeriodicEventGenerator(
          timestampExtractFunc, period, checkpointPeriod, delay, watermarkTimeUnit, scheduler);
    } else if (type.equals(ConfValues.EventGeneratorType.PUNCTUATED_EVENT_GEN.name())) {
      // punctuated event generator
      final MISTPredicate watermarkPredicate = SerializeUtils.deserializeFromString(
          conf.get(ConfKeys.Watermark.WATERMARK_PREDICATE.name()), classLoader);
      final WatermarkTimestampFunction tf = SerializeUtils.deserializeFromString(
          conf.get(ConfKeys.Watermark.TIMESTAMP_PARSE_OBJECT.name()), classLoader);
      return new PunctuatedEventGenerator(
          timestampExtractFunc, watermarkPredicate, tf, checkpointPeriod, watermarkTimeUnit, scheduler);
    } else {
      throw new RuntimeException("Invalid event generator: " + type);
    }
  }

  /**
   * Get a new data generator.
   * @param conf configuration
   * @param classLoader external class loader
   * @return data generator
   */
  @SuppressWarnings("unchecked")
  public DataGenerator newDataGenerator(
      final Map<String, String> conf,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final String type = conf.get(ConfKeys.SourceConf.SOURCE_TYPE.name());

    if (type.equals(ConfValues.SourceType.KAFKA.name())) {
      // kafka source
      final String topic = conf.get(ConfKeys.KafkaSourceConf.KAFKA_TOPIC.name());
      final Map<String, Object> kafkaConsumerConf = SerializeUtils.deserializeFromString(
          conf.get(ConfKeys.KafkaSourceConf.KAFKA_CONSUMER_CONFIG.name()), classLoader);
      return new KafkaDataGenerator(topic, kafkaConsumerConf, kafkaSharedResource);
    } else if (type.equals(ConfValues.SourceType.NETTY.name())) {
      // netty source
      final String addr = conf.get(ConfKeys.NettySourceConf.SOURCE_ADDR.name());
      final int port = Integer.valueOf(conf.get(ConfKeys.NettySourceConf.SOURCE_PORT.name()));
      return new NettyTextDataGenerator(addr, port, nettySharedResource);
    } else if (type.equals(ConfValues.SourceType.MQTT.name())) {
      // mqtt source
      final String brokerURI = conf.get(ConfKeys.MQTTSourceConf.MQTT_SRC_BROKER_URI.name());
      final String topic = conf.get(ConfKeys.MQTTSourceConf.MQTT_SRC_TOPIC.name());
      return mqttSharedResource.getDataGenerator(brokerURI, topic);
    } else {
      throw new RuntimeException("Invalid source type: " + type);
    }
  }

  /**
   * Get a new operator.
   * @param conf configuration
   * @param classLoader external class loader
   * @return new operator
   */
  @SuppressWarnings("unchecked")
  public Operator newOperator(
      final Map<String, String> conf,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final String type = conf.get(ConfKeys.OperatorConf.OP_TYPE.name());

    if (type.equals(ConfValues.OperatorType.MAP.name())) {

      return new MapOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.FILTER.name())) {

      return new FilterOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.FLAT_MAP.name())) {

      return new FlatMapOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.APPLY_STATEFUL.name())) {

      return new ApplyStatefulOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.STATE_TRANSITION.name())) {

      final String initialState = conf.get(ConfKeys.StateTransitionOperator.INITIAL_STATE.name());
      final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable =
          getObject(conf, ConfKeys.StateTransitionOperator.STATE_TABLE.name(), classLoader);
      final Set<String> finalState =
          getObject(conf, ConfKeys.StateTransitionOperator.FINAL_STATE.name(), classLoader);
      return new StateTransitionOperator(initialState, finalState, stateTable);

    } else if (type.equals(ConfValues.OperatorType.CEP.name())) {

      final List<CepEventPattern> cepEventPatterns =
          getObject(conf, ConfKeys.CepOperator.CEP_EVENT.name(), classLoader);
      final long windowTime = Long.valueOf(conf.get(ConfKeys.CepOperator.WINDOW_TIME.name()));
      return new CepOperator(cepEventPatterns, windowTime);

    } else if (type.equals(ConfValues.OperatorType.REDUCE_BY_KEY.name())) {

      final int keyFieldNum = Integer.valueOf(conf.get(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name()));
      final MISTBiFunction reduceFunc =
          getObject(conf, ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name(), classLoader);
      return new ReduceByKeyOperator(keyFieldNum, reduceFunc);

    } else if (type.equals(ConfValues.OperatorType.UNION.name())) {

      return new UnionOperator();

    } else if (type.equals(ConfValues.OperatorType.TIME_WINDOW.name())) {

      final int windowSize = Integer.valueOf(conf.get(ConfKeys.WindowOperator.WINDOW_SIZE.name()));
      final int windowInterval = Integer.valueOf(conf.get(ConfKeys.WindowOperator.WINDOW_INTERVAL.name()));
      return new TimeWindowOperator(windowSize, windowInterval);

    } else if (type.equals(ConfValues.OperatorType.COUNT_WINDOW.name())) {

      final int windowSize = Integer.valueOf(conf.get(ConfKeys.WindowOperator.WINDOW_SIZE.name()));
      final int windowInterval = Integer.valueOf(conf.get(ConfKeys.WindowOperator.WINDOW_INTERVAL.name()));
      return new CountWindowOperator(windowSize, windowInterval);

    } else if (type.equals(ConfValues.OperatorType.SESSION_WINDOW.name())) {

      final int windowInterval = Integer.valueOf(conf.get(ConfKeys.WindowOperator.WINDOW_INTERVAL.name()));
      return new SessionWindowOperator(windowInterval);

    } else if (type.equals(ConfValues.OperatorType.JOIN.name())) {

      return new JoinOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.AGGREGATE_WINDOW.name())) {

      return new AggregateWindowOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else if (type.equals(ConfValues.OperatorType.APPLY_STATEFUL_WINDOW.name())) {

      return new ApplyStatefulWindowOperator(getObject(conf, ConfKeys.OperatorConf.UDF_STRING.name(), classLoader));

    } else {
      throw new RuntimeException("Invalid operator: " + type);
    }
  }

  private <V> V getObject(final Map<String, String> conf,
                          final String key,
                          final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    return SerializeUtils.deserializeFromString(
        conf.get(key), classLoader);
  }
  /**
   * Get a new sink.
   * @param conf configuration
   * @param classLoader external class loader
   * @return new sink
   */
  @SuppressWarnings("unchecked")
  public <T> Sink<T> newSink(
      final Map<String, String> conf,
      final ClassLoader classLoader) throws IOException {
    final String type = conf.get(ConfKeys.SinkConf.SINK_TYPE.name());

    if (type.equals(ConfValues.SinkType.NETTY.name())) {

      final String serverAddress = conf.get(ConfKeys.NettySink.SINK_ADDRESS.name());
      final int serverPort = Integer.valueOf(conf.get(ConfKeys.NettySink.SINK_PORT.name()));
      return (Sink<T>)new NettyTextSink(serverAddress, serverPort, nettySharedResource, identifierFactory);

    } else if (type.equals(ConfValues.SinkType.MQTT.name())) {

      final String brokerURI = conf.get(ConfKeys.MqttSink.MQTT_SINK_BROKER_URI.name());
      final String topic = conf.get(ConfKeys.MqttSink.MQTT_SINK_TOPIC.name());
      try {
        return (Sink<T>)new MqttSink(brokerURI, topic, mqttSharedResource);
      } catch (final MqttException e) {
        e.printStackTrace();
        throw new IOException(e);
      }

    } else {
      throw new RuntimeException("Invalid sink type: " + type);
    }
  }

  @Override
  public void close() throws Exception {
    kafkaSharedResource.close();
    nettySharedResource.close();
    mqttSharedResource.close();
  }
}
