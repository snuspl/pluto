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

package edu.snu.mist.client.datastreams;


import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.cep.CepEventPattern;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.CountWindowInformation;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.common.windows.WindowInformation;
import edu.snu.mist.formats.avro.Direction;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class implements ContinuousStream by configuring operations.
 * <T> data type of the stream.
 */
public class ContinuousStreamImpl<T> extends MISTStreamImpl<T> implements ContinuousStream<T> {

  /**
   * The number of branches diverged from this stream.
   */
  private int condBranchCount;
  /**
   * The branch index that represents the order of branch.
   * If this index is n which is greater than 0,
   * than it means that this stream is n'th (conditional) branch from upstream.
   */
  private final int branchIndex;

  public ContinuousStreamImpl(final DAG<MISTStream, MISTEdge> dag,
                              final Map<String, String> conf) {
    this(dag, conf, 0);
  }

  private ContinuousStreamImpl(final DAG<MISTStream, MISTEdge> dag,
                               final Map<String, String> conf,
                               final int branchIndex) {
    super(dag, conf);
    this.condBranchCount = 0;
    this.branchIndex = branchIndex;
  }

  @Override
  public int getCondBranchCount() {
    return condBranchCount;
  }

  @Override
  public int getBranchIndex() {
    return branchIndex;
  }

  /**
   * Transform the upstream to a new windowed stream
   * by applying the operation corresponding to the given configuration.
   * @param conf configuration
   * @param upStream upstream
   * @param <OUT> output type
   * @return windowed stream
   */
  private <OUT> WindowedStream<OUT> transformToWindowedStream(
      final Map<String, String> conf,
      final MISTStream upStream) {
    final WindowedStream<OUT> downStream = new WindowedStreamImpl<>(dag, conf);
    dag.addVertex(downStream);
    dag.addEdge(upStream, downStream, new MISTEdge(Direction.LEFT));
    return downStream;
  }

  /**
   * Transform two upstreams to a new continuous stream
   * by applying the operation corresponding to the given configuration.
   * @param conf configuration
   * @param leftStream left stream
   * @param rightStream right stream
   * @param <OUT> output type
   * @return continuous stream
   */
  private <OUT> ContinuousStream<OUT> transformToDoubleInputContinuousStream(
      final Map<String, String> conf,
      final MISTStream leftStream,
      final MISTStream rightStream) {
    final ContinuousStream<OUT> downStream = new ContinuousStreamImpl<>(dag, conf);
    dag.addVertex(downStream);
    dag.addEdge(leftStream, downStream, new MISTEdge(Direction.LEFT));
    dag.addEdge(rightStream, downStream, new MISTEdge(Direction.RIGHT));
    return downStream;
  }

  /**
   * Create a new continuous stream that is processed by the operator using a single udf
   * (ex. map, filter, flatMap, applyStateful).
   * @param udf a user-defined function
   * @param operatorType a class representing the operator
   * @param <OUT> the result type of the operation
   * @return a new transformed continuous stream
   */
  private <OUT> ContinuousStream<OUT> transformWithSingleUdfOperator(
      final Serializable udf,
      final ConfValues.OperatorType operatorType) {

    final Map<String, String> confMap = new HashMap<>();

    try {
      confMap.put(ConfKeys.OperatorConf.UDF_STRING.name(),
          SerializeUtils.serializeToString(udf));
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), operatorType.name());
      return transformToSingleInputContinuousStream(confMap, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <OUT> ContinuousStream<OUT> map(final MISTFunction<T, OUT> mapFunc) {
    return transformWithSingleUdfOperator(mapFunc, ConfValues.OperatorType.MAP);
  }

  @Override
  public <OUT> ContinuousStream<OUT> flatMap(final MISTFunction<T, List<OUT>> flatMapFunc) {
    return transformWithSingleUdfOperator(flatMapFunc, ConfValues.OperatorType.FLAT_MAP);
  }

  @Override
  public ContinuousStream<T> filter(final MISTPredicate<T> filterFunc) {
    return transformWithSingleUdfOperator(filterFunc, ConfValues.OperatorType.FILTER);
  }

  @Override
  public <OUT> ContinuousStream<OUT> applyStateful(
      final ApplyStatefulFunction<T, OUT> applyStatefulFunction) {
    return transformWithSingleUdfOperator(applyStatefulFunction, ConfValues.OperatorType.APPLY_STATEFUL);
  }

  @Override
  public ContinuousStream<Tuple2<T, String>> stateTransition(
          final String initialState,
          final Set<String> finalState,
          final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable) throws IOException {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.STATE_TRANSITION.name());
    confMap.put(ConfKeys.StateTransitionOperator.INITIAL_STATE.name(), initialState);
    confMap.put(ConfKeys.StateTransitionOperator.STATE_TABLE.name(),
        SerializeUtils.serializeToString((Serializable) stateTable));
    confMap.put(ConfKeys.StateTransitionOperator.FINAL_STATE.name(),
        SerializeUtils.serializeToString((Serializable)finalState));

    return transformToSingleInputContinuousStream(confMap, this);
  }

  @Override
  public ContinuousStream<Map<String, List<T>>> cepOperator(final List<CepEventPattern<T>> cepEventPatterns,
                                                            final long windowTime) throws IOException {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.CEP.name());
    confMap.put(ConfKeys.CepOperator.CEP_EVENT.name(),
        SerializeUtils.serializeToString((Serializable) cepEventPatterns));
    confMap.put(ConfKeys.CepOperator.WINDOW_TIME.name(), String.valueOf(windowTime));

    return transformToSingleInputContinuousStream(confMap, this);
  }

  @Override
  public <K, V> ContinuousStream<Map<K, V>> reduceByKey(final int keyFieldNum,
                                                        final Class<K> keyType,
                                                        final MISTBiFunction<V, V, V> reduceFunc) {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.REDUCE_BY_KEY.name());

    try {
      confMap.put(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name(), String.valueOf(keyFieldNum));
      confMap.put(ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name(),
          SerializeUtils.serializeToString(reduceFunc));
      return transformToSingleInputContinuousStream(confMap, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public ContinuousStream<T> union(final ContinuousStream<T> inputStream) {
    // TODO[MIST-245]: Improve type checking.
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.UNION.name());
    return transformToDoubleInputContinuousStream(confMap, this, inputStream);
  }

  @Override
  public WindowedStream<T> window(final WindowInformation windowInfo) {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.WindowOperator.WINDOW_SIZE.name(), String.valueOf(windowInfo.getWindowSize()));
    confMap.put(ConfKeys.WindowOperator.WINDOW_INTERVAL.name(), String.valueOf(windowInfo.getWindowInterval()));

    if (windowInfo instanceof TimeWindowInformation) {
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.TIME_WINDOW.name());
    } else if (windowInfo instanceof CountWindowInformation) {
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.COUNT_WINDOW.name());
    } else {
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.SESSION_WINDOW.name());
    }
    return transformToWindowedStream(confMap, this);
  }

  /**
   * Before joining, maps two streams into a Tuple2 form, unifies them, and
   * applies windowing operation with user-defined WindowInformation.
   * After that, joins a pair of inputs in two streams that satisfies the user-defined predicate.
   */
  @Override
  public <U> WindowedStream<Tuple2<T, U>> join(final ContinuousStream<U> inputStream,
                                               final MISTBiPredicate<T, U> joinBiPredicate,
                                               final WindowInformation windowInfo) {
    final MISTFunction<T, Tuple2<T, U>> firstMapFunc = input -> new Tuple2<>(input, null);
    final MISTFunction<U, Tuple2<T, U>> secondMapFunc = input -> new Tuple2<>(null, input);
    final WindowedStream<Tuple2<T, U>> windowedStream = this
        .map(firstMapFunc)
        .union(inputStream.map(secondMapFunc))
        .window(windowInfo);

    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.JOIN.name());

    try {
      confMap.put(ConfKeys.OperatorConf.UDF_STRING.name(),
           SerializeUtils.serializeToString(joinBiPredicate));
      return transformToWindowedStream(confMap, windowedStream);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public ContinuousStream<T> routeIf(final MISTPredicate<T> condition) {
    condBranchCount++;
    try {
      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.OperatorConf.UDF_STRING.name(),
          SerializeUtils.serializeToString(condition));

      final ContinuousStream<T> downStream = new ContinuousStreamImpl<>(dag, confMap, condBranchCount);
      dag.addVertex(downStream);
      dag.addEdge(this, downStream, new MISTEdge(Direction.LEFT));
      return downStream;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public MISTStream<String> textSocketOutput(final String serverAddress,
                                             final int serverPort) {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.SinkConf.SINK_TYPE.name(), ConfValues.SinkType.NETTY.name());
    confMap.put(ConfKeys.NettySink.SINK_ADDRESS.name(), serverAddress);
    confMap.put(ConfKeys.NettySink.SINK_PORT.name(), String.valueOf(serverPort));

    final MISTStream<String> sink = new MISTStreamImpl<>(dag, confMap);
    dag.addVertex(sink);
    dag.addEdge(this, sink, new MISTEdge(Direction.LEFT));
    return sink;
  }

  @Override
  public MISTStream<MqttMessage> mqttOutput(final String brokerURI, final String topic) {

    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.SinkConf.SINK_TYPE.name(), ConfValues.SinkType.MQTT.name());
    confMap.put(ConfKeys.MqttSink.MQTT_SINK_BROKER_URI.name(), brokerURI);
    confMap.put(ConfKeys.MqttSink.MQTT_SINK_TOPIC.name(), topic);

    final MISTStream<MqttMessage> sink = new MISTStreamImpl<>(dag, confMap);
    dag.addVertex(sink);
    dag.addEdge(this, sink, new MISTEdge(Direction.LEFT));
    return sink;
  }
}
