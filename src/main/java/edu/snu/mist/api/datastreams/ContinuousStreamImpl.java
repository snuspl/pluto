/*
 * Copyright (C) 2017 Seoul National University
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

package edu.snu.mist.api.datastreams;


import edu.snu.mist.api.datastreams.configurations.*;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.CountWindowInformation;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.common.windows.WindowInformation;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements ContinuousStream by configuring operations using Tang.
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
                              final Configuration conf) {
    this(dag, conf, 0);
  }

  private ContinuousStreamImpl(final DAG<MISTStream, MISTEdge> dag,
                               final Configuration conf,
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
      final Configuration conf,
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
      final Configuration conf,
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
   * @param clazz a class representing the operator
   * @param <OUT> the result type of the operation
   * @return a new transformed continuous stream
   */
  private <OUT> ContinuousStream<OUT> transformWithSingleUdfOperator(
      final Serializable udf,
      final Class<? extends Operator> clazz) {
    try {
      final Configuration opConf = OperatorUDFConfiguration.CONF
          .set(OperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(udf))
          .set(OperatorUDFConfiguration.OPERATOR, clazz)
          .build();
      return transformToSingleInputContinuousStream(opConf, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Check the udf function and return the continuous stream that is transformed by the udf operator.
   * @param clazz class of the udf function
   * @param conf configuration of the udf function
   * @param upStream upstream of the transformation
   * @param <C> class type
   * @param <OUT> output type
   * @return continuous stream that is transformed by the udf operator
   */
  private <C, OUT> ContinuousStream<OUT> checkUdfAndTransform(final Class<? extends C> clazz,
                                                              final Configuration conf,
                                                              final MISTStream upStream) {
    checkUdf(clazz, conf);
    return transformToSingleInputContinuousStream(conf, upStream);
  }

  @Override
  public <OUT> ContinuousStream<OUT> map(final MISTFunction<T, OUT> mapFunc) {
    return transformWithSingleUdfOperator(mapFunc, MapOperator.class);
  }

  @Override
  public <OUT> ContinuousStream<OUT> map(final Class<? extends MISTFunction<T, OUT>> clazz,
                                         final Configuration funcConf) {
    final Configuration conf = Configurations.merge(MISTFuncOperatorConfiguration.CONF
        .set(MISTFuncOperatorConfiguration.OPERATOR, MapOperator.class)
        .set(MISTFuncOperatorConfiguration.UDF, clazz)
        .build(), funcConf);
    return checkUdfAndTransform(clazz, conf, this);
  }

  @Override
  public <OUT> ContinuousStream<OUT> flatMap(final MISTFunction<T, List<OUT>> flatMapFunc) {
    return transformWithSingleUdfOperator(flatMapFunc, FlatMapOperator.class);
  }

  @Override
  public <OUT> ContinuousStream<OUT> flatMap(final Class<? extends MISTFunction<T, List<OUT>>> clazz,
                                             final Configuration funcConf) {
    final Configuration conf = Configurations.merge(MISTFuncOperatorConfiguration.CONF
        .set(MISTFuncOperatorConfiguration.OPERATOR, FlatMapOperator.class)
        .set(MISTFuncOperatorConfiguration.UDF, clazz)
        .build(), funcConf);
    return checkUdfAndTransform(clazz, conf, this);
  }

  @Override
  public ContinuousStream<T> filter(final MISTPredicate<T> filterFunc) {
    return transformWithSingleUdfOperator(filterFunc, FilterOperator.class);
  }

  @Override
  public ContinuousStream<T> filter(final Class<? extends MISTPredicate<T>> clazz,
                                    final Configuration funcConf) {
    final Configuration conf = Configurations.merge(FilterOperatorConfiguration.CONF
        .set(FilterOperatorConfiguration.MIST_PREDICATE, clazz)
        .build(), funcConf);
    return checkUdfAndTransform(clazz, conf, this);
  }

  @Override
  public <OUT> ContinuousStream<OUT> applyStateful(
      final ApplyStatefulFunction<T, OUT> applyStatefulFunction) {
    return transformWithSingleUdfOperator(applyStatefulFunction, ApplyStatefulOperator.class);
  }

  @Override
  public <OUT> ContinuousStream<OUT> applyStateful(final Class<? extends ApplyStatefulFunction<T, OUT>> clazz,
                                                   final Configuration funcConf) {
    final Configuration conf = Configurations.merge(ApplyStatefulOperatorConfiguration.CONF
        .set(ApplyStatefulOperatorConfiguration.UDF, clazz)
        .set(ApplyStatefulOperatorConfiguration.OPERATOR, ApplyStatefulOperator.class)
        .build(), funcConf);
    return checkUdfAndTransform(clazz, conf, this);
  }

  @Override
  public ContinuousStream<T> nfa(
          final String initialState,
          final Set<String> finalState,
          final Map<String, List<Tuple2<MISTPredicate, String>>> stateTable) throws IOException {
    final Configuration opConf = NFAOperatorConfiguration.CONF
        .set(NFAOperatorConfiguration.INITIAL_STATE, initialState)
        .set(NFAOperatorConfiguration.FINAL_STATE, SerializeUtils.serializeToString((Serializable) finalState))
        .set(NFAOperatorConfiguration.STATE_TABLE, SerializeUtils.serializeToString((Serializable) stateTable))
        .build();

    return transformToSingleInputContinuousStream(opConf, this);
  }

  @Override
  public <K, V> ContinuousStream<Map<K, V>> reduceByKey(final int keyFieldNum,
                                                        final Class<K> keyType,
                                                        final MISTBiFunction<V, V, V> reduceFunc) {
    try {
      final Configuration opConf = ReduceByKeyOperatorUDFConfiguration.CONF
          .set(ReduceByKeyOperatorUDFConfiguration.KEY_INDEX, keyFieldNum)
          .set(ReduceByKeyOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(reduceFunc))
          .build();
      return transformToSingleInputContinuousStream(opConf, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <K, V> ContinuousStream<Map<K, V>> reduceByKey(
      final int keyFieldNum,
      final Class<K> keyType,
      final Class<? extends MISTBiFunction<V, V, V>> clazz,
      final Configuration funcConf) {
    final Configuration conf = Configurations.merge(ReduceByKeyOperatorConfiguration.CONF
        .set(ReduceByKeyOperatorConfiguration.KEY_INDEX, keyFieldNum)
        .set(ReduceByKeyOperatorConfiguration.MIST_BI_FUNC, clazz)
        .build(), funcConf);
    return checkUdfAndTransform(clazz, conf, this);
  }

  @Override
  public ContinuousStream<T> union(final ContinuousStream<T> inputStream) {
    // TODO[MIST-245]: Improve type checking.
    final Configuration opConf = UnionOperatorConfiguration.CONF.build();
    return transformToDoubleInputContinuousStream(opConf, this, inputStream);
  }

  @Override
  public WindowedStream<T> window(final WindowInformation windowInfo) {
    final ConfigurationModule confModule = WindowOperatorConfiguration.CONF
        .set(WindowOperatorConfiguration.WINDOW_SIZE, windowInfo.getWindowSize())
        .set(WindowOperatorConfiguration.WINDOW_INTERVAL, windowInfo.getWindowInterval());

    final Configuration opConf;
    if (windowInfo instanceof TimeWindowInformation) {
      opConf = confModule
          .set(WindowOperatorConfiguration.OPERATOR, TimeWindowOperator.class)
          .build();
    } else if (windowInfo instanceof CountWindowInformation) {
      opConf = confModule
          .set(WindowOperatorConfiguration.OPERATOR, CountWindowOperator.class)
          .build();
    } else {
      opConf = confModule
          .set(WindowOperatorConfiguration.OPERATOR, SessionWindowOperator.class)
          .build();
    }
    return transformToWindowedStream(opConf, this);
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
    try {
      final Configuration opConf = OperatorUDFConfiguration.CONF
          .set(OperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(joinBiPredicate))
          .set(OperatorUDFConfiguration.OPERATOR, JoinOperator.class)
          .build();
      return transformToWindowedStream(opConf, windowedStream);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <U> WindowedStream<Tuple2<T, U>> join(final ContinuousStream<U> inputStream,
                                               final Class<? extends MISTBiPredicate<T, U>> clazz,
                                               final Configuration funcConf,
                                               final WindowInformation windowInfo) {
    final MISTFunction<T, Tuple2<T, U>> firstMapFunc = input -> new Tuple2<>(input, null);
    final MISTFunction<U, Tuple2<T, U>> secondMapFunc = input -> new Tuple2<>(null, input);
    final WindowedStream<Tuple2<T, U>> windowedStream = this
        .map(firstMapFunc)
        .union(inputStream.map(secondMapFunc))
        .window(windowInfo);
    final Configuration opConf = Configurations.merge(JoinOperatorConfiguration.CONF
        .set(JoinOperatorConfiguration.MIST_BI_PREDICATE, clazz)
        .build(), funcConf);
    // Check the udf function
    checkUdf(clazz, opConf);
    return transformToWindowedStream(opConf, windowedStream);
  }

  @Override
  public ContinuousStream<T> routeIf(final MISTPredicate<T> condition) {
    condBranchCount++;
    try {
      final Configuration opConf = UDFConfiguration.CONF
          .set(UDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(condition))
          .build();

      final ContinuousStream<T> downStream = new ContinuousStreamImpl<>(dag, opConf, condBranchCount);
      dag.addVertex(downStream);
      dag.addEdge(this, downStream, new MISTEdge(Direction.LEFT));
      return downStream;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public ContinuousStream<T> routeIf(final Class<? extends MISTPredicate<T>> clazz,
                                     final Configuration funcConf) {
    // TODO: [MIST-436] Support predicate list generation through tang in branch operator API
    throw new RuntimeException("Branch operator setting through Tang is not supported yet.");
  }

  @Override
  public MISTStream<String> textSocketOutput(final String serverAddress,
                                             final int serverPort) {
    final Configuration opConf = TextSocketSinkConfiguration.CONF
        .set(TextSocketSinkConfiguration.SOCKET_HOST_ADDRESS, serverAddress)
        .set(TextSocketSinkConfiguration.SOCKET_HOST_PORT, serverPort)
        .build();
    final MISTStream<String> sink = new MISTStreamImpl<>(dag, opConf);
    dag.addVertex(sink);
    dag.addEdge(this, sink, new MISTEdge(Direction.LEFT));
    return sink;
  }

  @Override
  public MISTStream<MqttMessage> mqttOutput(final String brokerURI, final String topic) {
    final Configuration opConf = MqttSinkConfiguration.CONF
        .set(MqttSinkConfiguration.MQTT_BROKER_URI, brokerURI)
        .set(MqttSinkConfiguration.MQTT_TOPIC, topic)
        .build();
    final MISTStream<MqttMessage> sink = new MISTStreamImpl<>(dag, opConf);
    dag.addVertex(sink);
    dag.addEdge(this, sink, new MISTEdge(Direction.LEFT));
    return sink;
  }
}
