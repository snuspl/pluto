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

package edu.snu.mist.api.datastreams;


import edu.snu.mist.api.datastreams.configurations.*;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.CountWindowInformation;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.common.windows.WindowInformation;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationModule;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This abstract class contains common methods for ContinuousStream.
 * <T> data type of the stream.
 */
public final class ContinuousStreamImpl<T> extends MISTStreamImpl<T> implements ContinuousStream<T> {

  public ContinuousStreamImpl(final DAG<MISTStream, Direction> dag,
                              final Configuration conf) {
    super(dag, conf);
  }

  /**
   * Create a new continuous stream that is processed by the operator using a single udf
   * (ex. map, filter, flatMap, applyStateful).
   * @param udf a user-defined function
   * @param clazz a class representing the operator
   * @param dag a dag
   * @param <OUT> the result type of the operation
   * @return a new transformed continuous stream
   */
  private <OUT> ContinuousStream<OUT> transformWithSingleUdfOperator(
      final Serializable udf,
      final Class<? extends Operator> clazz,
      final DAG<MISTStream, Direction> dag) {
    try {
      final Configuration opConf = SingleInputOperatorUDFConfiguration.CONF
          .set(SingleInputOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(udf))
          .set(SingleInputOperatorUDFConfiguration.OPERATOR, clazz)
          .build();
      final ContinuousStream<OUT> downStream = new ContinuousStreamImpl<>(dag, opConf);
      dag.addVertex(downStream);
      dag.addEdge(this, downStream, Direction.LEFT);
      return downStream;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <OUT> ContinuousStream<OUT> map(final MISTFunction<T, OUT> mapFunc) {
    return transformWithSingleUdfOperator(mapFunc, MapOperator.class, dag);
  }

  @Override
  public <OUT> ContinuousStream<OUT> flatMap(final MISTFunction<T, List<OUT>> flatMapFunc) {
    return transformWithSingleUdfOperator(flatMapFunc, FlatMapOperator.class, dag);
  }

  @Override
  public ContinuousStream<T> filter(final MISTPredicate<T> filterFunc) {
    return transformWithSingleUdfOperator(filterFunc, FilterOperator.class, dag);
  }

  @Override
  public <OUT> ContinuousStream<OUT> applyStateful(
      final ApplyStatefulFunction<T, OUT> applyStatefulFunction) {
    return transformWithSingleUdfOperator(applyStatefulFunction, ApplyStatefulOperator.class, dag);
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
      final ContinuousStream<Map<K, V>> downStream = new ContinuousStreamImpl<>(dag, opConf);
      dag.addVertex(downStream);
      dag.addEdge(this, downStream, Direction.LEFT);
      return downStream;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

  }

  @Override
  public ContinuousStream<T> union(final ContinuousStream<T> inputStream) {
    // TODO[MIST-245]: Improve type checking.
    final Configuration opConf = UnionOperatorConfiguration.CONF.build();
    final ContinuousStream<T> downStream = new ContinuousStreamImpl<>(dag, opConf);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    dag.addEdge(inputStream, downStream, Direction.RIGHT);
    return downStream;
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

    final WindowedStream<T> downStream = new WindowedStreamImpl<>(dag, opConf);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
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
      final Configuration opConf = SingleInputOperatorUDFConfiguration.CONF
          .set(SingleInputOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(joinBiPredicate))
          .set(SingleInputOperatorUDFConfiguration.OPERATOR, JoinOperator.class)
          .build();
      final WindowedStream<Tuple2<T, U>> downStream = new WindowedStreamImpl<>(dag, opConf);
      dag.addVertex(downStream);
      dag.addEdge(windowedStream, downStream, Direction.LEFT);
      return downStream;
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
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
    dag.addEdge(this, sink, Direction.LEFT);
    return sink;
  }
}
