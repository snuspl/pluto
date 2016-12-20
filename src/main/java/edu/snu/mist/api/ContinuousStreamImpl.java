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

package edu.snu.mist.api;

import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTBiPredicate;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.TextSocketSink;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.windows.WindowInformation;
import edu.snu.mist.api.windows.WindowedStream;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;

import java.util.List;

/**
 * This abstract class contains common methods for ContinuousStream.
 * <T> data type of the stream.
 */
public abstract class ContinuousStreamImpl<T> extends MISTStreamImpl<T> implements ContinuousStream<T> {


  public ContinuousStreamImpl(final DAG<AvroVertexSerializable, Direction> dag) {
    super(dag);
  }

  @Override
  public <OUT> MapOperatorStream<T, OUT> map(final MISTFunction<T, OUT> mapFunc) {
    final MapOperatorStream<T, OUT> downStream = new MapOperatorStream<>(mapFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public <OUT> FlatMapOperatorStream<T, OUT> flatMap(final MISTFunction<T, List<OUT>> flatMapFunc) {
    final FlatMapOperatorStream<T, OUT> downStream = new FlatMapOperatorStream<>(flatMapFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public FilterOperatorStream<T> filter(final MISTPredicate<T> filterFunc) {
    final FilterOperatorStream<T> downStream = new FilterOperatorStream<>(filterFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public <K, V> ReduceByKeyOperatorStream<T, K, V> reduceByKey(final int keyFieldNum,
                                                               final Class<K> keyType,
                                                               final MISTBiFunction<V, V, V> reduceFunc) {
    final ReduceByKeyOperatorStream<T, K, V> downStream =
        new ReduceByKeyOperatorStream<>(keyFieldNum, keyType, reduceFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public <OUT> ApplyStatefulOperatorStream<T, OUT> applyStateful(
      final ApplyStatefulFunction<T, OUT> applyStatefulFunction) {
    final ApplyStatefulOperatorStream<T, OUT> downStream =
        new ApplyStatefulOperatorStream<>(applyStatefulFunction, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public UnionOperatorStream<T> union(final ContinuousStream<T> inputStream) {
    // TODO[MIST-245]: Improve type checking.
    final UnionOperatorStream<T> downStream = new UnionOperatorStream<>(dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    dag.addEdge(inputStream, downStream, Direction.RIGHT);
    return downStream;
  }

  @Override
  public WindowOperatorStream<T> window(final WindowInformation windowInfo) {
    final WindowOperatorStream<T> downStream = new WindowOperatorStream<>(windowInfo, dag);

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
  public <U> JoinOperatorStream<T, U> join(final ContinuousStream<U> inputStream,
                                           final MISTBiPredicate<T, U> joinBiPredicate,
                                           final WindowInformation windowInfo) {
    final MISTFunction<T, Tuple2<T, U>> firstMapFunc = input -> new Tuple2<>(input, null);
    final MISTFunction<U, Tuple2<T, U>> secondMapFunc = input -> new Tuple2<>(null, input);
    final WindowedStream<Tuple2<T, U>> windowedStream = this
        .map(firstMapFunc)
        .union(inputStream.map(secondMapFunc))
        .window(windowInfo);

    final JoinOperatorStream<T, U> downStream = new JoinOperatorStream<>(joinBiPredicate, dag);
    dag.addVertex(downStream);
    dag.addEdge(windowedStream, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public Sink textSocketOutput(final TextSocketSinkConfiguration textSocketSinkConfiguration) {
    final Sink sink = new TextSocketSink(textSocketSinkConfiguration);
    dag.addVertex(sink);
    dag.addEdge(this, sink, Direction.LEFT);
    return sink;
  }
}
