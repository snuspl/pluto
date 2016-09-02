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
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.operators.AggregateWindowOperatorStream;
import edu.snu.mist.api.operators.ApplyStatefulWindowOperatorStream;
import edu.snu.mist.api.operators.ReduceByKeyWindowOperatorStream;
import edu.snu.mist.api.window.*;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.*;

/**
 * The implementation class for describing WindowedStream.
 */
public final class WindowedStreamImpl<T> extends MISTStreamImpl<WindowData<T>> implements WindowedStream<T>  {

  /**
   * The policy for deciding the size of window inside.
   */
  private WindowSizePolicy windowSizePolicy;
  /**
   * The policy for deciding when to emit collected window data inside.
   */
  private WindowEmitPolicy windowEmitPolicy;

  public WindowedStreamImpl(final WindowSizePolicy windowSizePolicy,
                            final WindowEmitPolicy windowEmitPolicy,
                            final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(StreamType.BasicType.WINDOWED, dag);
    this.windowSizePolicy = windowSizePolicy;
    this.windowEmitPolicy = windowEmitPolicy;
  }

  @Override
  public WindowSizePolicy getWindowSizePolicy() {
    return windowSizePolicy;
  }

  @Override
  public WindowEmitPolicy getWindowEmitPolicy() {
    return windowEmitPolicy;
  }

  @Override
  public <K, V> ReduceByKeyWindowOperatorStream<T, K, V> reduceByKeyWindow(
      final int keyFieldNum,
      final Class<K> keyType,
      final MISTBiFunction<V, V, V> reduceFunc) {
    final ReduceByKeyWindowOperatorStream<T, K, V> downStream =
        new ReduceByKeyWindowOperatorStream<>(keyFieldNum, keyType, reduceFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, StreamType.Direction.LEFT);
    return downStream;
  }

  @Override
  public <R> AggregateWindowOperatorStream<T, R> aggregateWindow(final MISTFunction<WindowData<T>, R> aggregateFunc) {
    final AggregateWindowOperatorStream<T, R> downStream =
        new AggregateWindowOperatorStream<>(aggregateFunc, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, StreamType.Direction.LEFT);
    return downStream;
  }

  @Override
  public <R, S> ApplyStatefulWindowOperatorStream<T, R, S> applyStatefulWindow(
      final MISTBiFunction<T, S, S> updateStateFunc,
      final MISTFunction<S, R> produceResultFunc,
      final MISTSupplier<S> initializeStateSup) {
    final ApplyStatefulWindowOperatorStream<T, R, S> downStream =
        new ApplyStatefulWindowOperatorStream<>(updateStateFunc, produceResultFunc, initializeStateSup, dag);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, StreamType.Direction.LEFT);
    return downStream;
  }

  @Override
  public Vertex getSerializedVertex() {
    final Vertex.Builder vertexBuilder = Vertex.newBuilder();
    vertexBuilder.setVertexType(VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    final WindowSizePolicy sizePolicy = windowSizePolicy;
    if (sizePolicy.getSizePolicyType() == WindowType.SizePolicy.TIME) {
      wOpInfoBuilder.setSizePolicyType(SizePolicyTypeEnum.TIME);
      wOpInfoBuilder.setSizePolicyInfo(((TimeSizePolicy) sizePolicy).getTimeDuration());
    } else {
      throw new IllegalStateException("WindowSizePolicy is illegal!");
    }
    final WindowEmitPolicy emitPolicy = windowEmitPolicy;
    if (emitPolicy.getEmitPolicyType() == WindowType.EmitPolicy.TIME) {
      wOpInfoBuilder.setEmitPolicyType(EmitPolicyTypeEnum.TIME);
      wOpInfoBuilder.setEmitPolicyInfo(((TimeEmitPolicy) emitPolicy).getTimeInterval());
    } else {
      throw new IllegalStateException("WindowEmitPolicy is illegal!");
    }
    vertexBuilder.setAttributes(wOpInfoBuilder.build());
    return vertexBuilder.build();
  }
}
