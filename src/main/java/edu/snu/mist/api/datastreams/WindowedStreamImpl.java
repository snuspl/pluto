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

import edu.snu.mist.api.datastreams.configurations.ReduceByKeyOperatorUDFConfiguration;
import edu.snu.mist.api.datastreams.configurations.SingleInputOperatorUDFConfiguration;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.operators.AggregateWindowOperator;
import edu.snu.mist.common.operators.ApplyStatefulWindowOperator;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * The class for WindowedStream.
 */
final class WindowedStreamImpl<T> extends MISTStreamImpl<WindowData<T>> implements WindowedStream<T> {

  WindowedStreamImpl(final DAG<MISTStream, Direction> dag,
                     final Configuration conf) {
    super(dag, conf);
  }

  /**
   * Transform current stream to the continuous stream with the configuration.
   * @param conf configuration for transform
   * @param <U> result type of the transform
   * @return a transformed continuous stream
   */
  private <U> ContinuousStream<U> transformToContinuousStream(final Configuration conf) {
    final ContinuousStream<U> downStream = new ContinuousStreamImpl<>(dag, conf);
    dag.addVertex(downStream);
    dag.addEdge(this, downStream, Direction.LEFT);
    return downStream;
  }

  @Override
  public <K, V> ContinuousStream<Map<K, V>> reduceByKeyWindow(
      final int keyFieldNum,
      final Class<K> keyType,
      final MISTBiFunction<V, V, V> reduceFunc) {
    try {
      final Configuration conf = ReduceByKeyOperatorUDFConfiguration.CONF
          .set(ReduceByKeyOperatorUDFConfiguration.KEY_INDEX, keyFieldNum)
          .set(ReduceByKeyOperatorUDFConfiguration.UDF_STRING,
              SerializeUtils.serializeToString(reduceFunc))
          .build();
      return transformToContinuousStream(conf);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <R> ContinuousStream<R> aggregateWindow(final MISTFunction<WindowData<T>, R> aggregateFunc) {
    try {
      final Configuration conf = SingleInputOperatorUDFConfiguration.CONF
          .set(SingleInputOperatorUDFConfiguration.UDF_STRING, SerializeUtils.serializeToString(aggregateFunc))
          .set(SingleInputOperatorUDFConfiguration.OPERATOR, AggregateWindowOperator.class)
          .build();
      return transformToContinuousStream(conf);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <R> ContinuousStream<R> applyStatefulWindow(
      final ApplyStatefulFunction<T, R> applyStatefulFunction) {
    try {
      final Configuration conf = SingleInputOperatorUDFConfiguration.CONF
          .set(SingleInputOperatorUDFConfiguration.UDF_STRING,
              SerializeUtils.serializeToString(applyStatefulFunction))
          .set(SingleInputOperatorUDFConfiguration.OPERATOR, ApplyStatefulWindowOperator.class)
          .build();
      return transformToContinuousStream(conf);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
