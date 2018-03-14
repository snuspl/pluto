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
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.windows.WindowData;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The class for WindowedStream.
 */
final class WindowedStreamImpl<T> extends MISTStreamImpl<WindowData<T>> implements WindowedStream<T> {

  WindowedStreamImpl(final DAG<MISTStream, MISTEdge> dag,
                     final Map<String, String> confMap) {
    super(dag, confMap);
  }

  @Override
  public <K, V> ContinuousStream<Map<K, V>> reduceByKeyWindow(
      final int keyFieldNum,
      final Class<K> keyType,
      final MISTBiFunction<V, V, V> reduceFunc) {
    final Map<String, String> confMap = new HashMap<>();
    confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.REDUCE_BY_KEY.name());
    confMap.put(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name(), String.valueOf(keyFieldNum));
    try {
      confMap.put(ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name(),
          SerializeUtils.serializeToString(reduceFunc));
      return transformToSingleInputContinuousStream(confMap, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <R> ContinuousStream<R> aggregateWindow(final MISTFunction<WindowData<T>, R> aggregateFunc) {
    try {
      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(), ConfValues.OperatorType.AGGREGATE_WINDOW.name());
      confMap.put(ConfKeys.OperatorConf.UDF_STRING.name(), SerializeUtils.serializeToString(aggregateFunc));
      return transformToSingleInputContinuousStream(confMap, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public <R> ContinuousStream<R> applyStatefulWindow(
      final ApplyStatefulFunction<T, R> applyStatefulFunction) {
    try {
      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.OperatorConf.OP_TYPE.name(),
          ConfValues.OperatorType.APPLY_STATEFUL_WINDOW.name());
      confMap.put(ConfKeys.OperatorConf.UDF_STRING.name(),
          SerializeUtils.serializeToString(applyStatefulFunction));
      return transformToSingleInputContinuousStream(confMap, this);
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
