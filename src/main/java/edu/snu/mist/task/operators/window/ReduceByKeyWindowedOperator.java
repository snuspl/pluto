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
package edu.snu.mist.task.operators.window;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.BaseOperator;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.function.BiFunction;

/**
 * This class applies reduce by key operation to windowed data.
 * @param <K> key type
 * @param <V> value type
 */
public final class ReduceByKeyWindowedOperator<K extends Serializable, V extends Serializable>
    extends BaseOperator<WindowedData<Tuple2<K, V>>, HashMap<K, V>>
    implements WindowedOperator<Tuple2<K, V>, HashMap<K, V>> {

  /**
   * A reduce function.
   */
  private final BiFunction<V, V, V> reduceFunc;

  /**
   * An index of key.
   */
  private final int keyIndex;

  @Inject
  private ReduceByKeyWindowedOperator(final BiFunction<V, V, V> reduceFunc,
                                      @Parameter(QueryId.class) final String queryId,
                                      @Parameter(OperatorId.class) final String operatorId,
                                      @Parameter(KeyIndex.class) final int keyIndex,
                                      final StringIdentifierFactory idFactory) {
    super(idFactory.getNewInstance(queryId), idFactory.getNewInstance(operatorId));
    this.reduceFunc = reduceFunc;
    this.keyIndex = keyIndex;
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.REDUCE_BY_KEY_WINDOW;
  }

  @Override
  public void handle(final WindowedData<Tuple2<K, V>> windowedInput) {
    final HashMap<K, V> output = new HashMap<>();
    for (final Tuple2 input : windowedInput.getData()) {
      final K key = (K)input.get(keyIndex);
      final V val = (V)input.get(1 - keyIndex);
      final V oldVal = output.get(key);
      if (oldVal == null) {
        output.put(key, val);
      } else {
        output.put(key, reduceFunc.apply(oldVal, val));
      }
    }
    outputEmitter.emit(output);
  }
}
