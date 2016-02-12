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
package edu.snu.mist.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.function.BiFunction;

/**
 * This operator reduces the value by key.
 * @param <K> key type
 * @param <V> value type
 * TODO[MIST-#]: Support non-serializable key and value
 */
public final class ReduceByKeyOperator<K extends Serializable, V extends Serializable>
    extends StatefulOperator<Tuple2, HashMap<K, V>, HashMap<K, V>> {

  /**
   * A reduce function.
   */
  private final BiFunction<V, V, V> reduceFunc;

  /**
   * An index of key.
   */
  private final int keyIndex;

  /**
   * @param reduceFunc reduce function
   * @param queryId identifier of the query which contains this operator
   * @param operatorId identifier of operator
   * @param keyIndex index of key
   * @param idFactory identifier factory
   */
  @Inject
  private ReduceByKeyOperator(final BiFunction<V, V, V> reduceFunc,
                              @Parameter(QueryId.class) final String queryId,
                              @Parameter(OperatorId.class) final String operatorId,
                              @Parameter(KeyIndex.class) final int keyIndex,
                              final StringIdentifierFactory idFactory) {
    super(idFactory.getNewInstance(queryId), idFactory.getNewInstance(operatorId));
    this.reduceFunc = reduceFunc;
    this.keyIndex = keyIndex;
  }

  @Override
  protected HashMap<K, V> createInitialState() {
    return new HashMap<>();
  }

  /**
   * Reduces the value by key.
   * It creates a new map whenever it updates the state.
   * This produces immutable output.
   * @param input input tuple
   * @param state previous state
   * @return output
   */
  @SuppressWarnings("unchecked")
  @Override
  protected HashMap<K, V> updateState(final Tuple2 input, final HashMap<K, V> state) {
    final HashMap<K, V> newState = new HashMap<>(state);
    final K key = (K)input.get(keyIndex);
    final V val = (V)input.get(1 - keyIndex);
    final V oldVal = newState.get(key);
    if (oldVal == null) {
      newState.put(key, val);
    } else {
      newState.put(key, reduceFunc.apply(oldVal, val));
    }
    return newState;
  }

  /**
   * The state and the generated output are the same, so just emits the state.
   * @param finalState state
   * @return output
   */
  @Override
  protected HashMap<K, V> generateOutput(final HashMap<K, V> finalState) {
    return finalState;
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.REDUCE_BY_KEY;
  }
}
