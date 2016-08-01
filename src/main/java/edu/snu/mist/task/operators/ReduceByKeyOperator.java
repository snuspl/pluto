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
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;

import java.io.Serializable;
import java.util.HashMap;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator reduces the value by key.
 * @param <K> key type
 * @param <V> value type
 * TODO[MIST-#]: Support non-serializable key and value.
 * Currently, we use HashMap instead of Map because HashMap is serializable, but Map isn't.
 * This can be changed to Map when we support non-serializable state.
 */
public final class ReduceByKeyOperator<K extends Serializable, V extends Serializable>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ReduceByKeyOperator.class.getName());

  /**
   * A reduce function.
   */
  private final BiFunction<V, V, V> reduceFunc;

  /**
   * An index of key.
   */
  private final int keyIndex;

  /**
   * KeyValue state.
   */
  private HashMap<K, V> state;

  /**
   * @param reduceFunc reduce function
   * @param queryId identifier of the query which contains this operator
   * @param operatorId identifier of operator
   * @param keyIndex index of key
   */
  public ReduceByKeyOperator(final String queryId,
                             final String operatorId,
                             final int keyIndex,
                             final BiFunction<V, V, V> reduceFunc) {
    super(queryId, operatorId);
    this.reduceFunc = reduceFunc;
    this.keyIndex = keyIndex;
    this.state = createInitialState();
  }

  private HashMap<K, V> createInitialState() {
    return new HashMap<>();
  }

  /**
   * Reduces the value by key.
   * It creates a new map whenever it updates the state.
   * This produces immutable output.
   * @param input input tuple
   * @param st previous state
   * @return output
   */
  @SuppressWarnings("unchecked")
  private HashMap<K, V> updateState(final Tuple2 input, final HashMap<K, V> st) {
    final HashMap<K, V> newState = new HashMap<>(st);
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
  private HashMap<K, V> generateOutput(final HashMap<K, V> finalState) {
    return finalState;
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.REDUCE_BY_KEY;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    final HashMap<K, V> intermediateState = updateState((Tuple2)input.getValue(), state);
    final HashMap<K, V> output = generateOutput(intermediateState);
    LOG.log(Level.FINE, "{0} updates the state {1} with input {2} to {3}, and generates {4}",
        new Object[]{getOperatorIdentifier(), state, input, intermediateState, output});
    input.setValue(output);
    outputEmitter.emitData(input);
    state = intermediateState;
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}
