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
package edu.snu.mist.core.operators;

import com.rits.cloning.Cloner;
import com.rits.cloning.ObjenesisInstantiationStrategy;
import edu.snu.mist.core.MistCheckpointEvent;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.core.operators.parameters.KeyIndex;
import edu.snu.mist.core.operators.parameters.SerializedUdf;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
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
    extends OneStreamStateHandlerOperator {
  private static final Logger LOG = Logger.getLogger(ReduceByKeyOperator.class.getName());

  /**
   * A reduce function.
   */
  private final MISTBiFunction<V, V, V> reduceFunc;

  /**
   * An index of key.
   */
  private final int keyIndex;

  /**
   * KeyValue state.
   */
  private HashMap<K, V> state;

  @Inject
  private ReduceByKeyOperator(
      @Parameter(KeyIndex.class) final int keyIndex,
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(keyIndex, SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  /**
   * @param reduceFunc reduce function
   * @param keyIndex index of key
   */
  @Inject
  public ReduceByKeyOperator(@Parameter(KeyIndex.class) final int keyIndex,
                             final MISTBiFunction<V, V, V> reduceFunc) {
    super();
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
  public void processLeftData(final MistDataEvent input) {
    if (isEarlierThanRecoveredTimestamp(input)) {
      return;
    }
    final HashMap<K, V> intermediateState = updateState((Tuple2)input.getValue(), state);
    final HashMap<K, V> output = generateOutput(intermediateState);

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "{0} updates the state {1} with input {2} to {3}, and generates {4}",
          new Object[]{this.getClass().getName(), state, input, intermediateState, output});
    }

    input.setValue(output);
    updateLatestEventTimestamp(input.getTimestamp());
    outputEmitter.emitData(input);
    state = intermediateState;
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    if (isEarlierThanRecoveredTimestamp(input)) {
      return;
    }
    updateLatestEventTimestamp(input.getTimestamp());
    outputEmitter.emitWatermark(input);
  }

  @Override
  public Map<String, Object> getStateSnapshot() {
    final Map<String, Object> stateMap = new HashMap<>();
    stateMap.put("reduceByKeyState", new Cloner(new ObjenesisInstantiationStrategy()).deepClone(state));
    return stateMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setState(final Map<String, Object> loadedState) {
    state = (HashMap<K, V>)loadedState.get("reduceByKeyState");
  }

  @Override
  public void processLeftCheckpoint(final MistCheckpointEvent input) {
    checkpointMap.put(latestTimestampBeforeCheckpoint, getStateSnapshot());
  }
}
