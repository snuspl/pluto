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
package edu.snu.mist.core.task;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is used to serialize the states of operators.
 * The states must be ensured that they implement the Serializable interface.
 */
public final class StateSerializer {

  private static final Logger LOG = Logger.getLogger(StateSerializer.class.getName());

  /**
   * Receives a Map<String, Object>, serializes the values, and returns it.
   * @param stateMap
   * @return the serialized StateMap
   */
  public static Map<String, Object> serializeStateMap(final Map<String, Object> stateMap) {
    final Map<String, Object> result = new HashMap<>();
    for (final Map.Entry<String, Object> mapEntry : stateMap.entrySet()) {
      final Object state = mapEntry.getValue();
      if ((state instanceof Boolean)
          || (state instanceof Integer)
          || (state instanceof Long)
          || (state instanceof Float)
          || (state instanceof Double)
          || (state instanceof String)) {
        result.put(mapEntry.getKey(), state);
      } else {
        final Object serializedState = serializeState(state);
        if (serializedState == null) {
          throw new RuntimeException("Error while serializing the operator state.");
        } else {
          result.put(mapEntry.getKey(), serializedState);
        }
      }
    }
    return result;
  }

  /**
   * Serializes an object that implements Serializable into a ByteBuffer.
   * @param obj
   * @return the serialized state
   */
  private static ByteBuffer serializeState(final Object obj) {
    try {
      try (final ByteArrayOutputStream b = new ByteArrayOutputStream()) {
        try (final ObjectOutputStream o = new ObjectOutputStream(b)) {
          o.writeObject(obj);
          o.flush();
        }
        return ByteBuffer.wrap(b.toByteArray());
      }
    } catch (final IOException e) {
      LOG.log(Level.SEVERE, "An exception occured while serializing the state.");
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Receives a Map<String, ByteBuffer>, deserializes the values, and returns it.
   * @param serializedStateMap
   * @return the deserialized StateMap
   */
  public static Map<String, Object> deserializeStateMap(final Map<String, Object> serializedStateMap) {
    final Map<String, Object> result = new HashMap<>();
    for (final Map.Entry<String, Object> mapEntry : serializedStateMap.entrySet()) {
      final Object state = mapEntry.getValue();
      if (state instanceof ByteBuffer) {
        final Object deserializedState = deserializeState((ByteBuffer)state);
        if (deserializedState == null) {
          throw new RuntimeException("Error while deserializing the operator state.");
        } else {
          result.put(mapEntry.getKey(), deserializedState);
        }
      } else {
        result.put(mapEntry.getKey(), state);
      }
    }
    return result;
  }

  /**
   * Deserializes an ByteBuffer into an Object.
   * @param byteBuffer
   * @return the deserialized state
   */
  private static Object deserializeState(final ByteBuffer byteBuffer) {
    final byte[] bytes = new byte[byteBuffer.remaining()];
    byteBuffer.get(bytes);
    try {
      try (final ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
        try (final ObjectInputStream o = new ObjectInputStream(b)) {
          return o.readObject();
        }
      }
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "An exception occured while deserializing the state.");
      e.printStackTrace();
      return null;
    }
  }

  private StateSerializer() {
  }
}
