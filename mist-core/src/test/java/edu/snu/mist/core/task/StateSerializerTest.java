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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.windows.Window;
import edu.snu.mist.common.windows.WindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public final class StateSerializerTest {
  @Test
  public void testPrimitiveStates() {
    // States to be serialized.
    final int testInt = 5;
    final boolean testBoolean = true;
    final long testLong = 99L;
    final float testFloat = 3.1425f;
    final double testDouble = 1972.7;
    final String testString = "cogito ergo sum!";

    // The map that contains the above states.
    final Map<String, Object> testStateMap = new HashMap<>();
    testStateMap.put("testInt", testInt);
    testStateMap.put("testBoolean", testBoolean);
    testStateMap.put("testLong", testLong);
    testStateMap.put("testFloat", testFloat);
    testStateMap.put("testDouble", testDouble);
    testStateMap.put("testString", testString);

    // Serialize the stateMap and check that primitive types are not serialized to ByteBuffer.
    final Map<String, Object> serializedMap = StateSerializer.serializeStateMap(testStateMap);
    Assert.assertEquals(testInt, serializedMap.get("testInt"));
    Assert.assertEquals(testBoolean, serializedMap.get("testBoolean"));
    Assert.assertEquals(testLong, serializedMap.get("testLong"));
    Assert.assertEquals(testFloat, serializedMap.get("testFloat"));
    Assert.assertEquals(testDouble, serializedMap.get("testDouble"));
    Assert.assertEquals(testString, serializedMap.get("testString"));

    // Deserialize the stateMap.
    final Map<String, Object> deserializedMap = StateSerializer.deserializeStateMap(serializedMap);
    final int deserializedInt = (int)deserializedMap.get("testInt");
    final boolean deserializedBoolean = (boolean)deserializedMap.get("testBoolean");
    final long deserializedLong = (long)deserializedMap.get("testLong");
    final float deserializedFloat = (float)deserializedMap.get("testFloat");
    final double deserializedDouble = (double)deserializedMap.get("testDouble");
    final String deserializedString = (String)deserializedMap.get("testString");

    // Compare the results.
    Assert.assertEquals(testInt, deserializedInt);
    Assert.assertEquals(testBoolean, deserializedBoolean);
    Assert.assertEquals(testLong, deserializedLong);
    Assert.assertEquals(testFloat, deserializedFloat, 0.00001);
    Assert.assertEquals(testDouble, deserializedDouble, 0.00001);
    Assert.assertEquals(testString, deserializedString);
  }

  @Test
  public void testOtherStates() {
    // States to be serialized.
    final Map<String, Integer> testMap = new HashMap<>();
    testMap.put("Cheeseburgers", 6);
    testMap.put("Drinks", 3);
    final MistWatermarkEvent testWatermarkEvent = new MistWatermarkEvent(98L, false);
    final Window<String> testWindow = new WindowImpl<>(100L);
    final Queue<Window<String>> testQueue = new LinkedList<>();
    testQueue.add(testWindow);

    // The map that contains the above states.
    final Map<String, Object> testStateMap = new HashMap<>();
    testStateMap.put("testMap", testMap);
    testStateMap.put("testWatermarkEvent", testWatermarkEvent);
    testStateMap.put("testWindow", testWindow);
    testStateMap.put("testQueue", testQueue);

    // Serialize and deserialize the stateMap.
    final Map<String, Object> serializedStateMap = StateSerializer.serializeStateMap(testStateMap);
    final Map<String, Object> deserializedStateMap = StateSerializer.deserializeStateMap(serializedStateMap);
    final Map<String, Integer> deserializedMap = (Map<String, Integer>)deserializedStateMap.get("testMap");
    final MistWatermarkEvent deserializedWatermarkEvent =
        (MistWatermarkEvent)deserializedStateMap.get("testWatermarkEvent");
    final Window<String> deserializedWindow = (Window<String>)deserializedStateMap.get("testWindow");
    final Queue<Window<String>> deserializedQueue = (Queue<Window<String>>)deserializedStateMap.get("testQueue");

    // Compare the results.
    Assert.assertEquals(testMap, deserializedMap);
    Assert.assertEquals(testWatermarkEvent, deserializedWatermarkEvent);
    Assert.assertEquals(testWindow, deserializedWindow);
    Assert.assertEquals(testQueue, deserializedQueue);
  }
}
