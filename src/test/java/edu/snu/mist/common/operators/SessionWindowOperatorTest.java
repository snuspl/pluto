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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.windows.Window;
import edu.snu.mist.common.windows.WindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static edu.snu.mist.common.utils.OperatorTestUtils.checkWindowData;

public final class SessionWindowOperatorTest {

  /**
   * Some MistDataEvent and MistWatermarkEvent used during the test.
   */
  private final MistDataEvent d1 = new MistDataEvent(1, 200L);
  private final MistDataEvent d2 = new MistDataEvent(2, 700L);
  private final MistDataEvent d3 = new MistDataEvent(3, 1750L);
  private final MistDataEvent d4 = new MistDataEvent(4, 1800L);
  private final MistDataEvent d5 = new MistDataEvent(5, 2000L);
  private final MistDataEvent d6 = new MistDataEvent(6, 2700L);
  private final MistWatermarkEvent w1 = new MistWatermarkEvent(1200L);
  private final MistWatermarkEvent w2 = new MistWatermarkEvent(2400L);
  private final MistWatermarkEvent w3 = new MistWatermarkEvent(3100L);
  private final MistWatermarkEvent w4 = new MistWatermarkEvent(3700L);

  /**
   * Test whether SessionWindowOperator creates windows properly.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testSessionWindowOperator() throws InterruptedException {
    final int sessionInterval = 500;
    final SessionWindowOperator<Integer> sessionWindowOperator =
        new SessionWindowOperator<>("testAggOp", sessionInterval);
    final List<MistEvent> result = new LinkedList<>();
    sessionWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // (200)Window1-(1200):
    //                        (1750)Window2-----(2000):
    //                                                     (2700)Window3-(3100):
    //                                                                           (3700)Window5---: (will not be emitted)
    // d1-----d2-----w1---------d3--------d4-------d5-------d6----w2------w3-------w4:
    // expected results:
    // d1, d2, w1 in Window1
    // d3, d4, d5 in Window2
    // d6, w3 in Window3
    sessionWindowOperator.processLeftData(d1);
    sessionWindowOperator.processLeftData(d2);
    sessionWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(0, result.size());
    sessionWindowOperator.processLeftData(d3);
    Assert.assertEquals(2, result.size());
    final Collection<Integer> expectedResult1 = new LinkedList<>();
    expectedResult1.add(1);
    expectedResult1.add(2);
    checkWindowData(result.get(0), expectedResult1, d1.getTimestamp(),
        w1.getTimestamp() - d1.getTimestamp() + 1, w1.getTimestamp());
    Assert.assertEquals(w1, result.get(1));

    sessionWindowOperator.processLeftData(d4);
    sessionWindowOperator.processLeftData(d5);
    Assert.assertEquals(2, result.size());
    sessionWindowOperator.processLeftData(d6);
    Assert.assertEquals(3, result.size());
    final Collection<Integer> expectedResult2 = new LinkedList<>();
    expectedResult2.add(3);
    expectedResult2.add(4);
    expectedResult2.add(5);
    checkWindowData(result.get(2), expectedResult2, d3.getTimestamp(),
        d5.getTimestamp() - d3.getTimestamp() + 1, d5.getTimestamp());

    sessionWindowOperator.processLeftWatermark(w2);
    sessionWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(3, result.size());
    sessionWindowOperator.processLeftWatermark(w4);
    Assert.assertEquals(5, result.size());
    final Collection<Integer> expectedResult3 = new LinkedList<>();
    expectedResult3.add(6);
    checkWindowData(
        result.get(3), expectedResult3, d6.getTimestamp(),
        w3.getTimestamp() - d6.getTimestamp() + 1, w3.getTimestamp());
    Assert.assertEquals(w3, result.get(4));

    // Test for getting the state of SessionWindowOperator.
    final SessionWindowOperator<Integer> sessionWindowOperator2 =
        new SessionWindowOperator<>("getOp", sessionInterval);
    sessionWindowOperator2.processLeftData(d6);
    sessionWindowOperator2.processLeftWatermark(w3);

    // Generate the expected SessionWindowOperator's state.
    final Window expectedCurrentWindow = new WindowImpl<>(d6.getTimestamp(), Long.MAX_VALUE, new LinkedList<>());
    expectedCurrentWindow.putData(d6);
    expectedCurrentWindow.putWatermark(w3);
    expectedCurrentWindow.setEnd(w3.getTimestamp());

    final long expectedLatestDataTimestamp = d6.getTimestamp();
    final boolean expectedStartedNewWindow = true;

    // Get the current SessionWindowOperator's state.
    final Map<String, Object> operatorState = sessionWindowOperator2.getOperatorState();
    final Window<Integer> currentWindow = (Window<Integer>)operatorState.get("currentWindow");
    final long latestDataTimestamp = (long)operatorState.get("latestDataTimestamp");
    final boolean startedNewWindow = (boolean)operatorState.get("startedNewWindow");

    // Compare the expected and original operator's state.
    Assert.assertEquals(expectedCurrentWindow, currentWindow);
    Assert.assertEquals(expectedLatestDataTimestamp, latestDataTimestamp);
    Assert.assertEquals(expectedStartedNewWindow, startedNewWindow);

    // Test for setting the state of SessionWindowOperator.
    final SessionWindowOperator sessionWindowOperator3 =
        new SessionWindowOperator("setSessionWindowOperator", sessionInterval);

    // Use the generated state above and set it to a new SessionWindowOperator.
    final Map<String, Object> loadStateMap = new HashMap<>();
    loadStateMap.put("currentWindow", expectedCurrentWindow);
    loadStateMap.put("latestDataTimestamp", expectedLatestDataTimestamp);
    loadStateMap.put("startedNewWindow", expectedStartedNewWindow);
    sessionWindowOperator3.setState(loadStateMap);

    // Compare the set operator and the original.
    final Map<String, Object> operatorState3 = sessionWindowOperator3.getOperatorState();
    final Window<Integer> currentWindow3 =
        (Window<Integer>)operatorState3.get("currentWindow");
    final long latestDataTimestamp3 = (long)operatorState3.get("latestDataTimestamp");
    final boolean startedNewWindow3 = (boolean)operatorState3.get("startedNewWindow");
    Assert.assertEquals(currentWindow3, currentWindow);
    Assert.assertEquals(latestDataTimestamp3, latestDataTimestamp);
    Assert.assertEquals(startedNewWindow3, startedNewWindow);
  }
}
