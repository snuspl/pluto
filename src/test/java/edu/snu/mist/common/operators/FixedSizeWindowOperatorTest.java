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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static edu.snu.mist.common.utils.OperatorTestUtils.checkWindowData;

public final class FixedSizeWindowOperatorTest {

  /**
   * Some MistDataEvent and MistWatermarkEvent used during these tests.
   */
  private final MistDataEvent d1 = new MistDataEvent(1, 1000L);
  private final MistDataEvent d2 = new MistDataEvent(2, 1300L);
  private final MistDataEvent d3 = new MistDataEvent(3, 1550L);
  private final MistDataEvent d4 = new MistDataEvent(4, 1790L);
  private final MistDataEvent d5 = new MistDataEvent(5, 2000L);
  private final MistDataEvent d6 = new MistDataEvent(6, 3000L);
  private final MistDataEvent d7 = new MistDataEvent(7, 4000L);
  private final MistDataEvent d8 = new MistDataEvent(8, 5000L);
  private final MistDataEvent d9 = new MistDataEvent(9, 6000L);
  private final MistWatermarkEvent w1 = new MistWatermarkEvent(1550L);
  private final MistWatermarkEvent w2 = new MistWatermarkEvent(1800L);
  private final MistWatermarkEvent w3 = new MistWatermarkEvent(2050L);
  private final MistWatermarkEvent w4 = new MistWatermarkEvent(2300L);

  /**
   * Test TimeWindowOperator creating sliding window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testSlidingTimeWindowOperator() throws InterruptedException {
    final int windowSize = 500;
    final int emissionInterval = 250;

    final TimeWindowOperator<Integer> timeWindowOperator =
        new TimeWindowOperator<>("testAggOp", windowSize, emissionInterval);

    final List<MistEvent> result = new LinkedList<>();
    timeWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // (1000)Window1-----(1249):
    // (1000)Window2----------------------(1499):
    //                   (1250)Window3----------------(1749):
    //                                    (1500)Window4----------------------(1999):
    //                                                (1750)Window5-----------------------------: (will not be emitted)
    // d1--------------------------d2-----------d3-w1-------------w2-----------------w3:
    // expected results:
    // d1 in Window1
    // d1, d2 in Window2
    // d2, d3, w1 in Window3
    // d3, w2 in Window4
    timeWindowOperator.processLeftData(d1);
    Assert.assertEquals(0, result.size());

    timeWindowOperator.processLeftData(d2);
    Assert.assertEquals(1, result.size());
    final Collection<Integer> expectedResult1 = new LinkedList<>();
    expectedResult1.add(1);
    checkWindowData(result.get(0), expectedResult1, d1.getTimestamp(), emissionInterval, d1.getTimestamp());

    timeWindowOperator.processLeftData(d3);
    timeWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(2, result.size());
    final Collection<Integer> expectedResult2 = new LinkedList<>();
    expectedResult2.add(1);
    expectedResult2.add(2);
    checkWindowData(result.get(1), expectedResult2, d1.getTimestamp(), windowSize, d2.getTimestamp());

    timeWindowOperator.processLeftWatermark(w2);
    Assert.assertEquals(4, result.size());
    final Collection<Integer> expectedResult3 = new LinkedList<>();
    expectedResult3.add(2);
    expectedResult3.add(3);
    checkWindowData(
        result.get(2), expectedResult3, d1.getTimestamp() + emissionInterval, windowSize, d3.getTimestamp());
    Assert.assertEquals(w1, result.get(3));

    timeWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(6, result.size());
    final Collection<Integer> expectedResult4 = new LinkedList<>();
    expectedResult4.add(3);
    checkWindowData(
        result.get(4), expectedResult4, d1.getTimestamp() + 2 * emissionInterval, windowSize, w2.getTimestamp());
    Assert.assertEquals(w2, result.get(5));
  }

  /**
   * Test TimeWindowOperator creating hopping window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testHoppingTimeWindowOperator() throws InterruptedException {
    final int windowSize = 500;
    final int emissionInterval = 750;

    final TimeWindowOperator<Integer> timeWindowOperator =
            new TimeWindowOperator<>("testAggOp", windowSize, emissionInterval);

    final List<MistEvent> result = new LinkedList<>();
    timeWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // (1000)Window1------------------(1499):
    //                                                     (1750)Window2------------------(2249):
    // d1-----------------------d2---------------d3-w1------------d4-w2-----------------w3------w4:
    // expected results:
    // d1, d2 in Window1
    // d4, w3 in Window2
    timeWindowOperator.processLeftData(d1);
    timeWindowOperator.processLeftData(d2);
    Assert.assertEquals(0, result.size());

    timeWindowOperator.processLeftData(d3);
    timeWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(1, result.size());
    final Collection<Integer> expectedResult1 = new LinkedList<>();
    expectedResult1.add(1);
    expectedResult1.add(2);
    checkWindowData(result.get(0), expectedResult1, d1.getTimestamp(), windowSize, d2.getTimestamp());

    timeWindowOperator.processLeftData(d4);
    timeWindowOperator.processLeftWatermark(w2);
    timeWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(1, result.size());

    timeWindowOperator.processLeftWatermark(w4);
    Assert.assertEquals(3, result.size());
    final Collection<Integer> expectedResult2 = new LinkedList<>();
    expectedResult2.add(4);
    checkWindowData(
        result.get(1), expectedResult2, d1.getTimestamp() + emissionInterval, windowSize, w3.getTimestamp());
    Assert.assertEquals(w3, result.get(2));
  }

  /**
   * Test CountWindowOperator creating sliding window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testSlidingCountWindowOperator() throws InterruptedException {
    final int windowSize = 5;
    final int emissionInterval = 3;

    final CountWindowOperator<Integer> countWindowOperator =
            new CountWindowOperator<>("testAggOp", windowSize, emissionInterval);

    final List<MistEvent> result = new LinkedList<>();
    countWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // (1)Window1(3):
    //       (2)Window2--------(6):
    //                     (5)Window3-------(9):
    //                                   (8)Window4--------------: (will not be emitted)
    // d1----d2--d3--d4-w1-d5--d6--d7-w2-d8-d9:
    // expected results:
    // d1, d2, d3 in Window1
    // d2, d3, d4, d5, d6, w1 in Window2
    // d5, d6, d7, d8, d9, w2 in Window3
    countWindowOperator.processLeftData(d1);
    countWindowOperator.processLeftData(d2);
    Assert.assertEquals(0, result.size());

    countWindowOperator.processLeftData(d3);
    Assert.assertEquals(1, result.size());
    final Collection<Integer> expectedResult1 = new LinkedList<>();
    expectedResult1.add(1);
    expectedResult1.add(2);
    expectedResult1.add(3);
    checkWindowData(result.get(0), expectedResult1, 1L, emissionInterval, d3.getTimestamp());

    countWindowOperator.processLeftData(d4);
    countWindowOperator.processLeftWatermark(w1);
    countWindowOperator.processLeftData(d5);
    Assert.assertEquals(1, result.size());

    countWindowOperator.processLeftData(d6);
    Assert.assertEquals(3, result.size());
    final Collection<Integer> expectedResult2 = new LinkedList<>();
    expectedResult2.add(2);
    expectedResult2.add(3);
    expectedResult2.add(4);
    expectedResult2.add(5);
    expectedResult2.add(6);
    checkWindowData(result.get(1), expectedResult2, 2L, windowSize, d6.getTimestamp());
    Assert.assertEquals(w1, result.get(2));

    countWindowOperator.processLeftData(d7);
    countWindowOperator.processLeftWatermark(w2);
    countWindowOperator.processLeftData(d8);
    Assert.assertEquals(3, result.size());

    countWindowOperator.processLeftData(d9);
    Assert.assertEquals(5, result.size());
    final Collection<Integer> expectedResult3 = new LinkedList<>();
    expectedResult3.add(5);
    expectedResult3.add(6);
    expectedResult3.add(7);
    expectedResult3.add(8);
    expectedResult3.add(9);
    checkWindowData(result.get(3), expectedResult3, 5L, windowSize, d9.getTimestamp());
    Assert.assertEquals(w2, result.get(4));
  }

  /**
   * Test CountWindowOperator creating hopping window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testHoppingCountWindowOperator() throws InterruptedException {
    final int windowSize = 3;
    final int emissionInterval = 5;

    final CountWindowOperator<Integer> countWindowOperator =
        new CountWindowOperator<>("testAggOp", windowSize, emissionInterval);

    final List<MistEvent> result = new LinkedList<>();
    countWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // (1)Window1-(3):
    //                        (6)Window2--(8):
    // d1--d2-w1--d3--d4--d5--d6-w2-d7-w3-d8:
    // expected results:
    // d1, d2, d3, w1 in Window1
    // d6, d7, d8, w3 in Window2
    countWindowOperator.processLeftData(d1);
    countWindowOperator.processLeftData(d2);
    countWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(0, result.size());

    countWindowOperator.processLeftData(d3);
    Assert.assertEquals(2, result.size());
    final Collection<Integer> expectedResult1 = new LinkedList<>();
    expectedResult1.add(1);
    expectedResult1.add(2);
    expectedResult1.add(3);
    checkWindowData(result.get(0), expectedResult1, 1L, windowSize, d3.getTimestamp());
    Assert.assertEquals(w1, result.get(1));

    countWindowOperator.processLeftData(d4);
    countWindowOperator.processLeftData(d5);
    countWindowOperator.processLeftData(d6);
    countWindowOperator.processLeftWatermark(w2);
    countWindowOperator.processLeftData(d7);
    countWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(2, result.size());

    countWindowOperator.processLeftData(d8);
    Assert.assertEquals(4, result.size());
    final Collection<Integer> expectedResult2 = new LinkedList<>();
    expectedResult2.add(6);
    expectedResult2.add(7);
    expectedResult2.add(8);
    checkWindowData(result.get(2), expectedResult2, emissionInterval + 1L, windowSize, d8.getTimestamp());
    Assert.assertEquals(w3, result.get(3));
  }
}
