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
package edu.snu.mist.common.utils;

import edu.snu.mist.common.windows.Window;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import org.junit.Assert;

import java.util.Collection;
import java.util.Queue;

/**
 * This is a utility class for operator test.
 */
public final class OperatorTestUtils {

  private OperatorTestUtils() {
    // empty constructor
  }

  /**
   * Checks the windowed result is equal to the expected result.
   */
  public static void checkWindowData(final MistEvent result,
                                     final Collection<Integer> expectedResult,
                                     final long expectedWindowStartMoment,
                                     final long expectedWindowSize,
                                     final long expectedWindowTimestamp) {
    Assert.assertTrue(result.isData());
    Assert.assertTrue(((MistDataEvent)result).getValue() instanceof WindowData);
    final WindowData windowData = (WindowData)((MistDataEvent)result).getValue();
    Assert.assertEquals(expectedResult, windowData.getDataCollection());
    Assert.assertEquals(expectedWindowStartMoment, windowData.getStart());
    Assert.assertEquals(expectedWindowSize, windowData.getEnd() - windowData.getStart() + 1);
    Assert.assertEquals(expectedWindowTimestamp, result.getTimestamp());
  }

  /**
   * Checks if two windows with integer data are equal.
   */
  public static void checkWindowEquality(final Window<Integer> window1, final Window<Integer> window2) {
    Assert.assertEquals(window1.getStart(), window2.getStart());
    Assert.assertEquals(window1.getEnd(), window2.getEnd());
    Assert.assertEquals(window1.getDataCollection(), window2.getDataCollection());
    Assert.assertEquals(window1.getLatestTimestamp(), window2.getLatestTimestamp());
    Assert.assertEquals(window1.getLatestWatermark().getTimestamp(), window2.getLatestWatermark().getTimestamp());
  }

  /**
   * Checks if two windows with integer data are equal.
   */
  public static void checkWindowQueueEquality(final Queue<Window<Integer>> windowQueue1,
                                              final Queue<Window<Integer>> windowQueue2) {
    Assert.assertEquals(windowQueue1.size(), windowQueue2.size());
    while (!windowQueue1.isEmpty()) {
      checkWindowEquality(windowQueue1.poll(), windowQueue2.poll());
    }
  }
}
