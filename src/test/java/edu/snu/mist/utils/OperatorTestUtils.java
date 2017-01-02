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
package edu.snu.mist.utils;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.windows.WindowData;
import org.junit.Assert;

import java.util.Collection;

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
}
