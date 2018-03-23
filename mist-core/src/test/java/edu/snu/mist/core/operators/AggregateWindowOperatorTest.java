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

import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.core.operators.window.AggregateWindowOperator;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.core.operators.window.WindowImpl;
import edu.snu.mist.core.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class AggregateWindowOperatorTest {

  /**
   * Test AggregateWindowOperator.
   * It calculates the maximum value from the collection.
   */
  @Test
  public void testAggregateWindowOperator() {
    // input stream events
    final WindowImpl<Integer> window = new WindowImpl<>(0L, 100L);
    window.putData(new MistDataEvent(10, 10));
    window.putData(new MistDataEvent(20, 20));
    window.putData(new MistDataEvent(15, 30));
    window.putData(new MistDataEvent(30, 90));
    final MistDataEvent dataEvent = new MistDataEvent(window, 90L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(101L);

    // functions that dealing with input WindowData
    final MISTFunction<WindowData<Integer>, String> aggregateFunc =
        (windowData) -> {
          return windowData.getDataCollection().toString() + ", " + windowData.getStart() + ", " + windowData.getEnd();
        };

    final AggregateWindowOperator<Integer, String> aggregateWindowOperator =
        new AggregateWindowOperator<>(aggregateFunc);

    final List<MistEvent> result = new LinkedList<>();
    aggregateWindowOperator.setOutputEmitter(new OutputBufferEmitter(result));

    aggregateWindowOperator.processLeftData(dataEvent);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertEquals("[10, 20, 15, 30], 0, 99", ((MistDataEvent)result.get(0)).getValue());
    Assert.assertEquals(90L, result.get(0).getTimestamp());

    aggregateWindowOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(watermarkEvent, result.get(1));
  }
}
