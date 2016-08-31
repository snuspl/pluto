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

import edu.snu.mist.api.window.WindowData;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class TimeWindowOperatorTest {

  /**
   * Test TimeWindowOperator creating sliding window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testSlidingTimeWindowOperator() throws InterruptedException {
    final int windowSize = 500;
    final int emitPeriod = 250;

    final TimeWindowOperator<Integer> timeWindowOperator =
        new TimeWindowOperator<>("testQuery", "testAggOp", windowSize, emitPeriod);

    final List<MistEvent> result = new LinkedList<>();
    timeWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // Window1-------------:
    // Window2------------------------------:
    //                   Window3------------------------------:
    //                                      Window4------------------------------:
    //                                                     Window5-----------------------------: (will not be emitted)
    // d1-----------------------d2---------------d3-w1-------------w2-----------------w3:
    // expected results:
    // d1 in Window1
    // d1, d2 in Window2
    // d2, d3, w1 in Window3
    // d3, w2 in Window4
    final MistDataEvent d1 = new MistDataEvent(1, 1000L);
    timeWindowOperator.processLeftData(d1);
    Assert.assertEquals(0, result.size());

    final MistDataEvent d2 = new MistDataEvent(2, 1300L);
    timeWindowOperator.processLeftData(d2);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertTrue(((MistDataEvent)result.get(0)).getValue() instanceof WindowData);
    final WindowData windowData1 = (WindowData)((MistDataEvent)result.get(0)).getValue();
    Assert.assertEquals(1, windowData1.getDataCollection().size());
    Assert.assertEquals(1, windowData1.getDataCollection().iterator().next());
    Assert.assertEquals(d1.getTimestamp(), windowData1.getStart());
    Assert.assertEquals(emitPeriod, windowData1.getEnd() - windowData1.getStart());
    Assert.assertEquals(d1.getTimestamp(), result.get(0).getTimestamp());

    final MistDataEvent d3 = new MistDataEvent(3, 1550);
    final MistWatermarkEvent w1 = new MistWatermarkEvent(1550);
    timeWindowOperator.processLeftData(d3);
    timeWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.get(1).isData());
    Assert.assertTrue(((MistDataEvent)result.get(1)).getValue() instanceof WindowData);
    final WindowData windowData2 = (WindowData)((MistDataEvent)result.get(1)).getValue();
    Assert.assertEquals(2, windowData2.getDataCollection().size());
    final Iterator itr1 = windowData2.getDataCollection().iterator();
    Assert.assertEquals(1, itr1.next());
    Assert.assertEquals(2, itr1.next());
    Assert.assertEquals(d1.getTimestamp(), windowData2.getStart());
    Assert.assertEquals(windowSize, windowData2.getEnd() - windowData2.getStart());
    Assert.assertEquals(d2.getTimestamp(), result.get(1).getTimestamp());

    final MistWatermarkEvent w2 = new MistWatermarkEvent(1800);
    timeWindowOperator.processLeftWatermark(w2);
    Assert.assertEquals(4, result.size());
    Assert.assertTrue(result.get(2).isData());
    Assert.assertTrue(((MistDataEvent)result.get(2)).getValue() instanceof WindowData);
    final WindowData windowData3 = (WindowData)((MistDataEvent)result.get(2)).getValue();
    Assert.assertEquals(2, windowData3.getDataCollection().size());
    final Iterator itr2 = windowData3.getDataCollection().iterator();
    Assert.assertEquals(2, itr2.next());
    Assert.assertEquals(3, itr2.next());
    Assert.assertEquals(d1.getTimestamp() + emitPeriod, windowData3.getStart());
    Assert.assertEquals(windowSize, windowData3.getEnd() - windowData3.getStart());
    Assert.assertEquals(d3.getTimestamp(), result.get(2).getTimestamp());
    Assert.assertEquals(w1, result.get(3));
    
    final MistWatermarkEvent w3 = new MistWatermarkEvent(2050);
    timeWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(6, result.size());
    Assert.assertTrue(result.get(4).isData());
    Assert.assertTrue(((MistDataEvent)result.get(4)).getValue() instanceof WindowData);
    final WindowData windowData4 = (WindowData)((MistDataEvent)result.get(4)).getValue();
    Assert.assertEquals(1, windowData4.getDataCollection().size());
    Assert.assertEquals(3, windowData4.getDataCollection().iterator().next());
    Assert.assertEquals(d1.getTimestamp() + 2 * emitPeriod, windowData4.getStart());
    Assert.assertEquals(windowSize, windowData4.getEnd() - windowData4.getStart());
    Assert.assertEquals(w2.getTimestamp(), result.get(4).getTimestamp());

    Assert.assertEquals(w2, result.get(5));
  }

  /**
   * Test TimeWindowOperator creating hopping window.
   * It receives some continuous data stream and groups them as a collection.
   */
  @Test
  public void testHoppingTimeWindowOperator() throws InterruptedException {
    final int windowSize = 500;
    final int emitPeriod = 750;

    final TimeWindowOperator<Integer> timeWindowOperator =
            new TimeWindowOperator<>("testQuery", "testAggOp", windowSize, emitPeriod);

    final List<MistEvent> result = new LinkedList<>();
    timeWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // Window1--------------------------:
    //                                                     Window2--------------------------:
    // d1-----------------------d2-------------w1-------------d3--------w2-w3-------------------w4:
    // expected results:
    // d1, d2 in Window1
    // d3, w3 in Window2
    final MistDataEvent d1 = new MistDataEvent(1, 0L);
    timeWindowOperator.processLeftData(d1);
    final MistDataEvent d2 = new MistDataEvent(2, 400L);
    timeWindowOperator.processLeftData(d2);
    Assert.assertEquals(0, result.size());

    final MistWatermarkEvent w1 = new MistWatermarkEvent(600L);
    timeWindowOperator.processLeftWatermark(w1);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertTrue(((MistDataEvent)result.get(0)).getValue() instanceof WindowData);
    final WindowData windowData1 = (WindowData)((MistDataEvent)result.get(0)).getValue();
    Assert.assertEquals(2, windowData1.getDataCollection().size());
    final Iterator itr1 = windowData1.getDataCollection().iterator();
    Assert.assertEquals(1, itr1.next());
    Assert.assertEquals(2, itr1.next());
    Assert.assertEquals(d1.getTimestamp(), windowData1.getStart());
    Assert.assertEquals(windowSize, windowData1.getEnd() - windowData1.getStart());
    Assert.assertEquals(d2.getTimestamp(), result.get(0).getTimestamp());

    final MistDataEvent d3 = new MistDataEvent(3, 1100L);
    timeWindowOperator.processLeftData(d3);
    final MistWatermarkEvent w2 = new MistWatermarkEvent(800L);
    timeWindowOperator.processLeftWatermark(w2);
    final MistWatermarkEvent w3 = new MistWatermarkEvent(900L);
    timeWindowOperator.processLeftWatermark(w3);
    Assert.assertEquals(1, result.size());

    final MistWatermarkEvent w4 = new MistWatermarkEvent(1300L);
    timeWindowOperator.processLeftWatermark(w4);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.get(1).isData());
    Assert.assertTrue(((MistDataEvent)result.get(1)).getValue() instanceof WindowData);
    final WindowData windowData2 = (WindowData)((MistDataEvent)result.get(1)).getValue();
    Assert.assertEquals(1, windowData2.getDataCollection().size());
    Assert.assertEquals(3, windowData2.getDataCollection().iterator().next());
    Assert.assertEquals(d1.getTimestamp() + emitPeriod, windowData2.getStart());
    Assert.assertEquals(windowSize, windowData2.getEnd() - windowData2.getStart());
    Assert.assertEquals(d3.getTimestamp(), result.get(1).getTimestamp());

    Assert.assertEquals(w3, result.get(2));
  }

  /**
   * Simple output emitter which adds events to the list.
   */
  private class SimpleOutputEmitter implements OutputEmitter {
    private final List<MistEvent> list;

    public SimpleOutputEmitter(final List<MistEvent> list) {
      this.list = list;
    }

    @Override
    public void emitData(final MistDataEvent data) {
      list.add(data);
    }
    @Override
    public void emitWatermark(final MistWatermarkEvent watermark) {
      list.add(watermark);
    }
  }
}
