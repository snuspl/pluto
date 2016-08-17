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

import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class UnionOperatorTest {

  /**
   * Test union operation.
   * It merges two streams into one. (ordered by timestamp)
   */
  @Test
  public void testUnionOperator() throws InjectionException {
    // input stream events
    final MistDataEvent a = new MistDataEvent("a", 1L);
    final MistDataEvent b = new MistDataEvent("b", 3L);
    final MistDataEvent c = new MistDataEvent("c", 5L);
    final MistWatermarkEvent lw1 = new MistWatermarkEvent(9L);
    final MistWatermarkEvent lw2 = new MistWatermarkEvent(10L);
    final MistDataEvent d = new MistDataEvent("d", 1L);
    final MistDataEvent e = new MistDataEvent("e", 2L);
    final MistDataEvent f = new MistDataEvent("f", 4L);
    final MistWatermarkEvent rw1 = new MistWatermarkEvent(3L);
    final MistDataEvent g = new MistDataEvent("g", 6L);
    final MistWatermarkEvent rw2 = new MistWatermarkEvent(11L);

    final UnionOperator unionOperator = new UnionOperator("testQuery", "testUnionOp");

    final List<MistEvent> result = new LinkedList<>();
    unionOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // Test:
    //  * Left: ----------W:10--W:9--c----b--------a---->
    //  * Right -W:11 --g--------------W:3--f--e--d------>
    // Merged stream: ----g--c--f--b--e--d--a-->
    unionOperator.processLeftData(a);
    Assert.assertEquals(0, result.size());

    unionOperator.processRightData(d);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(a, result.get(0));
    Assert.assertEquals(d, result.get(1));

    unionOperator.processRightData(e);
    unionOperator.processRightData(f);
    Assert.assertEquals(2, result.size());

    unionOperator.processLeftData(b);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(e, result.get(2));
    Assert.assertEquals(b, result.get(3));

    unionOperator.processRightWatermark(rw1);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals(rw1, result.get(4));

    unionOperator.processLeftData(c);
    Assert.assertEquals(6, result.size());
    Assert.assertEquals(f, result.get(5));

    unionOperator.processLeftWatermark(lw1);
    Assert.assertEquals(6, result.size());

    unionOperator.processLeftWatermark(lw2);
    Assert.assertEquals(6, result.size());

    unionOperator.processRightData(g);
    Assert.assertEquals(8, result.size());
    Assert.assertEquals(c, result.get(6));
    Assert.assertEquals(g, result.get(7));

    unionOperator.processRightWatermark(rw2);
    Assert.assertEquals(9, result.size());
    Assert.assertEquals(lw2, result.get(8));
  }

  /**
   * Simple output emitter which adds events to the list.
   */
  class SimpleOutputEmitter implements OutputEmitter {
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
