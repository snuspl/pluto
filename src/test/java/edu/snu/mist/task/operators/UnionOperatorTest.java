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

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
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
    final MistWatermarkEvent lw1 = new MistWatermarkEvent(10L);
    final MistDataEvent d = new MistDataEvent("d", 1L);
    final MistDataEvent e = new MistDataEvent("e", 2L);
    final MistDataEvent f = new MistDataEvent("f", 4L);
    final MistWatermarkEvent rw1 = new MistWatermarkEvent(4L);
    final MistDataEvent g = new MistDataEvent("g", 6L);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(OperatorId.class, "testUnionOperator");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final UnionOperator unionOperator = injector.getInstance(UnionOperator.class);

    List<MistEvent> result = new LinkedList<>();
    unionOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    // Test:
    //  * Left: ---W:10 -- c ---- b ------- a ---->
    //  * Right -g------------W:4--- f--e--d------>
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
    Assert.assertEquals(7, result.size());
    Assert.assertEquals(lw1, result.get(6));

    unionOperator.processRightData(g);
    Assert.assertEquals(9, result.size());
    Assert.assertEquals(c, result.get(7));
    Assert.assertEquals(g, result.get(8));
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
