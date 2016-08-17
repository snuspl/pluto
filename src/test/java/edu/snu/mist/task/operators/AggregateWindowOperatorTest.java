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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public final class AggregateWindowOperatorTest {

  /**
   * Test AggregateWindowOperator.
   * It calculates the maximum value from the collection.
   */
  @Test
  public void testAggregateWindowOperator() {
    // input stream events
    final ArrayList<Integer> list = new ArrayList<>();
    list.add(10);
    list.add(20);
    list.add(15);
    list.add(30);
    final MistDataEvent collectionEvent = new MistDataEvent(list, 0L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(1L);

    // functions that dealing with state
    final BiFunction<Integer, Integer, Integer> updateStateFunc =
        (input, state) -> {
          if (input > state) {
            return input;
          } else {
            return state;
          }
        };
    final Function<Integer, String> produceResultFunc = state -> state.toString();
    final Supplier<Integer> initializeStateSup = () -> Integer.MIN_VALUE;

    final AggregateWindowOperator<Integer, String, Integer> aggregateWindowOperator =
        new AggregateWindowOperator<>(
            "testQuery", "testAggOp", updateStateFunc, produceResultFunc, initializeStateSup);

    final List<MistEvent> result = new LinkedList<>();
    aggregateWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    aggregateWindowOperator.processLeftData(collectionEvent);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0) instanceof MistDataEvent);
    Assert.assertEquals("30", ((MistDataEvent)result.get(0)).getValue());
    Assert.assertEquals(0L, result.get(0).getTimestamp());

    aggregateWindowOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(watermarkEvent, result.get(1));
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
