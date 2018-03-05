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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.MISTBiPredicate;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.utils.OutputBufferEmitter;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.common.windows.WindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public final class JoinOperatorTest {

  /**
   * Tests JoinOperator.
   * It joins a pair of inputs in two streams that has same key.
   */
  @Test
  public void testAggregateWindowOperator() {
    // input stream events
    final WindowImpl<Integer> window = new WindowImpl<>(0L, 100L);
    window.putData(new MistDataEvent(new Tuple2<>(new Tuple2<>("Hello", 1), null), 10));
    window.putData(new MistDataEvent(new Tuple2<>(new Tuple2<>("MIST", 2), null), 20));
    window.putData(new MistDataEvent(new Tuple2<>(null, new Tuple2<>(1, 3000L)), 30));
    window.putData(new MistDataEvent(new Tuple2<>(new Tuple2<>("SNUCMS", 3), null), 40));
    window.putData(new MistDataEvent(new Tuple2<>(null, new Tuple2<>(1, 4000L)), 50));
    window.putData(new MistDataEvent(new Tuple2<>(null, new Tuple2<>(2, 5000L)), 60));
    final MistDataEvent dataEvent = new MistDataEvent(window, 60L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(101L, false);

    // predicate that tests whether two input data have same key or not
    final MISTBiPredicate<Tuple2<String, Integer>, Tuple2<Integer, Long>> joinPredicate =
        (tuple1, tuple2) -> tuple1.get(1).equals(tuple2.get(0));

    final JoinOperator<Tuple2<String, Integer>, Tuple2<Integer, Long>> joinOperator =
        new JoinOperator<>(joinPredicate);

    // expected pairs
    // {Hello, 1} and {1, 3000L}
    // {Hello, 1} and {1, 4000L}
    // {MIST, 2} and {2, 5000L}
    final List<MistEvent> result = new LinkedList<>();
    joinOperator.setOutputEmitter(new OutputBufferEmitter(result));

    joinOperator.processLeftData(dataEvent);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertTrue(((MistDataEvent)result.get(0)).getValue() instanceof WindowData);
    final WindowData windowData = (WindowData)((MistDataEvent)result.get(0)).getValue();
    Assert.assertEquals(0L, windowData.getStart());
    Assert.assertEquals(99L, windowData.getEnd());
    final Collection<Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Long>>> dataCollection =
        windowData.getDataCollection();
    final Iterator iterator = dataCollection.iterator();
    Assert.assertEquals(3, dataCollection.size());
    Assert.assertEquals(new Tuple2<>(new Tuple2<>("Hello", 1), new Tuple2<>(1, 3000L)), iterator.next());
    Assert.assertEquals(new Tuple2<>(new Tuple2<>("Hello", 1), new Tuple2<>(1, 4000L)), iterator.next());
    Assert.assertEquals(new Tuple2<>(new Tuple2<>("MIST", 2), new Tuple2<>(2, 5000L)), iterator.next());
    Assert.assertEquals(60L, result.get(0).getTimestamp());

    joinOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(watermarkEvent, result.get(1));
  }
}
