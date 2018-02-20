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
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.utils.IndexOutputEmitter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class ConditionalBranchOperatorTest {

  /**
   * Test conditional branch operation.
   * It classifies the input string according to it's length.
   */
  @Test
  public void testConditionalBranchOperator() throws InjectionException {
    // input stream events
    final MistDataEvent d1 = new MistDataEvent("1", 1L);
    final MistDataEvent d2 = new MistDataEvent("22", 2L);
    final MistDataEvent d3 = new MistDataEvent("333", 3L);
    final MistDataEvent d4 = new MistDataEvent("4444", 4L);
    final MistDataEvent d5 = new MistDataEvent("55555", 5L);
    final MistWatermarkEvent w1 = new MistWatermarkEvent(6L);

    // classify the string according to it's length
    final List<MISTPredicate<String>> predicates = new ArrayList<>();
    // "1" will be passed with index 1
    predicates.add((input) -> input.length() < 2);
    // "22" will be passed with index 2
    predicates.add((input) -> input.length() < 3);
    // "333", "4444" will be passed with index 3
    predicates.add((input) -> input.length() < 5);
    // "55555" will not be passed

    final List<Tuple<MistEvent, Integer>> result = new LinkedList<>();
    final ConditionalBranchOperator<String> conditionalBranchOperator = new ConditionalBranchOperator<>(predicates);
    conditionalBranchOperator.setOutputEmitter(new IndexOutputEmitter(result));

    conditionalBranchOperator.processLeftData(d1);
    conditionalBranchOperator.processLeftData(d2);
    conditionalBranchOperator.processLeftData(d3);
    conditionalBranchOperator.processLeftData(d4);
    conditionalBranchOperator.processLeftData(d5);
    conditionalBranchOperator.processLeftWatermark(w1);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals(new Tuple<>(d1, 1), result.get(0));
    Assert.assertEquals(new Tuple<>(d2, 2), result.get(1));
    Assert.assertEquals(new Tuple<>(d3, 3), result.get(2));
    Assert.assertEquals(new Tuple<>(d4, 3), result.get(3));
    Assert.assertEquals(new Tuple<>(w1, 0), result.get(4));
  }
}
