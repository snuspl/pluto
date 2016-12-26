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
package edu.snu.mist.core.task;

import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.MistWatermarkEvent;
import edu.snu.mist.core.task.operators.OneStreamOperator;
import edu.snu.mist.core.task.operators.Operator;
import edu.snu.mist.core.task.utils.TestOutputEmitter;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class PartitionedQueryTest {
  // TODO[MIST-70]: Consider concurrency issue in execution of PartitionedQuery

  private MistDataEvent createEvent(final int val) {
    return new MistDataEvent(val, System.currentTimeMillis());
  }

  /**
   * Tests whether the PartitionedQuery correctly executes the chained operators.
   * @throws InjectionException
   */
  @SuppressWarnings("unchecked")
  @Test
  public void partitionedQueryExecutionTest() throws InjectionException {

    final List<Integer> result = new LinkedList<>();
    final Integer input = 3;

    final PartitionedQuery partitionedQuery = new DefaultPartitionedQuery();
    partitionedQuery.setOutputEmitter(new TestOutputEmitter<>(result));

    final Injector injector = Tang.Factory.getTang().newInjector();
    final StringIdentifierFactory idFactory = injector.getInstance(StringIdentifierFactory.class);
    final String squareOpId = "squareOp";
    final String incOpId = "incOp";
    final String doubleOpId = "doubleOp";

    final Operator squareOp = new SquareOperator(squareOpId);
    final Operator incOp = new IncrementOperator(incOpId);
    final Operator doubleOp = new DoubleOperator(doubleOpId);

    // 2 * (input * input + 1)
    final Integer expected1 = 2 * (input * input + 1);
    partitionedQuery.insertToHead(doubleOp);
    partitionedQuery.insertToHead(incOp);
    partitionedQuery.insertToHead(squareOp);
    partitionedQuery.addNextEvent(createEvent(input), Direction.LEFT);
    partitionedQuery.processNextEvent();
    Assert.assertEquals(expected1, result.remove(0));

    // 2 * (input + 1)
    partitionedQuery.removeFromHead();
    final Integer expected2 = 2 * (input + 1);
    partitionedQuery.addNextEvent(createEvent(input), Direction.LEFT);
    partitionedQuery.processNextEvent();
    Assert.assertEquals(expected2, result.remove(0));

    // input + 1
    partitionedQuery.removeFromTail();
    final Integer expected3 = input + 1;
    partitionedQuery.addNextEvent(createEvent(input), Direction.LEFT);
    partitionedQuery.processNextEvent();
    Assert.assertEquals(expected3, result.remove(0));

    // 2 * input + 1
    partitionedQuery.insertToHead(doubleOp);
    final Integer expected4 = 2 * input + 1;
    partitionedQuery.addNextEvent(createEvent(input), Direction.LEFT);
    partitionedQuery.processNextEvent();
    Assert.assertEquals(expected4, result.remove(0));

    // (2 * input + 1) * (2 * input + 1)
    partitionedQuery.insertToTail(squareOp);
    final Integer expected5 = (2 * input + 1) * (2 * input + 1);
    partitionedQuery.addNextEvent(createEvent(input), Direction.LEFT);
    partitionedQuery.processNextEvent();
    Assert.assertEquals(expected5, result.remove(0));
  }

  /**
   * This emits squared inputs.
   */
  class SquareOperator extends OneStreamOperator {
    SquareOperator(final String operatorId) {
      super(operatorId);
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int i = (int)data.getValue();
      final int val = i * i;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }

  /**
   * This increments the input.
   */
  class IncrementOperator extends OneStreamOperator {
    IncrementOperator(final String operatorId) {
      super(operatorId);
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int val = (int)data.getValue() + 1;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }

  /**
   * This doubles the input.
   */
  class DoubleOperator extends OneStreamOperator {
    DoubleOperator(final String operatorId) {
      super(operatorId);
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int val = (int)data.getValue() * 2;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }
}
