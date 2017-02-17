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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.OneStreamOperator;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestOutputEmitter;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class OperatorChainTest {
  // TODO[MIST-70]: Consider concurrency issue in execution of OperatorChain

  private MistDataEvent createEvent(final int val, final long timestamp) {
    return new MistDataEvent(val, timestamp);
  }

  /**
   * Tests whether the OperatorChain correctly executes the chained operators.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @SuppressWarnings("unchecked")
  @Test
  public void operatorChainExecutionTest() throws InjectionException {

    final List<Integer> result = new LinkedList<>();
    final Integer input = 3;

    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    operatorChain.setOutputEmitter(new TestOutputEmitter<>(result));

    final Injector injector = Tang.Factory.getTang().newInjector();
    final StringIdentifierFactory idFactory = injector.getInstance(StringIdentifierFactory.class);
    final String squareOpId = "squareOp";
    final String incOpId = "incOp";
    final String doubleOpId = "doubleOp";

    final PhysicalOperator squareOp = new DefaultPhysicalOperatorImpl(squareOpId,
        new SquareOperator(), operatorChain);
    final PhysicalOperator incOp = new DefaultPhysicalOperatorImpl(incOpId,
        new IncrementOperator(), operatorChain);
    final PhysicalOperator doubleOp = new DefaultPhysicalOperatorImpl(doubleOpId,
        new DoubleOperator(), operatorChain);

    // 2 * (input * input + 1)
    long timestamp = 1;
    final Integer expected1 = 2 * (input * input + 1);
    operatorChain.insertToHead(doubleOp);
    operatorChain.insertToHead(incOp);
    operatorChain.insertToHead(squareOp);
    operatorChain.addNextEvent(createEvent(input, timestamp), Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(expected1, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, squareOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());

    // 2 * (input + 1)
    timestamp += 1;
    operatorChain.removeFromHead();
    final Integer expected2 = 2 * (input + 1);
    operatorChain.addNextEvent(createEvent(input, timestamp), Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(expected2, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());

    // input + 1
    timestamp += 1;
    operatorChain.removeFromTail();
    final Integer expected3 = input + 1;
    operatorChain.addNextEvent(createEvent(input, timestamp), Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(expected3, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());

    // 2 * input + 1
    timestamp += 1;
    operatorChain.insertToHead(doubleOp);
    final Integer expected4 = 2 * input + 1;
    operatorChain.addNextEvent(createEvent(input, timestamp), Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(expected4, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());

    // (2 * input + 1) * (2 * input + 1)
    timestamp += 1;
    operatorChain.insertToTail(squareOp);
    final Integer expected5 = (2 * input + 1) * (2 * input + 1);
    operatorChain.addNextEvent(createEvent(input, timestamp), Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(expected5, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, squareOp.getLatestDataTimestamp());

    // Check watermark timestamp
    timestamp += 1;
    final MistEvent mistWatermarkEvent = new MistWatermarkEvent(timestamp);
    operatorChain.addNextEvent(mistWatermarkEvent, Direction.LEFT);
    operatorChain.processNextEvent();
    Assert.assertEquals(timestamp, doubleOp.getLatestWatermarkTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestWatermarkTimestamp());
    Assert.assertEquals(timestamp, squareOp.getLatestWatermarkTimestamp());
  }

  /**
   * This emits squared inputs.
   */
  class SquareOperator extends OneStreamOperator {

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int i = (int)data.getValue();
      final int val = i * i;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      outputEmitter.emitWatermark(watermark);
    }
  }

  /**
   * This increments the input.
   */
  class IncrementOperator extends OneStreamOperator {

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int val = (int)data.getValue() + 1;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      outputEmitter.emitWatermark(watermark);
    }
  }

  /**
   * This doubles the input.
   */
  class DoubleOperator extends OneStreamOperator {

    @Override
    public void processLeftData(final MistDataEvent data) {
      final int val = (int)data.getValue() * 2;
      data.setValue(val);
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      outputEmitter.emitWatermark(watermark);
    }
  }
}
