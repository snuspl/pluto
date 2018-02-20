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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.OneStreamOperator;

public final class OperatorChainTest {
  // TODO[MIST-70]: Consider concurrency issue in execution of OperatorChain

  /**
   * Tests whether the OperatorChain correctly executes the chained operators.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @SuppressWarnings("unchecked")
  /*
  public void operatorChainExecutionTest() throws InjectionException {

    final List<MistEvent> result = new LinkedList<>();
    final Integer input = 3;

    final OperatorChain operatorChain = new DefaultOperatorChainImpl("testOpChain");
    operatorChain.setOutputEmitter(new OutputBufferEmitter(result));

    final String squareOpId = "squareOp";
    final String incOpId = "incOp";
    final String doubleOpId = "doubleOp";

    final PhysicalOperator squareOp = new DefaultPhysicalOperatorImpl(squareOpId,
        null, new SquareOperator(), operatorChain);
    final PhysicalOperator incOp = new DefaultPhysicalOperatorImpl(incOpId,
        null, new IncrementOperator(), operatorChain);
    final PhysicalOperator doubleOp = new DefaultPhysicalOperatorImpl(doubleOpId,
        null, new DoubleOperator(), operatorChain);

    // 2 * (input * input + 1)
    long timestamp = 1;
    final MistDataEvent expected1 = new MistDataEvent(2 * (input * input + 1), timestamp);
    operatorChain.insertToHead(doubleOp);
    operatorChain.insertToHead(incOp);
    operatorChain.insertToHead(squareOp);
    operatorChain.addNextEvent(new MistDataEvent(input, timestamp), Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(expected1, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, squareOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    // Check index
    Assert.assertEquals(squareOp, operatorChain.get(0));
    Assert.assertEquals(incOp, operatorChain.get(1));
    Assert.assertEquals(doubleOp, operatorChain.get(2));

    // 2 * (input + 1)
    timestamp += 1;
    operatorChain.removeFromHead();
    final MistDataEvent expected2 = new MistDataEvent(2 * (input + 1), timestamp);
    operatorChain.addNextEvent(new MistDataEvent(input, timestamp), Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(expected2, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    // Check index
    Assert.assertEquals(incOp, operatorChain.get(0));
    Assert.assertEquals(doubleOp, operatorChain.get(1));

    // input + 1
    timestamp += 1;
    operatorChain.removeFromTail();
    final MistDataEvent expected3 = new MistDataEvent(input + 1, timestamp);
    operatorChain.addNextEvent(new MistDataEvent(input, timestamp), Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(expected3, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    // Check index
    Assert.assertEquals(incOp, operatorChain.get(0));


    // 2 * input + 1
    timestamp += 1;
    operatorChain.insertToHead(doubleOp);
    final MistDataEvent expected4 = new MistDataEvent(2 * input + 1, timestamp);
    operatorChain.addNextEvent(new MistDataEvent(input, timestamp), Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(expected4, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    // Check index
    Assert.assertEquals(doubleOp, operatorChain.get(0));
    Assert.assertEquals(incOp, operatorChain.get(1));

    // (2 * input + 1) * (2 * input + 1)
    timestamp += 1;
    operatorChain.insertToTail(squareOp);
    final MistDataEvent expected5 = new MistDataEvent((2 * input + 1) * (2 * input + 1), timestamp);
    operatorChain.addNextEvent(new MistDataEvent(input, timestamp), Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(expected5, result.remove(0));
    // Check latest timestamp
    Assert.assertEquals(timestamp, doubleOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestDataTimestamp());
    Assert.assertEquals(timestamp, squareOp.getLatestDataTimestamp());
    // Check index
    Assert.assertEquals(doubleOp, operatorChain.get(0));
    Assert.assertEquals(incOp, operatorChain.get(1));
    Assert.assertEquals(squareOp, operatorChain.get(2));

    // Check watermark timestamp
    timestamp += 1;
    final MistEvent mistWatermarkEvent = new MistWatermarkEvent(timestamp);
    operatorChain.addNextEvent(mistWatermarkEvent, Direction.LEFT);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    operatorChain.processNextEvent();
    Assert.assertEquals(0, operatorChain.numberOfEvents());
    Assert.assertEquals(timestamp, doubleOp.getLatestWatermarkTimestamp());
    Assert.assertEquals(timestamp, incOp.getLatestWatermarkTimestamp());
    Assert.assertEquals(timestamp, squareOp.getLatestWatermarkTimestamp());
  }
*/

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
