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
package edu.snu.mist.task;

import edu.snu.mist.task.operators.BaseOperator;
import edu.snu.mist.task.operators.Operator;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class OperatorChainTest {
  // TODO[MIST-70]: Consider concurrency issue in execution of OperatorChain

  /**
   * Tests whether the OperatorChain correctly executes the chained operators.
   * @throws InjectionException
   */
  @SuppressWarnings("unchecked")
  @Test
  public void operatorChainExecutionTest() throws InjectionException {

    final List<Integer> result = new LinkedList<>();
    final Integer input = 3;

    final OperatorChain operatorChain = new DefaultOperatorChain();
    operatorChain.setOutputEmitter(output -> result.add((Integer) output));

    final Operator<Integer, Integer> squareOp = new SquareOperator();
    final Operator<Integer, Integer> incOp = new IncrementOperator();
    final Operator<Integer, Integer> doubleOp = new DoubleOperator();

    // 2 * (input * input + 1)
    final Integer expected1 = 2 * (input * input + 1);
    operatorChain.insertToHead(doubleOp);
    operatorChain.insertToHead(incOp);
    operatorChain.insertToHead(squareOp);
    operatorChain.handle(input);
    Assert.assertEquals(expected1, result.remove(0));

    // 2 * (input + 1)
    operatorChain.removeFromHead();
    final Integer expected2 = 2 * (input + 1);
    operatorChain.handle(input);
    Assert.assertEquals(expected2, result.remove(0));

    // input + 1
    operatorChain.removeFromTail();
    final Integer expected3 = input + 1;
    operatorChain.handle(input);
    Assert.assertEquals(expected3, result.remove(0));

    // 2 * input + 1
    operatorChain.insertToHead(doubleOp);
    final Integer expected4 = 2 * input + 1;
    operatorChain.handle(input);
    Assert.assertEquals(expected4, result.remove(0));

    // (2 * input + 1) * (2 * input + 1)
    operatorChain.insertToTail(squareOp);
    final Integer expected5 = (2 * input + 1) * (2 * input + 1);
    operatorChain.handle(input);
    Assert.assertEquals(expected5, result.remove(0));
  }

  /**
   * This emits squared inputs.
   */
  class SquareOperator extends BaseOperator<Integer, Integer> {
    SquareOperator() {
      super();
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input * input);
    }
  }

  /**
   * This increments the input.
   */
  class IncrementOperator extends BaseOperator<Integer, Integer> {
    IncrementOperator() {
      super();
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input + 1);
    }
  }

  /**
   * This doubles the input.
   */
  class DoubleOperator extends BaseOperator<Integer, Integer> {
    DoubleOperator() {
      super();
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input * 2);
    }
  }
}
