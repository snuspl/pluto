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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.operators.BaseOperator;
import edu.snu.mist.task.operators.Operator;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class PartitionedQueryTest {
  // TODO[MIST-70]: Consider concurrency issue in execution of PartitionedQuery

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
    partitionedQuery.setOutputEmitter(output -> result.add((Integer) output));

    final Injector injector = Tang.Factory.getTang().newInjector();
    final StringIdentifierFactory idFactory = injector.getInstance(StringIdentifierFactory.class);
    final Identifier queryId = idFactory.getNewInstance("testQuery");
    final Identifier squareOpId = idFactory.getNewInstance("squareOp");
    final Identifier incOpId = idFactory.getNewInstance("incOp");
    final Identifier doubleOpId = idFactory.getNewInstance("doubleOp");

    final Operator<Integer, Integer> squareOp = new SquareOperator(queryId, squareOpId);
    final Operator<Integer, Integer> incOp = new IncrementOperator(queryId, incOpId);
    final Operator<Integer, Integer> doubleOp = new DoubleOperator(queryId, doubleOpId);

    // 2 * (input * input + 1)
    final Integer expected1 = 2 * (input * input + 1);
    partitionedQuery.insertToHead(doubleOp);
    partitionedQuery.insertToHead(incOp);
    partitionedQuery.insertToHead(squareOp);
    partitionedQuery.handle(input);
    Assert.assertEquals(expected1, result.remove(0));

    // 2 * (input + 1)
    partitionedQuery.removeFromHead();
    final Integer expected2 = 2 * (input + 1);
    partitionedQuery.handle(input);
    Assert.assertEquals(expected2, result.remove(0));

    // input + 1
    partitionedQuery.removeFromTail();
    final Integer expected3 = input + 1;
    partitionedQuery.handle(input);
    Assert.assertEquals(expected3, result.remove(0));

    // 2 * input + 1
    partitionedQuery.insertToHead(doubleOp);
    final Integer expected4 = 2 * input + 1;
    partitionedQuery.handle(input);
    Assert.assertEquals(expected4, result.remove(0));

    // (2 * input + 1) * (2 * input + 1)
    partitionedQuery.insertToTail(squareOp);
    final Integer expected5 = (2 * input + 1) * (2 * input + 1);
    partitionedQuery.handle(input);
    Assert.assertEquals(expected5, result.remove(0));
  }

  /**
   * This emits squared inputs.
   */
  class SquareOperator extends BaseOperator<Integer, Integer> {
    SquareOperator(final Identifier queryId,
                   final Identifier operatorId) {
      super(queryId, operatorId);
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input * input);
    }

    @Override
    public StreamType.OperatorType getOperatorType() {
      return StreamType.OperatorType.MAP;
    }
  }

  /**
   * This increments the input.
   */
  class IncrementOperator extends BaseOperator<Integer, Integer> {
    IncrementOperator(final Identifier queryId,
                      final Identifier operatorId) {
      super(queryId, operatorId);
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input + 1);
    }

    @Override
    public StreamType.OperatorType getOperatorType() {
      return StreamType.OperatorType.MAP;
    }
  }

  /**
   * This doubles the input.
   */
  class DoubleOperator extends BaseOperator<Integer, Integer> {
    DoubleOperator(final Identifier queryId,
                   final Identifier operatorId) {
      super(queryId, operatorId);
    }

    @Override
    public void handle(final Integer input) {
      outputEmitter.emit(input * 2);
    }

    @Override
    public StreamType.OperatorType getOperatorType() {
      return StreamType.OperatorType.MAP;
    }
  }
}
