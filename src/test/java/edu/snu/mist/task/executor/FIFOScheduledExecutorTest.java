/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.executor;

import com.google.common.collect.ImmutableList;
import edu.snu.mist.task.executor.impl.DefaultExecutorTask;
import edu.snu.mist.task.executor.impl.FIFOScheduler;
import edu.snu.mist.task.operator.BaseOperator;
import edu.snu.mist.task.operator.Operator;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

/**
 * This test submits multiple `ExecutorTask`s to MistExecutor with FIFO scheduler
 * and checks the executor tasks are executed in FIFO order.
 */
public final class FIFOScheduledExecutorTest {

  /**
   * A mist Executor.
   */
  private final MistExecutor executor;

  /**
   * A list storing outputs of executor tasks.
   */
  private final List<Integer> outputs = new LinkedList<>();

  /**
   * An identifier factory.
   */
  private final StringIdentifierFactory idfac;

  public FIFOScheduledExecutorTest() throws InjectionException {
    this.idfac = new StringIdentifierFactory();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Identifier id = idfac.getNewInstance("executor");

    jcb.bindImplementation(ExecutorTaskScheduler.class, FIFOScheduler.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(Identifier.class, id);

    this.executor = injector.getInstance(MistExecutor.class);
  }

  /**
   * Submits 100 `ExecutorTask`s and checks the tasks are executed in FIFO order.
   * First `ExecutorTask` executes `WhileLoopOperator`,
   * in order to see the next 99 tasks are scheduled in FIFO order.
   * @throws Exception
   */
  @Test
  public void fifoScheduledExecutorTest() throws Exception {
    final int numTasks = 100;
    final Operator<Integer, Integer> whileLoopOp =
        new WhileLoopOperator(idfac.getNewInstance("while-loop-op"));
    final Operator<Integer, Integer> noOp = new NoOperator(idfac.getNewInstance("no-op"));

    executor.onNext(new DefaultExecutorTask<>(whileLoopOp, ImmutableList.of(0)));
    for (int i = 1; i < numTasks; i++) {
      executor.onNext(new DefaultExecutorTask<>(noOp, ImmutableList.of(i)));
    }

    Thread.sleep(2500);
    executor.close();
    for (int i = 0; i < numTasks; i++) {
      Assert.assertEquals(Integer.valueOf(i), outputs.get(i));
    }
  }

  /**
   * This operator do `while loop` during two seconds
   * and pushes inputs to outputs list.
   */
  class WhileLoopOperator extends BaseOperator<Integer, Integer> {

    public WhileLoopOperator(final Identifier id) {
      super(id);
    }

    @Override
    public void onNext(final List<Integer> inputs) {
      final long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 2000) {
        // empty
      }
      outputs.add(inputs.get(0));
      System.out.println(identifier + ", input: " + inputs.get(0));
    }
  }

  /**
   * This operator just adds inputs to outputs list.
   */
  class NoOperator extends BaseOperator<Integer, Integer> {
    public NoOperator(final Identifier id) {
      super(id);
    }

    @Override
    public void onNext(final List<Integer> inputs) {
      outputs.add(inputs.get(0));
      System.out.println(identifier + ", input: " + inputs.get(0));
    }
  }
}
