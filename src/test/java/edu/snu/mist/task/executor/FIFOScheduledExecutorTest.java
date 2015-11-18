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
 * This test submits multiple executor tasks to MistExecutor with FIFO scheduler
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
   * Submits 100 executor tasks and checks the tasks are executed in FIFO order.
   * First executor task is executing while loop during 2 seconds,
   * in order to see the remaining 99 tasks are scheduled in FIFO order.
   * @throws Exception
   */
  @Test
  public void fifoScheduledExecutorTest() throws Exception {
    final int numTasks = 100;
    for (int i = 0; i < numTasks; i++) {
      executor.onNext(
          new DefaultExecutorTask<>(
              new TestOperator(idfac.getNewInstance("operator-" + i), i == 0),
              ImmutableList.of(i)));
    }

    Thread.sleep(2500);
    executor.close();
    for (int i = 0; i < numTasks; i++) {
      Assert.assertEquals(Integer.valueOf(i), outputs.get(i));
    }
  }

  /**
   * Test operator.
   */
  class TestOperator extends BaseOperator<Integer, Integer> {
    private boolean first;

    public TestOperator(final Identifier id, final boolean first) {
      super(id);
      this.first = first;
    }

    @Override
    public void onNext(final List<Integer> inputs) {
      if (first) {
        final long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 2000) {
          // empty
        }
      }
      outputs.add(inputs.get(0));
      System.out.println(identifier);
    }
  }
}
