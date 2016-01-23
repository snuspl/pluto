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
package edu.snu.mist.task.executor;

import edu.snu.mist.task.OperatorChainJob;
import edu.snu.mist.task.executor.queues.FIFOQueue;
import edu.snu.mist.task.executor.queues.SchedulingQueue;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class ExecutorJobSchedulingTest {

  /**
   * A list storing outputs of OperatorChainJobs.
   */
  private List<Integer> outputs;

  @Before
  public void init() {
    outputs = new LinkedList<>();
  }

  /**
   * This test submits multiple `OperatorChainJob`s to MistExecutor with FIFO queue
   * and checks the jobs are executed in FIFO order.
   *
   * Submits 100 `OperatorChainJob`s and checks the jobs are executed in FIFO order.
   * First `OperatorChainJob` executes while loop during 2 seconds,
   * to delay next 99 tasks and see the delayed tasks are scheduled in FIFO order.
   * @throws Exception
   */
  @Test
  public void fifoScheduledExecutorTest() throws Exception {
    final int numTasks = 100;
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(SchedulingQueue.class, FIFOQueue.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final MistExecutor executor = injector.getInstance(MistExecutor.class);
    final OperatorChainJob firstJob = () -> {
      final long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() - startTime < 2000) {
        // empty
      }
      outputs.add(0);
      System.out.println("while-op");
    };

    executor.submit(firstJob);
    for (int i = 1; i < numTasks; i++) {
      final int input = i;
      final OperatorChainJob simpleJob = () -> {
        outputs.add(input);
        System.out.println("simple-op-" + input);
      };
      executor.submit(simpleJob);
    }

    Thread.sleep(2500);
    executor.close();
    for (int i = 0; i < numTasks; i++) {
      Assert.assertEquals(Integer.valueOf(i), outputs.get(i));
    }
  }
}
