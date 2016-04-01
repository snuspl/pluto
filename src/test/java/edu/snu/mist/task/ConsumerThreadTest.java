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

import edu.snu.mist.task.queues.DefaultPartitionedQueryQueue;
import edu.snu.mist.task.queues.PartitionedQueryQueue;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class ConsumerThreadTest {

  /**
   * Test whether the consumer thread consumes tasks from shared queues correctly.
   *
   */
  @Test
  public void randomPickConsumerTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final PartitionedQueryQueueManager queueManager = injector.getInstance(PartitionedQueryQueueManager.class);

    final PartitionedQueryQueue queue1 = new DefaultPartitionedQueryQueue();
    final PartitionedQueryQueue queue2 = new DefaultPartitionedQueryQueue();

    final int numTasks = 100;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> list2 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add tasks to queues
      queue1.add(() -> list1.add(data));
      queue2.add(() -> list2.add(data));
      result.add(data);
    }

    // Add queues to queueManager
    queueManager.insert(queue1);
    queueManager.insert(queue2);

    // Create a consumer thread
    final Thread consumerThread = new Thread(new ConsumerThread(queueManager));
    consumerThread.start();

    while (!(list1.size() == numTasks && list2.size() == numTasks)) {
      // do nothing until consumer thread consumes all of the tasks
    }
    Assert.assertEquals(result, list1);
    Assert.assertEquals(result, list2);
    consumerThread.interrupt();
  }
}
