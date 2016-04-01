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
package edu.snu.mist.task.queues;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class PartitionedQueryQueueTest {

  /**
   * Test whether the partitioned query queue adds/polls tasks correctly.
   * @throws InterruptedException
   */
  @Test
  public void testPartitionedQueryQueue() throws InterruptedException {
    final PartitionedQueryQueue queue = new DefaultPartitionedQueryQueue();
    final List<String> result = new LinkedList<>();
    queue.add(() -> result.add("1"));
    queue.add(() -> result.add("2"));
    queue.poll().run();
    Assert.assertEquals(Arrays.asList("1"), result);
    queue.poll().run();
    Assert.assertEquals(Arrays.asList("1", "2"), result);
  }

  /**
   * Test whether the queue guarantees sequential processing of tasks in a queue.
   * @throws InterruptedException
   */
  @Test
  public void testConcurrentAccessPartitionedQueryQueue() throws InterruptedException {
    final PartitionedQueryQueue queue = new DefaultPartitionedQueryQueue();
    final List<String> result = new LinkedList<>();
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    queue.add(() -> {
      try {
        countDownLatch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      result.add("1");
    });
    queue.add(() -> result.add("2"));

    // Another thread polls a task.
    // Then, the partitionedQueryQueue should block next poll() until the first polled task is finished
    // in order to meet sequential processing.
    final Runnable task = queue.poll();
    final Thread thread = new Thread(() -> task.run());
    thread.start();

    // This should return null
    Assert.assertEquals(null, queue.poll());
    countDownLatch.countDown();

    Runnable task2;
    while ((task2 = queue.poll()) == null) {
      // do while until queue.poll() returns a task
    }
    task2.run();
    Assert.assertEquals(Arrays.asList("1", "2"), result);
  }
}
