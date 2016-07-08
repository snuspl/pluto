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
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

public class QueueManagerTest {

  /**
   * Test whether RandomlyPickQueueManager selects a queue.
   */
  @Test
  public void randomPickQueueManagerTest() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final RandomlyPickQueueManager queueManager = injector.getInstance(RandomlyPickQueueManager.class);

    // Select a queue
    final PartitionedQueryQueue queue = new DefaultPartitionedQueryQueue();
    queueManager.insert(queue);
    final PartitionedQueryQueue selectedQueue = queueManager.pickQueue();
    Assert.assertEquals(queue, selectedQueue);

    // When QueueManager has no queue, it should returns null
    queueManager.delete(queue);
    final PartitionedQueryQueue q = queueManager.pickQueue();
    Assert.assertEquals(null, q);
  }
}
