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

import edu.snu.mist.task.queues.PartitionedQueryQueue;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface manages queues of partitioned queries.
 */
@DefaultImplementation(RandomlyPickQueueManager.class)
public interface PartitionedQueryQueueManager {

  /**
   * Insert a partitioned query queue.
   * @param queue partitioned query queue
   */
  void insert(PartitionedQueryQueue queue);

  /**
   * Delete a partitioned query queue.
   * @param queue partitioned query queue
   */
  void delete(PartitionedQueryQueue queue);

  /**
   * Pick a partitioned query queue.
   * @return a partitioned query queue.
   * Returns null if there is no queue.
   */
  PartitionedQueryQueue pickQueue();
}
