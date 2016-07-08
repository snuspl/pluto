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

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Queue;

/**
 * This is an interface of a queue of a query.
 */
@DefaultImplementation(DefaultPartitionedQueryQueue.class)
public interface PartitionedQueryQueue extends Queue<Runnable> {

  /**
   * Return a task from the queue.
   * It returns null if 1) it is empty or
   * 2) previously polled task is not finished in order to guarantee sequential processing.
   * @return a task
   */
  @Override
  Runnable poll();

  /**
   * Get polling rate how many tasks are actually polled.
   * @return polling rate
   */
  double pollingRate();
}