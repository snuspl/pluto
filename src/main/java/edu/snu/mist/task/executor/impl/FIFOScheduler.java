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
package edu.snu.mist.task.executor.impl;

import edu.snu.mist.task.executor.ExecutorTaskScheduler;

import javax.inject.Inject;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A simple FIFO scheduler which inherits linked blocking queue.
 */
public final class FIFOScheduler extends LinkedBlockingQueue<Runnable> implements ExecutorTaskScheduler {

  @Inject
  private FIFOScheduler() {
    super();
  }

  /**
   * Returns the size of queue.
   * @return
   */
  @Override
  public int getCurrentLoad() {
    return size();
  }
}
