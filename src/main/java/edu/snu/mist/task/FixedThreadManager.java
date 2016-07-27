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

import edu.snu.mist.task.parameters.NumThreads;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;

/**
 * This class manages a fixed number of threads.
 */
public final class FixedThreadManager implements ThreadManager {

  /**
   * A set of threads.
   */
  private final Set<Thread> threads;

  @Inject
  private FixedThreadManager(@Parameter(NumThreads.class) final int numThreads,
                             final PartitionedQueryManager queueManager) {
    this.threads = new HashSet<>();
    for (int i = 0; i < numThreads; i++) {
      final Thread thread = new Thread(new EventProcessor(queueManager));
      threads.add(thread);
      thread.start();
    }
  }

  @Override
  public Set<Thread> getThreads() {
    return threads;
  }

  @Override
  public void close() throws Exception {
    for (final Thread thread : threads) {
      thread.interrupt();
    }
  }
}
