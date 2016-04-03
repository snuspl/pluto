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

import edu.snu.mist.task.operators.window.WindowOperator;
import edu.snu.mist.task.parameters.NumScheduledExecutorThreads;
import edu.snu.mist.task.parameters.TimeWindowPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class gives time notification to WindowOperators every period (default is 1 second).
 * It gives System.currentTimeMillis() as a notification.
 */
public final class TimeWindowNotifier implements WindowNotifier {

  /**
   * Scheduled executor for generating time notifications.
   */
  private final ScheduledExecutorService executor;

  /**
   * Subscribers of notifications.
   */
  private final ConcurrentLinkedQueue<WindowOperator> queue;

  @Inject
  private TimeWindowNotifier(@Parameter(NumScheduledExecutorThreads.class) final int numThreads,
                             @Parameter(TimeWindowPeriod.class) final int period) {
    this.executor = Executors.newScheduledThreadPool(numThreads);
    this.queue = new ConcurrentLinkedQueue<>();

    // [MIST-#] Parallelize sending notifications for better performance.
    // If the number of windowing operators increases, sending notification could be a bottleneck.
    // We should improve it.
    this.executor.scheduleAtFixedRate(() -> {
      final long currTime = System.currentTimeMillis();
      for (final WindowOperator op : queue) {
        // give notification to a windowing operator.
        op.windowNotification(currTime);
      }
    }, period, period, TimeUnit.SECONDS);
  }

  @Override
  public void registerWindowOperator(final WindowOperator operator) {
    queue.add(operator);
  }

  @Override
  public void unregisterWindowOperator(final WindowOperator operator) {
    queue.remove(operator);
  }

  @Override
  public void close() throws Exception {
    executor.shutdown();
  }
}
