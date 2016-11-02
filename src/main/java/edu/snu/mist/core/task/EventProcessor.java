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
package edu.snu.mist.core.task;

/**
 * This class processes events of queries
 * by picking up one query from PartitionedQueryManager.
 */
public final class EventProcessor implements Runnable {

  /**
   * A partitioned query manager for picking up a query for event processing.
   */
  private final PartitionedQueryManager queryManager;

  public EventProcessor(final PartitionedQueryManager queryManager) {
    this.queryManager = queryManager;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        final PartitionedQuery query = queryManager.pickQuery();
        if (query != null) {
          query.processNextEvent();
        }
      } catch (final Exception t) {
        throw t;
      }
    }
  }
}
