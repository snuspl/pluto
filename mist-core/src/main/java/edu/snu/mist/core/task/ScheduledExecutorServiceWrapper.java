/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.core.parameters.NumPeriodicSchedulerThreads;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This is the wrapper class for injecting ScheduledExecutorService.
 */
public final class ScheduledExecutorServiceWrapper {
  private final ScheduledExecutorService scheduler;

  @Inject
  private ScheduledExecutorServiceWrapper(@Parameter(NumPeriodicSchedulerThreads.class) final int numThreads) {
    scheduler = Executors.newScheduledThreadPool(numThreads);
  }

  public ScheduledExecutorService getScheduler() {
    return scheduler;
  }
}
