/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task.globalsched.cfs;

import javax.inject.Inject;
import java.util.concurrent.TimeUnit;

/**
 * This is a start time of the mist task.
 * We need this time in order to calculate vruntime fairly.
 * If we do not use the same start time for group scheduling,
 * a group created later can have small vruntime compared to others.
 */
public final class MistStartTime {

  private final long startTime;
  private final long startTimeInMillis;

  @Inject
  private MistStartTime() {
    this.startTime = System.nanoTime();
    this.startTimeInMillis = TimeUnit.NANOSECONDS.toMillis(startTime);
  }

  public long getStartTimeInNano() {
    return startTime;
  }

  public long getStartTimeInMillis() {
    return startTimeInMillis;
  }
}
