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
package edu.snu.mist.core.task.groupAware.eventProcessors;


/**
 * This class is for the group isolator.
 */
public final class RuntimeProcessingInfo {

  /**
   * The currently processed group.
   */
  //private final SubGroup currGroup;

  /**
   * The start time of the group.
   */
  private final long startTime;

  /**
   * The number of processed events.
   */
  private final long numProcessedEvents;

  public RuntimeProcessingInfo(//final SubGroup currGroup,
                               final long startTime,
                               final long numProcessedEvents) {
    //this.currGroup = currGroup;
    this.startTime = startTime;
    this.numProcessedEvents = numProcessedEvents;
  }

  /*
  public SubGroup getCurrGroup() {
    return currGroup;
  }
  */

  public long getStartTime() {
    return startTime;
  }

  public long getNumProcessedEvents() {
    return numProcessedEvents;
  }
}
