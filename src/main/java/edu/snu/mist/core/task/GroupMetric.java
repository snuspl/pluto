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
package edu.snu.mist.core.task;

import edu.snu.mist.common.stat.EWMA;
import edu.snu.mist.core.parameters.GroupNumEventAlpha;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class which contains group metrics such as the number of queries or events.
 */
final class GroupMetric {

  /**
   * The number of all events inside the group operator chain queues.
   */
  private long numEvents;

  /**
   * EWMA of number of events.
   */
  private EWMA ewmaNumEvents;

  @Inject
  private GroupMetric(@Parameter(GroupNumEventAlpha.class) final double numEventAlpha) {
    this.numEvents = 0;
    this.ewmaNumEvents = new EWMA(numEventAlpha);
  }

  public void updateNumEvents(final long numEventsParam) {
    this.numEvents = numEventsParam;
    this.ewmaNumEvents.updateAndTick(numEventsParam);
  }

  public long getNumEvents() {
    return numEvents;
  }

  public double getEwmaNumEvents() {
    return ewmaNumEvents.getCurrentEwma();
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GroupMetric)) {
      return false;
    }
    final GroupMetric groupMetric = (GroupMetric) o;
    return this.numEvents == groupMetric.getNumEvents()
        && this.ewmaNumEvents.equals(groupMetric.getEwmaNumEvents());
  }

  @Override
  public int hashCode() {
    return ((Long) this.numEvents).hashCode() * 32;
  }
}