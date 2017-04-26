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
package edu.snu.mist.core.task.globalsched.metrics;

import edu.snu.mist.common.stats.EWMA;
import edu.snu.mist.core.parameters.DefaultGroupWeight;
import edu.snu.mist.core.parameters.GlobalNumEventAlpha;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class which contains metric of the number of events.
 */
public final class EventNumAndWeightMetric {

  /**
   * The number of all events inside the operator chain queues.
   */
  private volatile long numEvents;

  /**
   * The exponential weighted moving average for number of events.
   */
  private final EWMA ewmaNumEvents;

  /**
   * The weight used for scheduling.
   */
  private volatile int weight;

  @Inject
  private EventNumAndWeightMetric(@Parameter(DefaultGroupWeight.class) final int weight,
                                  @Parameter(GlobalNumEventAlpha.class) final double numEventAlpha) {
    this.numEvents = 0;
    this.ewmaNumEvents = new EWMA(numEventAlpha);
    this.weight = weight;
  }

  public void setNumEvents(final long numEventsParam) {
    this.numEvents = numEventsParam;
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

  public int getWeight() {
    return weight;
  }

  public void setWeight(final int weight) {
    this.weight = weight;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof EventNumAndWeightMetric)) {
      return false;
    }
    final EventNumAndWeightMetric eventNumMetric = (EventNumAndWeightMetric) o;
    return this.numEvents == eventNumMetric.getNumEvents() &&
        this.ewmaNumEvents.equals(eventNumMetric.getEwmaNumEvents()) &&
        this.weight == eventNumMetric.getWeight();
  }

  @Override
  public int hashCode() {
    return ((Long) this.numEvents).hashCode() * 32 + ((Integer) this.weight).hashCode() * 22;
  }
}