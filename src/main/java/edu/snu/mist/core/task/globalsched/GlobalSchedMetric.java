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
package edu.snu.mist.core.task.globalsched;

import javax.inject.Inject;

/**
 * A class which contains global metrics such as the number of events or cpu utilization.
 */
public final class GlobalSchedMetric {

  /**
   * The number of all events inside the operator chain queues.
   */
  private long numEvents;

  /**
   * The cpu utilization of the whole system provided by a low-level system monitor.
   */
  private double systemCpuUtil;

  /**
   * The cpu utilization of the JVM process provided by a low-level system monitor.
   */
  private double processCpuUtil;

  /**
   * The total weight of all groups.
   */
  private volatile long totalWeight;

  /**
   * The number of groups.
   */
  private volatile int numGroups;

  @Inject
  private GlobalSchedMetric(final GlobalSchedGroupInfoMap groupInfoMap) {
    this.numEvents = 0;
    this.systemCpuUtil = 0;
    this.processCpuUtil = 0;
    this.totalWeight = calculateTotalWeight(groupInfoMap);
    this.numGroups = groupInfoMap.size();
  }

  /**
   * Get the number of groups.
   */
  public int getNumGroups() {
    return numGroups;
  }

  /**
   * Set the number of groups.
   * @param groups
   */
  public void setNumGroups(final int groups) {
    numGroups = groups;
  }

  /**
   * Calculate the total weight of all groups.
   * @param groupInfoMap group info map
   * @return total weight
   */
  private long calculateTotalWeight(final GlobalSchedGroupInfoMap groupInfoMap) {
    long sum = 0;
    for (final GlobalSchedGroupInfo groupInfo : groupInfoMap.values()) {
      sum += groupInfo.getWeight();
    }
    return sum;
  }

  /**
   * Get the total weight of all groups.
   * @return total weight
   */
  public long getTotalWeight() {
    return totalWeight;
  }

  /**
   * Set the total weight of all groups.
   * @param weight total weight
   */
  public void setTotalWeight(final long weight) {
    totalWeight = weight;
  }

  public void setNumEvents(final long numEventsToSet) {
    this.numEvents = numEventsToSet;
  }

  public long getNumEvents() {
    return numEvents;
  }

  public double getSystemCpuUtil() {
    return systemCpuUtil;
  }

  public void setSystemCpuUtil(final double cpuUtil) {
    this.systemCpuUtil = cpuUtil;
  }

  public double getProcessCpuUtil() {
    return processCpuUtil;
  }

  public void setProcessCpuUtil(final double processCpuUtil) {
    this.processCpuUtil = processCpuUtil;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GlobalSchedMetric)) {
      return false;
    }
    final GlobalSchedMetric groupMetric = (GlobalSchedMetric) o;
    return this.numEvents == groupMetric.getNumEvents() && systemCpuUtil == groupMetric.getSystemCpuUtil()
        && processCpuUtil == groupMetric.processCpuUtil;
  }

  @Override
  public int hashCode() {
    return ((Long) this.numEvents).hashCode() * 31 + ((Double) this.systemCpuUtil).hashCode() * 21 +
        ((Double) this.processCpuUtil).hashCode() * 11;
  }
}