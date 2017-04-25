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

import edu.snu.mist.common.stat.EWMA;
import edu.snu.mist.core.parameters.GlobalNumEventAlpha;
import edu.snu.mist.core.parameters.GlobalProcCpuUtilAlpha;
import edu.snu.mist.core.parameters.GlobalSysCpuUtilAlpha;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class which contains global metrics such as the number of events or cpu utilization.
 */
final class GlobalSchedMetric {

  /**
   * The number of all events inside the operator chain queues.
   */
  private long numEvents;

  /**
   * The exponential weighted moving average for number of events.
   */
  private EWMA ewmaNumEvents;

  /**
   * The cpu utilization of the whole system provided by a low-level system monitor.
   */
  private double systemCpuUtil;

  /**
   * The EWMA value of cpu utilization.
   */
  private EWMA ewmaSystemCpuUtil;

  /**
   * The cpu utilization of the JVM process provided by a low-level system monitor.
   */
  private double processCpuUtil;

  /**
   * The EWMA value of process cpu utilization.
   */
  private EWMA ewmaProcessCpuUtil;

  /**
   * The total weight of all groups.
   */
  private volatile long totalWeight;

  @Inject
  private GlobalSchedMetric(final GlobalSchedGroupInfoMap groupInfoMap,
                            @Parameter(GlobalNumEventAlpha.class) final double numEventAlpha,
                            @Parameter(GlobalSysCpuUtilAlpha.class) final double sysCpuUtilAlpha,
                            @Parameter(GlobalProcCpuUtilAlpha.class) final double procCpuUtilAlpha) {
    this.numEvents = 0;
    this.ewmaNumEvents = new EWMA(numEventAlpha);
    this.systemCpuUtil = 0.0;
    this.ewmaSystemCpuUtil = new EWMA(sysCpuUtilAlpha);
    this.processCpuUtil = 0.0;
    this.ewmaProcessCpuUtil = new EWMA(procCpuUtilAlpha);
    this.totalWeight = calculateTotalWeight(groupInfoMap);
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

  public double getSystemCpuUtil() {
    return systemCpuUtil;
  }

  public double getEwmaSystemCpuUtil() {
    return ewmaSystemCpuUtil.getCurrentEwma();
  }

  public void updateSystemCpuUtil(final double cpuUtil) {
    this.systemCpuUtil = cpuUtil;
    this.ewmaSystemCpuUtil.updateAndTick(cpuUtil);
  }

  public double getProcessCpuUtil() {
    return processCpuUtil;
  }

  public double getEwmaProcessCpuUtil() {
    return ewmaProcessCpuUtil.getCurrentEwma();
  }

  public void updateProcessCpuUtil(final double processCpuUtilParam) {
    this.processCpuUtil = processCpuUtilParam;
    this.ewmaProcessCpuUtil.updateAndTick(processCpuUtilParam);
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof GlobalSchedMetric)) {
      return false;
    }
    final GlobalSchedMetric groupMetric = (GlobalSchedMetric) o;
    return this.numEvents == groupMetric.getNumEvents()
        && this.systemCpuUtil == groupMetric.getSystemCpuUtil()
        && this.processCpuUtil == groupMetric.getProcessCpuUtil()
        && this.ewmaNumEvents.equals(groupMetric.getEwmaNumEvents())
        && this.ewmaProcessCpuUtil.equals(groupMetric.getEwmaProcessCpuUtil())
        && this.ewmaSystemCpuUtil.equals(groupMetric.getEwmaSystemCpuUtil());
  }

  @Override
  public int hashCode() {
    return ((Long) this.numEvents).hashCode() * 31 + ((Double) this.systemCpuUtil).hashCode() * 21 +
        ((Double) this.processCpuUtil).hashCode() * 11;
  }
}