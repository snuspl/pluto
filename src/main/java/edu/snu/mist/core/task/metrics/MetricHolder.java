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
package edu.snu.mist.core.task.metrics;

import javax.inject.Inject;

/**
 * A class represents a metric holder.
 * If this holder is placed in a group info, the metrics in this holder will represent the status of the group.
 * Else if this holder is placed in a query manager, the metrics in this holder will represent the global status.
 */
public final class MetricHolder {

  /**
   * The number of events metric with EWMA.
   */
  private EWMAMetric numEventsMetric;

  /**
   * The system CPU utilization metric with EWMA.
   */
  private EWMAMetric cpuSysUtilMetric;

  /**
   * The process CPU utilization metric with EWMA.
   */
  private EWMAMetric cpuProcUtilMetric;

  /**
   * The heap memory usage metric with EWMA.
   */
  private EWMAMetric heapMemUsageMetric;

  /**
   * The non heap memory usage metric with EWMA.
   */
  private EWMAMetric nonHeapMemUsageMetric;

  /**
   * The weight metric.
   */
  private NormalMetric<Double> weightMetric;

  /**
   * The number of groups metric.
   */
  private NormalMetric<Integer> numGroupsMetric;

  @Inject
  private MetricHolder() {
    this.numEventsMetric = null;
    this.cpuSysUtilMetric = null;
    this.cpuProcUtilMetric = null;
    this.heapMemUsageMetric = null;
    this.nonHeapMemUsageMetric = null;
    this.weightMetric = null;
    this.numGroupsMetric = null;
  }

  /**
   * Initialize the number of events metric.
   * @param noe the number of events metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeNumEvents(final EWMAMetric noe) throws RuntimeException {
    if (this.numEventsMetric == null) {
      this.numEventsMetric = noe;
    } else {
      throw new RuntimeException("The number of events metric was set already.");
    }
  }

  /**
   * Initialize the CPU system utilization metric.
   * @param csu the CPU system utilization metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeCpuSysUtil(final EWMAMetric csu) throws RuntimeException {
    if (this.cpuSysUtilMetric == null) {
      this.cpuSysUtilMetric = csu;
    } else {
      throw new RuntimeException("The CPU system utilization metric was set already.");
    }
  }

  /**
   * Initialize the CPU process utilization metric.
   * @param cpu the CPU process utilization metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeCpuProcUtil(final EWMAMetric cpu) throws RuntimeException {
    if (this.cpuProcUtilMetric == null) {
      this.cpuProcUtilMetric = cpu;
    } else {
      throw new RuntimeException("The CPU process utilization metric was set already.");
    }
  }

  /**
   * Initialize the heap memory usage metric.
   * @param hmu the heap memory usage metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeHeapMemUsage(final EWMAMetric hmu) throws RuntimeException {
    if (this.heapMemUsageMetric == null) {
      this.heapMemUsageMetric = hmu;
    } else {
      throw new RuntimeException("The heap memory usage metric was set already.");
    }
  }

  /**
   * Initialize the non heap memory usage metric.
   * @param nhmu the non heap memory usage metric to set.
   * @throws RuntimeException
   */
  public void initializeNonHeapMemUsage(final EWMAMetric nhmu) throws RuntimeException {
    if (this.nonHeapMemUsageMetric == null) {
      this.nonHeapMemUsageMetric = nhmu;
    } else {
      throw new RuntimeException("The non heap memory usage metric was set already.");
    }
  }

  /**
   * Initialize the weight metric.
   * @param weight the weight metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeWeight(final NormalMetric weight) throws RuntimeException {
    if (this.weightMetric == null) {
      this.weightMetric = weight;
    } else {
      throw new RuntimeException("The weight metric was set already.");
    }
  }

  /**
   * Initialize the number of groups metric.
   * @param nog the number of groups metric to set
   * @throws RuntimeException if the metric was set already
   */
  public void initializeNumGroups(final NormalMetric nog) throws RuntimeException {
    if (this.numGroupsMetric == null) {
      this.numGroupsMetric = nog;
    } else {
      throw new RuntimeException("The number of groups metric was set already.");
    }
  }

  /**
   * @return the number of events metric
   * @throws RuntimeException if the metric was not set yet
   */
  public EWMAMetric getNumEventsMetric() throws RuntimeException {
    if (numEventsMetric != null) {
      return numEventsMetric;
    } else {
      throw new RuntimeException("The number of events metric was not set yet.");
    }
  }

  /**
   * @return the CPU system utilization metric
   * @throws RuntimeException if the metric was not set yet
   */
  public EWMAMetric getCpuSysUtilMetric() throws RuntimeException {
    if (cpuSysUtilMetric != null) {
      return cpuSysUtilMetric;
    } else {
      throw new RuntimeException("The CPU system utilization metric was not set yet.");
    }
  }

  /**
   * @return the CPU process utilization metric
   * @throws RuntimeException if the metric was not set yet
   */
  public EWMAMetric getCpuProcUtilMetric() throws RuntimeException {
    if (cpuProcUtilMetric != null) {
      return cpuProcUtilMetric;
    } else {
      throw new RuntimeException("The CPU process utilization metric was not set yet.");
    }
  }

  /**
   * @return the heap memory usage metric
   * @throws RuntimeException if the metric was not set yet
   */
  public EWMAMetric getHeapMemUsageMetric() throws RuntimeException {
    if (heapMemUsageMetric != null) {
      return heapMemUsageMetric;
    } else {
      throw new RuntimeException("The heap memory usage metric was not set yet.");
    }
  }

  /**
   * @return the non heap memory usage metric
   * @throws RuntimeException if the metric was not set yet
   */
  public EWMAMetric getNonHeapMemUsageMetric() throws RuntimeException {
    if (nonHeapMemUsageMetric != null) {
      return nonHeapMemUsageMetric;
    } else {
      throw new RuntimeException("The non heap memory usage metric was not set yet.");
    }
  }

  /**
   * @return the weight metric
   * @throws RuntimeException if the metric was not set yet
   */
  public NormalMetric<Double> getWeightMetric() throws RuntimeException {
    if (weightMetric != null) {
      return weightMetric;
    } else {
      throw new RuntimeException("The weight metric was not set yet.");
    }
  }

  /**
   * @return the number of groups metric
   * @throws RuntimeException if the metric was not set yet
   */
  public NormalMetric<Integer> getNumGroupsMetric() throws RuntimeException {
    if (numGroupsMetric != null) {
      return numGroupsMetric;
    } else {
      throw new RuntimeException("The number of groups metric was not set yet.");
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final MetricHolder that = (MetricHolder) o;

    if (numEventsMetric != null ? !numEventsMetric.equals(that.numEventsMetric) : that.numEventsMetric != null) {
      return false;
    }
    if (cpuSysUtilMetric != null ? !cpuSysUtilMetric.equals(that.cpuSysUtilMetric) : that.cpuSysUtilMetric != null) {
      return false;
    }
    if (cpuProcUtilMetric != null ? !cpuProcUtilMetric.equals(that.cpuProcUtilMetric) :
        that.cpuProcUtilMetric != null) {
      return false;
    }
    if (heapMemUsageMetric != null ? !heapMemUsageMetric.equals(that.heapMemUsageMetric) :
        that.heapMemUsageMetric != null) {
      return false;
    }
    if (nonHeapMemUsageMetric != null ? !nonHeapMemUsageMetric.equals(that.nonHeapMemUsageMetric) :
        that.nonHeapMemUsageMetric != null) {
      return false;
    }
    if (weightMetric != null ? !weightMetric.equals(that.weightMetric) : that.weightMetric != null) {
      return false;
    }
    return numGroupsMetric != null ? numGroupsMetric.equals(that.numGroupsMetric) : that.numGroupsMetric == null;
  }

  @Override
  public int hashCode() {
    int result = numEventsMetric != null ? numEventsMetric.hashCode() : 0;
    result = 31 * result + (cpuSysUtilMetric != null ? cpuSysUtilMetric.hashCode() : 0);
    result = 31 * result + (cpuProcUtilMetric != null ? cpuProcUtilMetric.hashCode() : 0);
    result = 31 * result + (heapMemUsageMetric != null ? heapMemUsageMetric.hashCode() : 0);
    result = 31 * result + (nonHeapMemUsageMetric != null ? nonHeapMemUsageMetric.hashCode() : 0);
    result = 31 * result + (weightMetric != null ? weightMetric.hashCode() : 0);
    result = 31 * result + (numGroupsMetric != null ? numGroupsMetric.hashCode() : 0);
    return result;
  }
}