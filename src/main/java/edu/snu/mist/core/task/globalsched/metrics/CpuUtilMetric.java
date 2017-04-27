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
import edu.snu.mist.core.parameters.GlobalProcCpuUtilAlpha;
import edu.snu.mist.core.parameters.GlobalSysCpuUtilAlpha;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class which contains metrics of cpu utilization.
 */
public final class CpuUtilMetric {

  /**
   * The cpu utilization of the whole system provided by a low-level system monitor.
   */
  private volatile double systemCpuUtil;

  /**
   * The EWMA value of cpu utilization.
   */
  private final EWMA ewmaSystemCpuUtil;

  /**
   * The cpu utilization of the JVM process provided by a low-level system monitor.
   */
  private volatile double processCpuUtil;

  /**
   * The EWMA value of process cpu utilization.
   */
  private final EWMA ewmaProcessCpuUtil;


  @Inject
  private CpuUtilMetric(@Parameter(GlobalSysCpuUtilAlpha.class) final double sysCpuUtilAlpha,
                        @Parameter(GlobalProcCpuUtilAlpha.class) final double procCpuUtilAlpha) {
    this.systemCpuUtil = 0.0;
    this.processCpuUtil = 0.0;
    this.ewmaSystemCpuUtil = new EWMA(sysCpuUtilAlpha);
    this.ewmaProcessCpuUtil = new EWMA(procCpuUtilAlpha);
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
    if (!(o instanceof CpuUtilMetric)) {
      return false;
    }
    final CpuUtilMetric cpuUtilMetric = (CpuUtilMetric) o;
    return this.systemCpuUtil == cpuUtilMetric.getSystemCpuUtil() &&
        this.processCpuUtil == cpuUtilMetric.getProcessCpuUtil() &&
        this.ewmaProcessCpuUtil.equals(cpuUtilMetric.getEwmaProcessCpuUtil()) &&
        this.ewmaSystemCpuUtil.equals(cpuUtilMetric.getEwmaSystemCpuUtil());
  }

  @Override
  public int hashCode() {
    return ((Double) this.systemCpuUtil).hashCode() * 31 + ((Double) this.processCpuUtil).hashCode();
  }
}