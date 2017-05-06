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

import edu.snu.mist.common.stats.EWMA;
import edu.snu.mist.core.parameters.HeapMemoryUsageAlpha;
import edu.snu.mist.core.parameters.NonHeapMemoryUsageAlpha;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class which contains metric of memory usage.
 */
public final class MemoryUsageMetric {

  /**
   * The heap memory usage which will be calculated as used memory / total memory.
   */
  private volatile double heapMemoryUsage;

  /**
   * EWMA of heap memory usage.
   */
  private final EWMA ewmaHeapMemoryUsage;

  /**
   * The non heap memory usage which will be calculated as used memory / total memory.
   * If one of non heap memory pools has undefined max size, this value will be 0.
   */
  private volatile double nonHeapMemoryUsage;

  /**
   * EWMA of non heap memory usage.
   */
  private final EWMA ewmaNonHeapMemoryUsage;

  @Inject
  private MemoryUsageMetric(@Parameter(HeapMemoryUsageAlpha.class) final double heapMemoryUsageAlpha,
                            @Parameter(NonHeapMemoryUsageAlpha.class) final double nonHeapMemoryUsageAlpha) {
    this.heapMemoryUsage = 0.0;
    this.nonHeapMemoryUsage = 0.0;
    this.ewmaHeapMemoryUsage = new EWMA(heapMemoryUsageAlpha);
    this.ewmaNonHeapMemoryUsage = new EWMA(nonHeapMemoryUsageAlpha);
  }

  public void updateHeapMemoryUsage(final double heapMemUsage) {
    this.heapMemoryUsage = heapMemUsage;
    this.ewmaHeapMemoryUsage.updateAndTick(heapMemUsage);
  }

  public void updateNonHeapMemoryUsage(final double nonHeapMemUsage) {
    this.nonHeapMemoryUsage = nonHeapMemUsage;
    this.ewmaNonHeapMemoryUsage.updateAndTick(nonHeapMemUsage);
  }

  public double getHeapMemoryUsage() {
    return heapMemoryUsage;
  }

  public double getEwmaHeapMemoryUsage() {
    return ewmaHeapMemoryUsage.getCurrentEwma();
  }

  public double getNonHeapMemoryUsage() {
    return nonHeapMemoryUsage;
  }

  public double getEwmaNonHeapMemoryUsage() {
    return ewmaNonHeapMemoryUsage.getCurrentEwma();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MemoryUsageMetric that = (MemoryUsageMetric) o;

    if (Double.compare(that.getHeapMemoryUsage(), getHeapMemoryUsage()) != 0) {
      return false;
    }
    if (Double.compare(that.getNonHeapMemoryUsage(), getNonHeapMemoryUsage()) != 0) {
      return false;
    }
    if (!((Double) getEwmaHeapMemoryUsage()).equals(that.getEwmaHeapMemoryUsage())) {
      return false;
    }
    return ((Double) getEwmaNonHeapMemoryUsage()).equals(that.getEwmaNonHeapMemoryUsage());
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(getHeapMemoryUsage());
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + ((Double) getEwmaHeapMemoryUsage()).hashCode();
    temp = Double.doubleToLongBits(getNonHeapMemoryUsage());
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    result = 31 * result + ((Double) getEwmaNonHeapMemoryUsage()).hashCode();
    return result;
  }
}