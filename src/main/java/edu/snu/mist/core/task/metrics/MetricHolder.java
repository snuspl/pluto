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
import java.util.HashMap;
import java.util.Map;

/**
 * A class represents a metric holder.
 * If this holder is placed in a group info, the metrics in this holder will represent the status of the group.
 * Else if this holder is placed in a query manager, the metrics in this holder will represent the global status.
 */
public final class MetricHolder {

  /**
   * The type of metrics with EWMA.
   */
  public enum EWMAMetricType {
    NUM_EVENTS, CPU_SYS_UTIL, CPU_PROC_UTIL, HEAP_MEM_USAGE, NON_HEAP_MEM_USAGE
  };

  /**
   * The type of metrics without EWMA.
   */
  public enum NormalMetricType {
    NUM_GROUP, WEIGHT
  };

  /**
   * The map between metric types and metrics with EWMA.
   */
  private final Map<EWMAMetricType, EWMAMetric> ewmaMetricMap;

  /**
   * The map between metric types and metrics without EWMA.
   */
  private final Map<NormalMetricType, NormalMetric> normalMetricMap;

  @Inject
  private MetricHolder() {
    this.ewmaMetricMap = new HashMap<>();
    this.normalMetricMap = new HashMap<>();
  }

  /**
   * Put a new metric with EWMA to this holder.
   * @param ewmaMetricType the type of new metric
   * @param metric the new metric to put
   * @return whether the putting process was successful
   */
  public boolean putEWMAMetric(final EWMAMetricType ewmaMetricType, final EWMAMetric metric) {
    return ewmaMetricMap.putIfAbsent(ewmaMetricType, metric) == null;
  }

  /**
   * Put a new metric without EWMA to this holder.
   * @param normalMetricType the type of new metric
   * @param metric the new metric to put
   * @return whether the putting process was successful
   */
  public boolean putNormalMetric(final NormalMetricType normalMetricType, final NormalMetric metric) {
    return normalMetricMap.putIfAbsent(normalMetricType, metric) == null;
  }

  /**
   * Get the ewma metric.
   * It can return a null if there is not any metric having the required type.
   * @param ewmaMetricType the type of metric to get
   * @return the required metric
   */
  public EWMAMetric getEWMAMetric(final EWMAMetricType ewmaMetricType) {
    return ewmaMetricMap.get(ewmaMetricType);
  }

  /**
   * Get the metric without ewma.
   * It can return a null if there is not any metric having the required type.
   * @param normalMetricType the type of metric to get
   * @return the required metric
   */
  public NormalMetric getNormalMetric(final NormalMetricType normalMetricType) {
    return normalMetricMap.get(normalMetricType);
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

    if (!ewmaMetricMap.equals(that.ewmaMetricMap)) {
      return false;
    }
    return normalMetricMap.equals(that.normalMetricMap);
  }

  @Override
  public int hashCode() {
    int result = ewmaMetricMap.hashCode();
    result = 31 * result + normalMetricMap.hashCode();
    return result;
  }
}