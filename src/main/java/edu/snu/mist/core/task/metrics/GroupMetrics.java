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

import edu.snu.mist.core.task.metrics.parameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A class represents a group metric holder.
 * The metrics in this holder will represent the group status.
 */
public final class GroupMetrics {

  /**
   * The number of events metric with EWMA.
   */
  private final EWMAMetric numEventsMetric;

  /**
   * The weight metric.
   */
  private final NormalMetric<Double> weightMetric;

  @Inject
  private GroupMetrics(@Parameter(NumEventAlpha.class) final double numEventAlpha) {
    this.numEventsMetric = new EWMAMetric(0.0, numEventAlpha);
    this.weightMetric = new NormalMetric<>(1.0);
  }

  /**
   * @return the number of events metric
   */
  public EWMAMetric getNumEventsMetric() throws RuntimeException {
    return numEventsMetric;
  }

  /**
   * @return the weight metric
   */
  public NormalMetric<Double> getWeightMetric() throws RuntimeException {
    return weightMetric;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final GroupMetrics that = (GroupMetrics) o;

    if (!getNumEventsMetric().equals(that.getNumEventsMetric())) {
      return false;
    }
    return getWeightMetric().equals(that.getWeightMetric());
  }

  @Override
  public int hashCode() {
    int result = getNumEventsMetric().hashCode();
    result = 31 * result + getWeightMetric().hashCode();
    return result;
  }
}