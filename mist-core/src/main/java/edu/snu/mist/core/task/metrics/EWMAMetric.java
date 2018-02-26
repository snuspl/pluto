/*
 * Copyright (C) 2018 Seoul National University
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

/**
 * A class represents a metric with EWMA such as CPU or memory utilization used for system management.
 */
public final class EWMAMetric {

  /**
   * The value of metric.
   */
  private volatile double value;

  /**
   * The EWMA of the metric.
   */
  private final EWMA ewmaValue;

  /**
   * Constructor of EWMAMetric.
   * @param defaultValue the default value of this metric.
   * @param alpha the decaying rate used in EWMA.
   */
  public EWMAMetric(final double defaultValue,
                    final double alpha) {
    this.value = defaultValue;
    this.ewmaValue = new EWMA(alpha);
  }

  /**
   * @return the value of this metric
   */
  public Double getValue() {
    return value;
  }

  /**
   * @return the current EWMA value of this metric
   */
  public Double getEwmaValue() {
    return ewmaValue.getCurrentEwma();
  }

  /**
   * Update the value of this metric.
   * @param updateValue the value to set
   */
  public void updateValue(final double updateValue) {
    this.value = updateValue;
    this.ewmaValue.updateAndTick(updateValue);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final EWMAMetric that = (EWMAMetric) o;

    if (Double.compare(that.getValue(), getValue()) != 0) {
      return false;
    }
    return getEwmaValue() != null ? getEwmaValue().equals(that.getEwmaValue()) : that.getEwmaValue() == null;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    temp = Double.doubleToLongBits(getValue());
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + (getEwmaValue() != null ? getEwmaValue().hashCode() : 0);
    return result;
  }
}