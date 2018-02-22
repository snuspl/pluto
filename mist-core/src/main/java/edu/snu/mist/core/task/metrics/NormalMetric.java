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
 * * A class represents a metric without EWMA such as the number of groups or weight.
 * @param <T> the type of this metric
 */
public final class NormalMetric<T> {

  /**
   * The value of metric.
   */
  private volatile T value;

  /**
   * Constructor of NormalMetric.
   * @param defaultValue the default value of this metric.
   */
  public NormalMetric(final T defaultValue) {
    this.value = defaultValue;
  }

  /**
   * @return the value of this metric
   */
  public T getValue() {
    return value;
  }

  /**
   * Set the value of this metric.
   * @param updateValue the value to set
   */
  public void setValue(final T updateValue) {
    this.value = updateValue;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final NormalMetric<?> that = (NormalMetric<?>) o;

    return getValue() != null ? getValue().equals(that.getValue()) : that.getValue() == null;
  }

  @Override
  public int hashCode() {
    return getValue() != null ? getValue().hashCode() : 0;
  }
}