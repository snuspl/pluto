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
package edu.snu.mist.common.stats;

/**
 * A class for calculating EWMA.
 */
public final class EWMA {

  /**
   * A decaying rate for EWMA.
   */
  private final double alpha;

  /**
   * Current ewma value.
   */
  private double currentEwma;

  public EWMA(final double alphaParam) {
    this.alpha = alphaParam;
    this.currentEwma = 0.0;
  }

  public void updateAndTick(final double newValue) {
    this.currentEwma += alpha * (newValue - currentEwma);
  }

  public double getCurrentEwma() {
    return currentEwma;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof EWMA)) {
      return false;
    } else {
      final EWMA ewma = (EWMA) o;
      return this.alpha == ewma.alpha && this.currentEwma == ewma.currentEwma;
    }
  }

  @Override
  public int hashCode() {
    final Double doubleObject = new Double(alpha * currentEwma);
    return doubleObject.hashCode();
  }
}
