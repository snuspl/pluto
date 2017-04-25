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
package edu.snu.mist.common;

/**
 * A class which contains helper methods for metrics.
 */
public final class MetricUtil {

  /**
   * The default decaying rate for calculating EWMA.
   */
  private static double defaultDecayingRate = 0.3;

  private MetricUtil() {
    // should not be called.
  }

  public static double calculateEwma(final double newValue,
                                     final double oldEWMA,
                                     final double decayingRate) {
    return (1.0 - decayingRate) * newValue + decayingRate * oldEWMA;
  }

  public static double calculateEwma(final double newValue,
                                     final double oldEWMA) {
    return calculateEwma(newValue, oldEWMA, defaultDecayingRate);
  }
}
