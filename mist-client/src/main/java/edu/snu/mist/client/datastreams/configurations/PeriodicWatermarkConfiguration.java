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
package edu.snu.mist.client.datastreams.configurations;

import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;

import java.util.HashMap;
import java.util.Map;

/**
 * The class represents periodic watermark configuration.
 */
public final class PeriodicWatermarkConfiguration {

  private PeriodicWatermarkConfiguration() {
    //do nothing
  }

  /**
   * Gets the builder for Configuration construction.
   * @return the builder
   */
  public static PeriodicWatermarkConfigurationBuilder newBuilder() {
    return new PeriodicWatermarkConfigurationBuilder();
  }

  /**
   * This class builds periodic WatermarkConfiguration.
   */
  public static final class PeriodicWatermarkConfigurationBuilder {

    private int watermarkPeriod;
    private int watermarkDelay;

    /**
     * Builds the PeriodicWatermarkConfiguration.
     * @return the configuration
     */
    public WatermarkConfiguration build() {
      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.Watermark.EVENT_GENERATOR.name(),
          ConfValues.EventGeneratorType.PERIODIC_EVENT_GEN.name());
      confMap.put(ConfKeys.Watermark.PERIODIC_WATERMARK_PERIOD.name(), String.valueOf(watermarkPeriod));
      confMap.put(ConfKeys.Watermark.PERIODIC_WATERMARK_DELAY.name(), String.valueOf(watermarkDelay));
      return new WatermarkConfiguration(confMap);
    }

    /**
     * Sets the configuration for the watermark period to the given period.
     * @param period the period given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PeriodicWatermarkConfigurationBuilder setWatermarkPeriod(final int period) {
      watermarkPeriod = period;
      return this;
    }

    /**
     * Sets the configuration for the expected delay to the given delay.
     * Expected delay means that the maximum delay between the time of data creation and being received by task.
     * This is an optional setting for event-time processing.
     * @param expectedDelay the expected delay given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PeriodicWatermarkConfigurationBuilder setExpectedDelay(final int expectedDelay) {
      watermarkDelay = expectedDelay;
      return this;
    }
  }
}