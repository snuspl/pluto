/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.parameters.PeriodicWatermarkParameters;
import edu.snu.mist.formats.avro.WatermarkTypeEnum;

import java.util.Arrays;
import java.util.Map;

/**
 * The class represents periodic watermark configuration.
 * @param <T> the type of source data
 */
public final class PeriodicWatermarkConfiguration<T> extends WatermarkConfiguration<T> {

  private PeriodicWatermarkConfiguration(final Map<String, Object> configMap) {
    super(configMap);
  }

  @Override
  public WatermarkTypeEnum getWatermarkType() {
    return WatermarkTypeEnum.PERIODIC;
  }

  /**
   * Gets the builder for Configuration construction.
   * @param <K> the type of source data that the target configuration will have
   * @return the builder
   */
  public static <K> PeriodicWatermarkConfigurationBuilder<K> newBuilder() {
    return new PeriodicWatermarkConfigurationBuilder<>();
  }

  /**
   * This class builds periodic WatermarkConfiguration.
   * @param <V> the type of source data that the target configuration will have
   */
  public static final class PeriodicWatermarkConfigurationBuilder<V> extends MISTConfigurationBuilderImpl {

    /**
     * Required parameters for periodic WatermarkConfiguration.
     */
    private final String[] periodicWatermarkRequiredParameters = {
        PeriodicWatermarkParameters.PERIOD
    };

    /**
     * Optional parameters for periodic WatermarkConfiguration.
     */
    private final String[] periodicWatermarkOptionalParameters = {
        PeriodicWatermarkParameters.EXPECTED_DELAY
    };

    private PeriodicWatermarkConfigurationBuilder() {
      requiredParameters.addAll(Arrays.asList(periodicWatermarkRequiredParameters));
      optionalParameters.addAll(Arrays.asList(periodicWatermarkOptionalParameters));
    }

    /**
     * Tests that required parameters are set and builds the PeriodicWatermarkConfiguration.
     * @return the configuration
     */
    public PeriodicWatermarkConfiguration<V> build() {
      readyToBuild();
      return new PeriodicWatermarkConfiguration<>(configMap);
    }

    /**
     * Sets the configuration for the watermark period to the given period.
     * @param period the period given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PeriodicWatermarkConfigurationBuilder<V> setWatermarkPeriod(final int period) {
      set(PeriodicWatermarkParameters.PERIOD, period);
      return this;
    }

    /**
     * Sets the configuration for the expected delay to the given delay.
     * Expected delay means that the maximum delay between the time of data creation and being received by task.
     * This is an optional setting for event-time processing.
     * @param expectedDelay the expected delay given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PeriodicWatermarkConfigurationBuilder<V> setExpectedDelay(final int expectedDelay) {
      set(PeriodicWatermarkParameters.EXPECTED_DELAY, expectedDelay);
      return this;
    }
  }
}