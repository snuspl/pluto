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

import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.parameters.PunctuatedWatermarkParameters;
import edu.snu.mist.formats.avro.WatermarkTypeEnum;

import java.util.Arrays;
import java.util.Map;

/**
 * The class represents punctuated watermark configuration.
 * @param <T> the type of source data
 */
public final class PunctuatedWatermarkConfiguration<T> extends WatermarkConfiguration<T> {

  private PunctuatedWatermarkConfiguration(final Map<String, Object> configMap) {
    super(configMap);
  }

  @Override
  public WatermarkTypeEnum getWatermarkType() {
    return WatermarkTypeEnum.PUNCTUATED;
  }

  /**
   * Gets the builder for Configuration construction.
   * @param <K> the type of source data that the target configuration will have
   * @return the builder
   */
  public static <K> PunctuatedWatermarkConfigurationBuilder<K> newBuilder() {
    return new PunctuatedWatermarkConfigurationBuilder<>();
  }

  /**
   * This class builds punctuated WatermarkConfiguration.
   * @param <V> the type of source data that the target configuration will have
   */
  public static final class PunctuatedWatermarkConfigurationBuilder<V> extends MISTConfigurationBuilderImpl {

    /**
     * Required parameters for punctuated WatermarkConfiguration.
     */
    private final String[] punctuatedWatermarkParameters = {
        PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK,
        PunctuatedWatermarkParameters.WATERMARK_PREDICATE
    };

    private PunctuatedWatermarkConfigurationBuilder() {
      requiredParameters.addAll(Arrays.asList(punctuatedWatermarkParameters));
    }

    /**
     * Tests that required parameters are set and builds the PunctuatedWatermarkConfiguration.
     * @return the configuration
     */
    public PunctuatedWatermarkConfiguration<V> build() {
      readyToBuild();
      return new PunctuatedWatermarkConfiguration<>(configMap);
    }

    /**
     * Sets the configuration for the function parsing timestamp from watermark to the given function.
     * @param function the function given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PunctuatedWatermarkConfigurationBuilder<V> setParsingWatermarkFunction(
        final MISTFunction<V, Long> function) {
      set(PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK, function);
      return this;
    }

    /**
     * Sets the configuration for the predicate testing whether the input is watermark or not to the given function.
     * @param predicate the predicate given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PunctuatedWatermarkConfigurationBuilder<V> setWatermarkPredicate(final MISTPredicate<V> predicate) {
      set(PunctuatedWatermarkParameters.WATERMARK_PREDICATE, predicate);
      return this;
    }
  }
}