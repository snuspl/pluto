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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.configurations.ConfValues;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.functions.WatermarkTimestampFunction;
import edu.snu.mist.common.parameters.SerializedTimestampParseUdf;
import edu.snu.mist.common.parameters.SerializedWatermarkPredicateUdf;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.common.sources.PunctuatedEventGenerator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * The class represents punctuated watermark configuration.
 */
public final class PunctuatedWatermarkConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> TIMESTAMP_PARSE_OBJECT = new RequiredParameter<>();
  public static final RequiredParameter<String> WATERMARK_PREDICATE = new RequiredParameter<>();

  private static final ConfigurationModule CONF = new PunctuatedWatermarkConfiguration()
      .bindNamedParameter(SerializedTimestampParseUdf.class, TIMESTAMP_PARSE_OBJECT)
      .bindNamedParameter(SerializedWatermarkPredicateUdf.class, WATERMARK_PREDICATE)
      .bindImplementation(EventGenerator.class, PunctuatedEventGenerator.class)
      .build();

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
  public static final class PunctuatedWatermarkConfigurationBuilder<V> {

    private MISTPredicate<V> watermarkPredicate;
    private WatermarkTimestampFunction<V> timestampParseObject;
    private Class<? extends MISTPredicate<V>> watermarkPredicateClass;
    private Configuration watermarkPredicateConf;
    private Class<? extends WatermarkTimestampFunction<V>> extractFuncClass;
    private Configuration extractFuncConf;

    /**
     * Builds the PunctuatedWatermarkConfiguration.
     * @return the configuration
     */
    public WatermarkConfiguration build() {
      final Map<String, String> confMap = new HashMap<>();
      confMap.put(ConfKeys.Watermark.EVENT_GENERATOR.name(),
          ConfValues.EventGeneratorType.PUNCTUATED_EVENT_GEN.name());

      if (watermarkPredicate == null) {
        // No udf is set
        return new WatermarkConfiguration(confMap);
      } else {
        // Lambda object is set
        try {
          confMap.put(ConfKeys.Watermark.TIMESTAMP_PARSE_OBJECT.name(),
               SerializeUtils.serializeToString(timestampParseObject));
          confMap.put(ConfKeys.Watermark.WATERMARK_PREDICATE.name(),
              SerializeUtils.serializeToString(watermarkPredicate));
          return new WatermarkConfiguration(confMap);
        } catch (final IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Sets the configuration for the function parsing timestamp from watermark to the given function.
     * @param function the function given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PunctuatedWatermarkConfigurationBuilder<V> setParsingWatermarkFunction(
        final WatermarkTimestampFunction<V> function) {
      timestampParseObject = function;
      return this;
    }

    /**
     * Sets the configuration for the predicate testing whether the input is watermark or not to the given function.
     * @param predicate the predicate given by users which they want to set
     * @return the configured WatermarkBuilder
     */
    public PunctuatedWatermarkConfigurationBuilder<V> setWatermarkPredicate(
        final MISTPredicate<V> predicate) {
      watermarkPredicate = predicate;
      return this;
    }
  }
}