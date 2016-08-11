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
package edu.snu.mist.api.sources;

import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.sources.builder.*;
import edu.snu.mist.api.sources.parameters.PeriodicWatermarkParameters;
import edu.snu.mist.api.sources.parameters.PunctuatedWatermarkParameters;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for WatermarkConfiguration.
 */
public class WatermarkConfigurationTest {
  /**
   * Test for PunctuatedWatermark configuration builder.
   */
  @Test
  public void testPunctuatedWatermarkConfBuilder() {
    /**
     * Configuration values for Watermark.
     */
    final MISTPredicate<String> watermarkPredicate = input -> input.split(":")[0].equals("Watermark");
    final MISTFunction<String, Long> parsingTimestampFunction = input -> Long.parseLong(input.split(":")[1]);

    final PunctuatedWatermarkConfiguration<String> punctuatedWatermarkConfiguration =
        PunctuatedWatermarkConfiguration.<String>newBuilder()
        .setWatermarkPredicate(watermarkPredicate)
        .setParsingWatermarkFunction(parsingTimestampFunction)
        .build();

    Assert.assertEquals(watermarkPredicate,
        punctuatedWatermarkConfiguration.getConfigurationValue(PunctuatedWatermarkParameters.WATERMARK_PREDICATE));
    Assert.assertEquals(parsingTimestampFunction,
        punctuatedWatermarkConfiguration.getConfigurationValue(
            PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK));
  }

  /**
   * Test for PeriodicWatermark configuration builder.
   */
  @Test
  public void testPeriodicWatermarkConfBuilder() {
    /**
     * Configuration values for Watermark.
     */
    final Integer period = 100;
    final Integer expectedDelay = 0;

    final PeriodicWatermarkConfiguration periodicWatermarkConfiguration =
        PeriodicWatermarkConfiguration.newBuilder()
            .setWatermarkPeriod(period)
            .setExpectedDelay(expectedDelay)
            .build();

    Assert.assertEquals(period,
            periodicWatermarkConfiguration.getConfigurationValue(PeriodicWatermarkParameters.PERIOD));
    Assert.assertEquals(expectedDelay,
            periodicWatermarkConfiguration.getConfigurationValue(PeriodicWatermarkParameters.EXPECTED_DELAY));
  }
}