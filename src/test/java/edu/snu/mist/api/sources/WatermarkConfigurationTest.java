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
   * Configuration values for WatermarkGenerator.
   */
  private final long period = 100;
  private final long expectedDelay = 0;
  private final MISTFunction watermarkFunction = input -> input.toString().split(":")[0].equals("Watermark");
  private final MISTFunction parsingTimestampFunction = input -> Long.parseLong(input.toString().split(":")[1]);

  /**
   * Test for TestSocketSource configuration builder.
   */
  @Test
  public void testWatermarkConfBuilder() {
    final PunctuatedWatermarkConfiguration punctuatedWatermarkConfiguration =
        new PunctuatedWatermarkConfigurationBuilder()
        .set(PunctuatedWatermarkParameters.WATERMARK_PREDICATE, watermarkFunction)
        .set(PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK, parsingTimestampFunction)
        .build();
    final PeriodicWatermarkConfiguration periodicWatermarkConfiguration =
        new PeriodicWatermarkConfigurationBuilder()
        .set(PeriodicWatermarkParameters.EXPECTED_DELAY, expectedDelay)
        .set(PeriodicWatermarkParameters.PERIOD, period)
        .build();

    Assert.assertEquals(period,
        (long) periodicWatermarkConfiguration.getConfigurationValue(PeriodicWatermarkParameters.PERIOD));
    Assert.assertEquals(expectedDelay,
        (long) periodicWatermarkConfiguration.getConfigurationValue(PeriodicWatermarkParameters.EXPECTED_DELAY));

    Assert.assertEquals(watermarkFunction,
        punctuatedWatermarkConfiguration.getConfigurationValue(PunctuatedWatermarkParameters.WATERMARK_PREDICATE));
    Assert.assertEquals(parsingTimestampFunction,
        punctuatedWatermarkConfiguration.getConfigurationValue(
            PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK));
  }
}