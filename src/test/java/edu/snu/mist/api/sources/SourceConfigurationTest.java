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
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilder;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import org.apache.reef.io.Tuple;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SourceConfiguration.
 */
public class SourceConfigurationTest {

  /**
   * Configuration values for TextSocketSource.
   */
  private final String socketHostName = "localhost2";
  private final long socketPort = 8088;
  private final MISTFunction<String, Tuple<String, Long>> timestampExtractionFunction =
      (MISTFunction) (input -> new Tuple<>(input.toString().split(":")[0],
          Long.parseLong(input.toString().split(":")[1])));

  /**
   * Test for TestSocketSource configuration builder.
   */
  @Test
  public void testTextSocketSourceConfBuilder() {
    final SourceConfiguration textSocketSourceConfiguration =
        new TextSocketSourceConfigurationBuilder()
        .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, socketHostName)
        .set(TextSocketSourceParameters.SOCKET_HOST_PORT, socketPort)
        .set(TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION, timestampExtractionFunction)
        .build();

    Assert.assertEquals(socketHostName,
        textSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_ADDRESS));
    Assert.assertEquals(socketPort,
        (long) textSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
    Assert.assertEquals(timestampExtractionFunction,
        textSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION));
  }
}
