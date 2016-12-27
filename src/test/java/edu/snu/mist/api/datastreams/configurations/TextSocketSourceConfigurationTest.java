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

import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.SourceParameters;
import edu.snu.mist.common.parameters.TextSocketSourceParameters;
import org.apache.reef.io.Tuple;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for TextSocketSourceConfiguration.
 */
public class TextSocketSourceConfigurationTest {
  /**
   * Configuration values for TextSocketSource.
   */
  private final String socketHostName = "localhost2";
  private final Integer socketPort = 8088;

  /**
   * Test for TextSocketSource configuration builder.
   */
  @Test
  public void testTextSocketSourceConfBuilder() {
    final MISTFunction<String, Tuple<String, Long>> timestampExtractionFunction =
        input -> new Tuple<>(input.split(":")[0], Long.parseLong(input.split(":")[1]));

    final TextSocketSourceConfiguration textSocketTextSocketSourceConfiguration =
        TextSocketSourceConfiguration.newBuilder()
        .setHostAddress(socketHostName)
        .setHostPort(socketPort)
        .setTimestampExtractionFunction(timestampExtractionFunction)
        .build();

    Assert.assertEquals(socketHostName,
        textSocketTextSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_ADDRESS));
    Assert.assertEquals(socketPort,
        textSocketTextSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
    Assert.assertEquals(timestampExtractionFunction,
        textSocketTextSocketSourceConfiguration.getConfigurationValue(SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION));
  }
}
