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

import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
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
  private final int socketPort = 8088;

  /**
   * Test for TestSocketSource configuration builder.
   */
  @Test
  public void testTextSocketSourceConfBuilder() {
    final SourceConfiguration textSocketSourceConfiguration = new TextSocketSourceConfigurationBuilderImpl()
        .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, socketHostName)
        .set(TextSocketSourceParameters.SOCKET_HOST_PORT, socketPort)
        .build();

    Assert.assertEquals(socketHostName,
        textSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_ADDRESS));
    Assert.assertEquals(socketPort,
        textSocketSourceConfiguration.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
  }
}
