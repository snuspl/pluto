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
package edu.snu.mist.api.sink;

import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SinkConfiguration.
 */
public class SinkConfigurationTest {

  /**
   * Configuration values for TextSocketSink.
   */
  private final String socketHostName = "localhost2";
  private final long socketPort = 8088;

  /**
   * Test whether TextSocketSink configuration contains right information or not.
   */
  @Test
  public void testTextSocketSinkConfBuilder() {
    final SinkConfiguration textSocketSinkConfiguration = new TextSocketSinkConfigurationBuilderImpl()
        .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, socketHostName)
        .set(TextSocketSinkParameters.SOCKET_HOST_PORT, socketPort)
        .build();

    Assert.assertEquals(socketHostName,
        textSocketSinkConfiguration.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_ADDRESS));
    Assert.assertEquals(socketPort,
        (long) textSocketSinkConfiguration.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT));
  }
}
