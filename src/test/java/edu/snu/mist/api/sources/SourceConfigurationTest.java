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

import edu.snu.mist.api.sources.builder.REEFNetworkSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.builder.SourceConfigurationBuilder;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SourceConfiguration.
 */
public class SourceConfigurationTest {

  /**
   * Configuration values for REEFNetworkSource.
   */
  private final String nameServerHostName = "localhost";
  private final int nameServerPort = 8080;
  private final String connectionId = "TestConn";
  private final String senderId = "TestSender";
  private final Class codec = StringCodec.class;

  /**
   * Configuration values for TextSocketSource.
   */
  private final String socketHostName = "localhost2";
  private final int socketPort = 8088;

  /**
   * Test for REEFNetworkSource configuration builder.
   */
  @Test
  public void testREEFNetworkSourceConfBuilder() {
    final SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceConfigurationBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, nameServerPort)
        .set(REEFNetworkSourceParameters.CONNECTION_ID, connectionId)
        .set(REEFNetworkSourceParameters.SENDER_ID, senderId)
        .set(REEFNetworkSourceParameters.CODEC, codec)
        .build();

    Assert.assertEquals(reefNetworkSourceConfiguration.
        getConfigurationValue(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME), nameServerHostName);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(
        REEFNetworkSourceParameters.NAME_SERVICE_PORT), nameServerPort);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.CONNECTION_ID)
        , connectionId);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.SENDER_ID)
        , senderId);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.CODEC)
        , codec);
  }

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

  /**
   * Test for duplicate configuration handling in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderDuplicate() {
    // NCSSourceParameters.NAME_SERVER_HOSTNAME is duplicate!
    final SourceConfigurationBuilder builder = new REEFNetworkSourceConfigurationBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "remotehost");
  }

  /**
   * Test for missing parameter detection in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderMissingParameter() {
    // NCSSourceParameters.CODEC is missing!
    final SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceConfigurationBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, nameServerPort)
        .set(REEFNetworkSourceParameters.CONNECTION_ID, connectionId)
        .set(REEFNetworkSourceParameters.SENDER_ID, senderId)
        .build();
  }
}
