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

import edu.snu.mist.api.sink.builder.REEFNetworkSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.SinkConfigurationBuilder;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SinkConfiguration.
 */
public class SinkConfigurationTest {

  /**
   * Configuration values for REEFNetworkSink.
   */
  private final String nameServerHostName = "localhost";
  private final int nameServerPort = 8080;
  private final String connectionId = "TestConn";
  private final String receiverId = "TestReceiver";
  private final Class codec = StringCodec.class;

  /**
   * Configuration values for TextSocketSink.
   */
  private final String socketHostName = "localhost2";
  private final int socketPort = 8088;

  /**
   * Tests whether REEFNetworkSink configuration contains right information or not.
   */
  @Test
  public void testREEFNetworkSinkConfBuilder() {

    final SinkConfiguration reefNetworkSinkConfiguration = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSinkParameters.NAME_SERVICE_PORT, nameServerPort)
        .set(REEFNetworkSinkParameters.CONNECTION_ID, connectionId)
        .set(REEFNetworkSinkParameters.RECEIVER_ID, receiverId)
        .set(REEFNetworkSinkParameters.CODEC, codec)
        .build();

    Assert.assertEquals(reefNetworkSinkConfiguration.
        getConfigurationValue(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME), nameServerHostName);
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(
        REEFNetworkSinkParameters.NAME_SERVICE_PORT), nameServerPort);
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.CONNECTION_ID)
        , connectionId);
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.RECEIVER_ID)
        , receiverId);
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.CODEC)
        , codec);
  }

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
        textSocketSinkConfiguration.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT));
  }


  /**
   * Test for duplicate configuration handling in SinkConfigurationBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSinkBuilderDuplicate() {
    // NCSSinkParameters.NAME_SERVER_HOSTNAME is duplicate!
    final SinkConfigurationBuilder builder = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "remotehost");
  }

  /**
   * Test for missing parameter detection in SinkConfigurationBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSinkBuilderMissingParameter() {
    // NCSSinkParameters.CODEC is missing!
    final SinkConfiguration reefNetworkSinkConfiguration = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, nameServerHostName)
        .set(REEFNetworkSinkParameters.NAME_SERVICE_PORT, nameServerPort)
        .set(REEFNetworkSinkParameters.CONNECTION_ID, connectionId)
        .set(REEFNetworkSinkParameters.RECEIVER_ID, receiverId)
        .build();
  }
}
