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
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SinkConfiguration.
 */
public class SinkConfigurationTest {

  /**
   * Test for REEFNetworkSinkBuilder.
   */
  @Test
  public void testREEFNetworkSinkBuilder() {
    final SinkConfiguration reefNetworkSinkConfiguration = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSinkParameters.NAME_SERVICE_PORT, 8080)
        .set(REEFNetworkSinkParameters.CONNECTION_ID, "TestConn")
        .set(REEFNetworkSinkParameters.RECEIVER_ID, "TestReceiver")
        .set(REEFNetworkSinkParameters.CODEC, StringCodec.class)
        .build();

    Assert.assertEquals(reefNetworkSinkConfiguration.
        getConfigurationValue(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME), "localhost");
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(
        REEFNetworkSinkParameters.NAME_SERVICE_PORT), 8080);
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.CONNECTION_ID)
        , "TestConn");
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.RECEIVER_ID)
        , "TestReceiver");
    Assert.assertEquals(reefNetworkSinkConfiguration.getConfigurationValue(REEFNetworkSinkParameters.CODEC)
        , StringCodec.class);
  }

  /**
   * Test for duplicate configuration handling in SinkConfigurationBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSinkBuilderDuplicate() {
    // NCSSinkParameters.NAME_SERVER_HOSTNAME is duplicate!
    final SinkConfigurationBuilder builder = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "remotehost");
  }

  /**
   * Test for missing parameter detection in SinkConfigurationBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSinkBuilderMissingParameter() {
    // NCSSinkParameters.CODEC is missing!
    final SinkConfiguration reefNetworkSinkConfiguration = new REEFNetworkSinkConfigurationBuilderImpl()
        .set(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSinkParameters.NAME_SERVICE_PORT, 8080)
        .set(REEFNetworkSinkParameters.CONNECTION_ID, "TestConn")
        .set(REEFNetworkSinkParameters.RECEIVER_ID, "TestReceiver")
        .build();
  }
}
