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

import edu.snu.mist.api.sources.builder.REEFNetworkSourceBuilderImpl;
import edu.snu.mist.api.sources.builder.SourceBuilder;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for SourceConfiguration.
 */
public class SourceConfigurationTest {

  /**
   * Test for REEFNetworkSourceBuilder.
   */
  @Test
  public void testREEFNetworkSourceBuilder() {
    final SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, 8080)
        .set(REEFNetworkSourceParameters.CONNECTION_ID, "TestConn")
        .set(REEFNetworkSourceParameters.SENDER_ID, "TestSender")
        .set(REEFNetworkSourceParameters.CODEC, StringCodec.class)
        .build();

    Assert.assertEquals(reefNetworkSourceConfiguration.
        getConfigurationValue(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME), "localhost");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(
        REEFNetworkSourceParameters.NAME_SERVICE_PORT), 8080);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.CONNECTION_ID)
        , "TestConn");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.SENDER_ID)
        , "TestSender");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(REEFNetworkSourceParameters.CODEC)
        , StringCodec.class);
  }

  /**
   * Test for duplicate configuration handling in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderDuplicate() {
    // NCSSourceParameters.NAME_SERVER_HOSTNAME is duplicate!
    final SourceBuilder builder = new REEFNetworkSourceBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "remotehost");
  }

  /**
   * Test for missing parameter detection in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderMissingParameter() {
    // NCSSourceParameters.CODEC is missing!
    final SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceBuilderImpl()
        .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, 8080)
        .set(REEFNetworkSourceParameters.CONNECTION_ID, "TestConn")
        .set(REEFNetworkSourceParameters.SENDER_ID, "TestSender")
        .build();
  }
}
