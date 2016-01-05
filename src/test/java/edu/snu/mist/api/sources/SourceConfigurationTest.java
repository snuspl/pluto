/*
 * Copyright (C) 2015 Seoul National University
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
import edu.snu.mist.api.sources.parameters.NCSSourceParameters;
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
    SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceBuilderImpl()
        .set(NCSSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(NCSSourceParameters.NAME_SERVICE_PORT, 8080)
        .set(NCSSourceParameters.CONNECTION_ID, "TestConn")
        .set(NCSSourceParameters.SENDER_ID, "TestSender")
        .set(NCSSourceParameters.CODEC, StringCodec.class)
        .build();

    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(NCSSourceParameters.NAME_SERVER_HOSTNAME)
        , "localhost");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(NCSSourceParameters.NAME_SERVICE_PORT)
        , 8080);
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(NCSSourceParameters.CONNECTION_ID)
        , "TestConn");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(NCSSourceParameters.SENDER_ID)
        , "TestSender");
    Assert.assertEquals(reefNetworkSourceConfiguration.getConfigurationValue(NCSSourceParameters.CODEC)
        , StringCodec.class);
  }

  /**
   * Test for duplicate configuration handling in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderDuplicate() {
    // NCSSourceParameters.NAME_SERVER_HOSTNAME is duplicate!
    SourceBuilder builder = new REEFNetworkSourceBuilderImpl()
        .set(NCSSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(NCSSourceParameters.NAME_SERVER_HOSTNAME, "remotehost");
  }

  /**
   * Test for missing parameter detection in SourceBuilder.
   */
  @Test(expected = IllegalStateException.class)
  public void testSourceBuilderMissingParameter() {
    // NCSSourceParameters.CODEC is missing!
    SourceConfiguration reefNetworkSourceConfiguration = new REEFNetworkSourceBuilderImpl()
        .set(NCSSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
        .set(NCSSourceParameters.NAME_SERVICE_PORT, 8080)
        .set(NCSSourceParameters.CONNECTION_ID, "TestConn")
        .set(NCSSourceParameters.SENDER_ID, "TestSender")
        .build();
  }
}
