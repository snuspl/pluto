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
package edu.snu.mist.api;

import edu.snu.mist.api.sources.builder.REEFNetworkSourceBuilderImpl;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import org.apache.reef.wake.remote.impl.StringCodec;

/**
 * This class contains necessary parameters for API testing.
 */
public final class APITestParameters {

  private APITestParameters() {
    // Do nothing here
  }

  public static final SourceConfiguration TEST_REEF_NETWORK_SOURCE_CONF = new REEFNetworkSourceBuilderImpl()
      .set(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME, "localhost")
      .set(REEFNetworkSourceParameters.NAME_SERVICE_PORT, 8080)
      .set(REEFNetworkSourceParameters.CONNECTION_ID, "TestConn")
      .set(REEFNetworkSourceParameters.SENDER_ID, "TestSender")
      .set(REEFNetworkSourceParameters.CODEC, StringCodec.class)
      .build();
}