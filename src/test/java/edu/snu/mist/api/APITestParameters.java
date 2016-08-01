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

import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;

/**
 * This class contains necessary parameters for API testing.
 */
public final class APITestParameters {

  private APITestParameters() {
    // Do nothing here
  }

  public static final SourceConfiguration LOCAL_TEXT_SOCKET_SOURCE_CONF =
      new TextSocketSourceConfigurationBuilderImpl()
      .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSourceParameters.SOCKET_HOST_PORT, 13666)
      .build();

  public static final SinkConfiguration LOCAL_TEXT_SOCKET_SINK_CONF = new TextSocketSinkConfigurationBuilderImpl()
      .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, "localhost")
      .set(TextSocketSinkParameters.SOCKET_HOST_PORT, 13667)
      .build();
}