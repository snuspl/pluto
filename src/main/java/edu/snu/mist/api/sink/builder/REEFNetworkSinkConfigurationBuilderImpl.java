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

package edu.snu.mist.api.sink.builder;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;

import javax.inject.Inject;
import java.util.Arrays;

/**
 * This class builds SinkConfiguration of REEFNetworkStreamSink.
 */
public final class REEFNetworkSinkConfigurationBuilderImpl extends SinkConfigurationBuilderImpl {

  /**
   * Required Parameters for ReefNetworkSink.
   */
  private static String[] reefNetworkRequiredParameters = {
      REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME,
      REEFNetworkSinkParameters.NAME_SERVICE_PORT,
      REEFNetworkSinkParameters.RECEIVER_ID,
      REEFNetworkSinkParameters.CONNECTION_ID,
      REEFNetworkSinkParameters.CODEC
  };

  @Inject
  public REEFNetworkSinkConfigurationBuilderImpl() {
    requiredParameters.addAll(Arrays.asList(reefNetworkRequiredParameters));
  }

  @Override
  public StreamType.SinkType getSinkType() {
    return StreamType.SinkType.REEF_NETWORK_SINK;
  }
}