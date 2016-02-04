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

package edu.snu.mist.api.sources.builder;

import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;

import javax.inject.Inject;
import java.util.Arrays;

/**
 * This class builds SourceConfiguration of REEFNetworkStreamSource.
 */
public final class REEFNetworkSourceConfigurationBuilderImpl extends SourceConfigurationBuilderImpl {

  /**
   * Required Parameters for ReefNetworkSource.
   */
  private static String[] reefNetworkRequiredParameters = {
      REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME,
      REEFNetworkSourceParameters.NAME_SERVICE_PORT,
      REEFNetworkSourceParameters.SENDER_ID,
      REEFNetworkSourceParameters.CONNECTION_ID,
      REEFNetworkSourceParameters.CODEC
  };

  @Inject
  public REEFNetworkSourceConfigurationBuilderImpl() {
    requiredParameters.addAll(Arrays.asList(reefNetworkRequiredParameters));
  }

  @Override
  public String getSourceType() {
    return "NCSSourceConf";
  }
}