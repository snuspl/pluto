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

package edu.snu.mist.api.sources.builder;

import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;
import org.apache.reef.wake.remote.Codec;

/**
 * The ConfigurationModule which provides a configuration for NCSSourceBuilder in user-side.
 */
public class NCSSourceBuilder extends ConfigurationModuleBuilder {

  /**
   * Tang NamedParameter for NCS connection name.
   */
  @NamedParameter
  public static class ConnectionName implements Name<String> {
  }

  /**
   * Tang NamedParameter for the name of the source NCS.
   */
  @NamedParameter
  public static class SourceNCSName implements Name<String> {
  }

  /**
   * Tang NamedParameter for the codec used for deserializing data.
   */
  @NamedParameter
  public static class DeserializeCodec implements Name<Codec> {
  }

  public static final RequiredParameter<String> CONNECTION_NAME = new RequiredParameter<>();

  public static final RequiredParameter<String> SOURCE_NCS_NAME = new RequiredParameter<>();

  public static final RequiredParameter<Codec> CODEC = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new NCSSourceBuilder().merge(NameResolverConfiguration.CONF)
      .bindNamedParameter(ConnectionName.class, CONNECTION_NAME)
      .bindNamedParameter(SourceNCSName.class, SOURCE_NCS_NAME)
      .bindNamedParameter(DeserializeCodec.class, CODEC)
      .build();

}
