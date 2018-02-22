/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.client.datastreams.configurations;

import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.parameters.WindowInterval;
import edu.snu.mist.common.parameters.WindowSize;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A configuration for window operators (time, count, session).
 */
public final class WindowOperatorConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<Integer> WINDOW_SIZE = new RequiredParameter<Integer>();
  public static final RequiredParameter<Integer> WINDOW_INTERVAL = new RequiredParameter<Integer>();
  public static final RequiredImpl<Operator> OPERATOR = new RequiredImpl<>();

  public static final ConfigurationModule CONF = new WindowOperatorConfiguration()
      .bindNamedParameter(WindowSize.class, WINDOW_SIZE)
      .bindNamedParameter(WindowInterval.class, WINDOW_INTERVAL)
      .bindImplementation(Operator.class, OPERATOR)
      .build();
}
