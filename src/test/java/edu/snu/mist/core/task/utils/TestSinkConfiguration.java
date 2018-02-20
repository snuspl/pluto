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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.common.sinks.Sink;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;

/**
 * This is a simple sink configuration for testing.
 */
public final class TestSinkConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredImpl<Sink> SINK = new RequiredImpl<>();

  public static final ConfigurationModule CONF = new TestSinkConfiguration()
      .bindImplementation(Sink.class, SINK)
      .build();
}