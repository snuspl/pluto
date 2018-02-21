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

import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.functions.WatermarkTimestampFunction;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.common.sources.PunctuatedEventGenerator;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;

/**
 * The class represents punctuated watermark configuration that is used for binding udf classes.
 */
public final class PunctuatedWatermarkUdfBindingConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredImpl<WatermarkTimestampFunction> TIMESTAMP_EXTRACT_FUNC = new RequiredImpl<>();
  public static final RequiredImpl<MISTPredicate> WATERMARK_PREDICATE = new RequiredImpl<>();

  public static final ConfigurationModule CONF = new PunctuatedWatermarkUdfBindingConfiguration()
      .bindImplementation(WatermarkTimestampFunction.class, TIMESTAMP_EXTRACT_FUNC)
      .bindImplementation(MISTPredicate.class, WATERMARK_PREDICATE)
      .bindImplementation(EventGenerator.class, PunctuatedEventGenerator.class)
      .build();
}