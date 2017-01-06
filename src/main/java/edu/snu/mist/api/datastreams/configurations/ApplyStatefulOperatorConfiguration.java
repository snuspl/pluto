/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.operators.Operator;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;

/**
 * A configuration for the operators that use ApplyStatefulFunction as a udf.
 */
public final class ApplyStatefulOperatorConfiguration extends ConfigurationModuleBuilder {

  /**
   * Required Implementation of ApplyStatefulFunction.
   */
  public static final RequiredImpl<ApplyStatefulFunction> UDF = new RequiredImpl<>();

  /**
   * Required operator class.
   */
  public static final RequiredImpl<Operator> OPERATOR = new RequiredImpl<>();

  /**
   * A configuration for binding the class of the user-defined function.
   */
  public static final ConfigurationModule CONF = new ApplyStatefulOperatorConfiguration()
      .bindImplementation(ApplyStatefulFunction.class, UDF)
      .bindImplementation(Operator.class, OPERATOR)
      .build();
}
