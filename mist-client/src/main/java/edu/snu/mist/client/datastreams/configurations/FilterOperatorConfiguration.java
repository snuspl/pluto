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
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.operators.Operator;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredImpl;

/**
 * A configuration for filter operator that binds a udf class.
 */
public final class FilterOperatorConfiguration extends ConfigurationModuleBuilder {

  /**
   * Required Implementation of MISTPredicate.
   */
  public static final RequiredImpl<MISTPredicate> MIST_PREDICATE = new RequiredImpl<>();

  /**
   * A configuration for binding udf class of filter operator.
   */
  public static final ConfigurationModule CONF = new FilterOperatorConfiguration()
      .bindImplementation(Operator.class, FilterOperator.class)
      .bindImplementation(MISTPredicate.class, MIST_PREDICATE)
      .build();
}
