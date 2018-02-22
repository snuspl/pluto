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
import edu.snu.mist.common.operators.ReduceByKeyOperator;
import edu.snu.mist.common.parameters.KeyIndex;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * A configuration for ReduceByKeyOperator that binds a udf class.
 */
public final class ReduceByKeyOperatorUDFConfiguration extends ConfigurationModuleBuilder {

  /**
   * Required Parameter for binding the serialized objects of the user-defined function.
   */
  public static final RequiredParameter<String> UDF_STRING = new RequiredParameter<>();

  /**
   * Required Parameter for the key index of the reduceByKey operator.
   */
  public static final RequiredParameter<Integer> KEY_INDEX = new RequiredParameter<>();

  /**
   * A configuration for binding the serialized objects of the user-defined function.
   */
  public static final ConfigurationModule CONF = new ReduceByKeyOperatorUDFConfiguration()
      .bindNamedParameter(SerializedUdf.class, UDF_STRING)
      .bindNamedParameter(KeyIndex.class, KEY_INDEX)
      .bindImplementation(Operator.class, ReduceByKeyOperator.class)
      .build();
}
