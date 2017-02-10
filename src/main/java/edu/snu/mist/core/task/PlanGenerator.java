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
package edu.snu.mist.core.task;

import edu.snu.mist.formats.avro.AvroLogicalPlan;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * This interface is for generating a pair of the logical and physical plan from avro logical plan.
 */
@DefaultImplementation(DefaultPlanGeneratorImpl.class)
public interface PlanGenerator {
  /**
   * Generates the pair of the logical and physical plan by deserializing the avro logical plan.
   * @param queryIdAndAvroLogicalPlan the tuple of queryId and avro logical plan
   * @return a pair of the logical and physical plan
   */
  LogicalAndPhysicalPlan generate(Tuple<String, AvroLogicalPlan> queryIdAndAvroLogicalPlan)
      throws IOException, ClassNotFoundException, InjectionException;
}
