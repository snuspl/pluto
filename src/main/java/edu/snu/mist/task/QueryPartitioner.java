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
package edu.snu.mist.task;

import edu.snu.mist.task.operators.Operator;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface converts a PhysicalPlan<Operator> to a PhysicalPlan<PartitionedQuery>
 * by chaining the operators.
 */
@DefaultImplementation(DefaultQueryPartitionerImpl.class)
public interface QueryPartitioner {
  /**
   * Chains the operators and converts the PhysicalPlan<Operator> to the PhysicalPlan<PartitionedQuery>.
   * @param plan a plan
   * @return a physical plan which contains PartitionedQuery.
   */
  PhysicalPlan<PartitionedQuery, MistEvent.Direction> chainOperators(PhysicalPlan<Operator, MistEvent.Direction> plan);
}
