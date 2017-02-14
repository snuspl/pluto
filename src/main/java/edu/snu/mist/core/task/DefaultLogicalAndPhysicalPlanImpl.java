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

import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.Tuple;

/**
 * This class contains the logical and physical plan of a query.
 */
final class DefaultLogicalAndPhysicalPlanImpl implements LogicalAndPhysicalPlan {

  private final DAG<LogicalVertex, Direction> logicalPlan;
  private final DAG<PhysicalVertex, Tuple<Direction, Integer>> physicalPlan;

  public DefaultLogicalAndPhysicalPlanImpl(final DAG<LogicalVertex, Direction> logicalPlan,
                                           final DAG<PhysicalVertex, Tuple<Direction, Integer>> physicalPlan) {
    this.logicalPlan = logicalPlan;
    this.physicalPlan = physicalPlan;
  }

  /**
   * Return the logical plan.
   * @return logical plan
   */
  public DAG<LogicalVertex, Direction> getLogicalPlan() {
    return logicalPlan;
  }

  /**
   * Return the physical plan.
   * @return physical plan
   */
  public DAG<PhysicalVertex, Tuple<Direction, Integer>> getPhysicalPlan() {
    return physicalPlan;
  }
}
