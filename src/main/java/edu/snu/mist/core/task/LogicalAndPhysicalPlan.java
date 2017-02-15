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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;

/**
 * This interface holds the logical and physical plan.
 * A logical plan is a DAG which consists of logical vertices and edges.
 * A physical plan ins a DAG which consists of physical vertices and edges.
 * In MIST, a logical vertex points to a physical vertex.
 * The physical vertex holds the actual object that performs actual computations,
 * such as receiving data stream (source), filter, map (operators), and sending results to clients (sink).
 * The logical and physical vertex is in the M:1 relationship, because several physical vertices can be merged to one.
 */
interface LogicalAndPhysicalPlan {

  /**
   * Return the logical plan.
   * @return logical plan
   */
  DAG<LogicalVertex, MISTEdge> getLogicalPlan();

  /**
   * Return the physical plan.
   * @return physical plan
   */
  DAG<PhysicalVertex, MISTEdge> getPhysicalPlan();
}
