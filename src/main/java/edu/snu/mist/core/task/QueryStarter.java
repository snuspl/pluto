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
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface represents a component that is responsible for starting and executing queries.
 */
@DefaultImplementation(ImmediateQueryMergingStarter.class)
public interface QueryStarter {

  /**
   * Start to execute the submitted query.
   * @param queryId query id
   * @param submittedDag the submitted dag
   */
  void start(String queryId, DAG<ExecutionVertex, MISTEdge> submittedDag);
}
