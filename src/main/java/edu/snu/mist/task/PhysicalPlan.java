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

import edu.snu.mist.common.DAG;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;

import java.util.Map;
import java.util.Set;

/**
 * This interface represents a PhysicalPlan of a query.
 * TODO[MIST-68]: Receive and deserialize logical plans into physical plans.
 * @param <E> type of operator
 */
public interface PhysicalPlan<E> {

  /**
   * Gets the DAG of operators.
   * @return a DAG
   */
  DAG<E> getOperators();

  /**
   * Gets the map containing SourceGenerator and its next operators.
   * @return a map
   */
  Map<SourceGenerator, Set<E>> getSourceMap();

  /**
   * Gets the map of operator and sinks.
   * @return a map
   */
  Map<E, Set<Sink>> getSinkMap();
}
