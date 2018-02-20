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
package edu.snu.mist.core.task;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import org.apache.reef.tang.annotations.DefaultImplementation;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.List;

/**
 * This interface is for generating the execution dag from avro operator chain dag.
 */
@DefaultImplementation(DefaultDagGeneratorImpl.class)
public interface DagGenerator {
  /**
   * Generates the execution dag from the configuration dag.
   * @param configDag the dag that has configuration of each vertices
   * @param jarFilePaths jar file path for creating udf instances
   * @return execution dag
   */
  ExecutionDag generate(DAG<ConfigVertex, MISTEdge> configDag, List<String> jarFilePaths)
      throws IOException, ClassNotFoundException, InjectionException;
}
