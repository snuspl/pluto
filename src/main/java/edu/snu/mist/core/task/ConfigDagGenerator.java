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
import edu.snu.mist.formats.avro.AvroDag;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface is for generating the configuration dag of vertices from avro dag.
 */
@DefaultImplementation(DefaultConfigDagGeneratorImpl.class)
public interface ConfigDagGenerator {
  /**
   * Generates the configuration dag from the avro dag.
   * It extracts configurations of vertices from the avro dag and creates a dag that contains the configuration.
   * By doing this, we can decouple the generation logic of execution dag from the avro dag.
   * @param avroDag the dag that is serialized by Avro
   * @return configuration dag
   */
  DAG<ConfigVertex, MISTEdge> generate(AvroDag avroDag);
}
