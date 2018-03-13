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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;
import edu.snu.mist.core.task.merging.ExecutionVertexGenerator;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A default implementation of DagGenerator.
 */
final class DefaultDagGeneratorImpl implements DagGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultDagGeneratorImpl.class.getName());

  private final ClassLoaderProvider classLoaderProvider;
  private final ExecutionVertexGenerator executionVertexGenerator;

  @Inject
  private DefaultDagGeneratorImpl(final ClassLoaderProvider classLoaderProvider,
                                  final ExecutionVertexGenerator executionVertexGenerator) {
    this.classLoaderProvider = classLoaderProvider;
    this.executionVertexGenerator = executionVertexGenerator;
  }

  private void dfsCreation(final ExecutionVertex parent,
                           final MISTEdge parentEdge,
                           final ConfigVertex currVertex,
                           final Map<ConfigVertex, ExecutionVertex> created,
                           final DAG<ConfigVertex, MISTEdge> configDag,
                           final ExecutionDag executionDag,
                           final URL[] urls,
                           final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final ExecutionVertex currExecutionVertex;
    final DAG<ExecutionVertex, MISTEdge> dag = executionDag.getDag();
    if (created.get(currVertex) == null) {
      currExecutionVertex = executionVertexGenerator.generate(currVertex, urls, classLoader);
      created.put(currVertex, currExecutionVertex);
      dag.addVertex(currExecutionVertex);
      // do dfs creation
      for (final Map.Entry<ConfigVertex, MISTEdge> edges : configDag.getEdges(currVertex).entrySet()) {
        final ConfigVertex childVertex = edges.getKey();
        final MISTEdge edge = edges.getValue();
        dfsCreation(currExecutionVertex, edge, childVertex, created, configDag, executionDag, urls, classLoader);
      }
    } else {
      currExecutionVertex = created.get(currVertex);
    }
    dag.addEdge(parent, currExecutionVertex, parentEdge);
  }

  /**
   * This generates the logical and physical plan from the avro operator chain dag.
   * Note that the avro operator chain dag is already partitioned,
   * so we need to rewind the partition to generate the logical dag.
   * @param configDag the tuple of queryId and avro operator chain dag
   * @return the logical and execution dag
   */
  @Override
  public ExecutionDag generate(final DAG<ConfigVertex, MISTEdge> configDag,
                               final List<String> jarFilePaths)
      throws IOException, ClassNotFoundException {
    // For execution dag
    final ExecutionDag executionDag = new ExecutionDag(new AdjacentListConcurrentMapDAG<>());

    // Get a class loader
    final URL[] urls = SerializeUtils.getJarFileURLs(jarFilePaths);
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    final Map<ConfigVertex, ExecutionVertex> created = new HashMap<>(configDag.numberOfVertices());
    for (final ConfigVertex source : configDag.getRootVertices()) {
      final ExecutionVertex currExecutionVertex =
          executionVertexGenerator.generate(source, urls, classLoader);
      executionDag.getDag().addVertex(currExecutionVertex);
      created.put(source, currExecutionVertex);
      // do dfs creation
      for (final Map.Entry<ConfigVertex, MISTEdge> edges : configDag.getEdges(source).entrySet()) {
        final ConfigVertex childVertex = edges.getKey();
        final MISTEdge edge = edges.getValue();
        dfsCreation(currExecutionVertex, edge, childVertex, created, configDag, executionDag, urls, classLoader);
      }
    }

    return executionDag;
  }
}