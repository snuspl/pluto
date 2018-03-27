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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This starter tries to merges the submitted dag with the currently running dag.
 * When a query is submitted, this starter first finds mergeable execution dags.
 * After that, it merges them with the submitted query.
 */
public final class ImmediateQueryMergingStarter implements QueryStarter {

  /**
   * An algorithm for finding the sub-dag between the execution and submitted dag.
   */
  private final CommonSubDagFinder commonSubDagFinder;

  /**
   * Map that has the source conf as a key and the physical execution dag as a value.
   */
  private final SrcAndDagMap<Map<String, String>> srcAndDagMap;

  /**
   * The map that has the query id as a key and its configuration dag as a value.
   */
  private final QueryIdConfigDagMap queryIdConfigDagMap;

  /**
   * Physical execution dags.
   */
  private final ExecutionDags executionDags;

  /**
   * Class loader provider.
   */
  private final ClassLoaderProvider classLoaderProvider;

  /**
   * Execution vertex generator.
   */
  private final ExecutionVertexGenerator executionVertexGenerator;

  /**
   * A map that has config vertex as a key and the corresponding execution vertex as a value.
   */
  private final ConfigExecutionVertexMap configExecutionVertexMap;

  /**
   * A map that has an execution vertex as a key and the reference count number as a value.
   * The reference count number represents how many queries are sharing the execution vertex.
   */
  private final ExecutionVertexCountMap executionVertexCountMap;

  /**
   * A map that has an execution vertex as a key and the dag that contains its vertex as a value.
   */
  private final ExecutionVertexDagMap executionVertexDagMap;

  /**
   * The list of jar file paths.
   */
  private final List<String> groupJarFilePaths;

  @Inject
  private ImmediateQueryMergingStarter(final CommonSubDagFinder commonSubDagFinder,
                                       final SrcAndDagMap<Map<String, String>> srcAndDagMap,
                                       final QueryIdConfigDagMap queryIdConfigDagMap,
                                       final ExecutionDags executionDags,
                                       final ConfigExecutionVertexMap configExecutionVertexMap,
                                       final ExecutionVertexCountMap executionVertexCountMap,
                                       final ClassLoaderProvider classLoaderProvider,
                                       final ExecutionVertexGenerator executionVertexGenerator,
                                       final ExecutionVertexDagMap executionVertexDagMap) {
    this.commonSubDagFinder = commonSubDagFinder;
    this.srcAndDagMap = srcAndDagMap;
    this.queryIdConfigDagMap = queryIdConfigDagMap;
    this.executionDags = executionDags;
    this.classLoaderProvider = classLoaderProvider;
    this.executionVertexGenerator = executionVertexGenerator;
    this.configExecutionVertexMap = configExecutionVertexMap;
    this.executionVertexCountMap = executionVertexCountMap;
    this.executionVertexDagMap = executionVertexDagMap;
    this.groupJarFilePaths = new CopyOnWriteArrayList<>();
  }

  @Override
  public synchronized void start(final String queryId,
                                 final Query query,
                                 final DAG<ConfigVertex, MISTEdge> submittedDag,
                                 final List<String> jarFilePaths) throws IOException, ClassNotFoundException {

    queryIdConfigDagMap.put(queryId, submittedDag);

    // Get a class loader
    final URL[] urls = SerializeUtils.getJarFileURLs(jarFilePaths);
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    synchronized (groupJarFilePaths) {
      if (jarFilePaths != null && jarFilePaths.size() != 0) {
        groupJarFilePaths.addAll(jarFilePaths);
      }
    }

    // Synchronize the execution dags to evade concurrent modifications
    // TODO:[MIST-590] We need to improve this code for concurrent modification
    synchronized (srcAndDagMap) {
      // Find mergeable DAGs from the execution dags
      final Map<Map<String, String>, ExecutionDag> mergeableDags = findMergeableDags(submittedDag);

      // Exit the merging process if there is no mergeable dag
      if (mergeableDags.size() == 0) {
        final ExecutionDag executionDag = generate(submittedDag, urls, classLoader);
        // Set up the output emitters of the submitted DAG
        QueryStarterUtils.setUpOutputEmitters(executionDag, query);

        for (final ExecutionVertex source : executionDag.getDag().getRootVertices()) {
          // Start the source
          final PhysicalSource src = (PhysicalSource) source;
          srcAndDagMap.put(src.getConfiguration(), executionDag);
          src.start();
        }

        // Update the execution dag of the execution vertex
        for (final ExecutionVertex ev : executionDag.getDag().getVertices()) {
          executionVertexDagMap.put(ev, executionDag);
        }

        executionDags.add(executionDag);
        return;
      }

      // If there exist mergeable execution dags,
      // Select the DAG that has the largest number of vertices and merge all of the DAG to the largest DAG
      final ExecutionDag sharableExecutionDag = selectLargestDag(mergeableDags.values());
      // Merge all dag into one execution dag
      // We suppose that all of the dags has no same vertices
      for (final ExecutionDag executionDag : mergeableDags.values()) {
        if (executionDag != sharableExecutionDag) {
          GraphUtils.copy(executionDag.getDag(), sharableExecutionDag.getDag());
          // Remove the execution dag
          executionDags.remove(executionDag);

          // Update all of the sources in the execution Dag
          for (final ExecutionVertex source : executionDag.getDag().getRootVertices()) {
            srcAndDagMap.replace(((PhysicalSource) source).getConfiguration(), sharableExecutionDag);
          }

          // Update the execution dag of the execution vertex
          for (final ExecutionVertex ev : executionDag.getDag().getVertices()) {
            executionVertexDagMap.put(ev, sharableExecutionDag);
          }
        }
      }

      // After that, find the sub-dag between the sharableDAG and the submitted dag
      final Map<ConfigVertex, ExecutionVertex> subDagMap =
          commonSubDagFinder.findSubDag(sharableExecutionDag, submittedDag);

      // After that, we should merge the sharable dag with the submitted dag
      // and update the output emitters of the sharable dag
      final Set<ConfigVertex> visited = new HashSet<>(submittedDag.numberOfVertices());
      for (final ConfigVertex source : submittedDag.getRootVertices()) {
        // dfs search
        ExecutionVertex executionVertex;
        if (subDagMap.get(source) == null) {
          executionVertex = executionVertexGenerator.generate(source, urls, classLoader);
          sharableExecutionDag.getDag().addVertex(executionVertex);
          executionVertexCountMap.put(executionVertex, 1);
          executionVertexDagMap.put(executionVertex, sharableExecutionDag);
        } else {
          executionVertex = subDagMap.get(source);
          executionVertexCountMap.put(executionVertex, executionVertexCountMap.get(executionVertex) + 1);
        }
        configExecutionVertexMap.put(source, executionVertex);

        for (final Map.Entry<ConfigVertex, MISTEdge> child : submittedDag.getEdges(source).entrySet()) {
          dfsMerge(subDagMap, visited, executionVertex,
              child.getValue(), child.getKey(), sharableExecutionDag, submittedDag, urls, classLoader);
        }
      }

      // If there are sources that are not shared, start them
      for (final ConfigVertex source : submittedDag.getRootVertices()) {
        if (!subDagMap.containsKey(source)) {
          srcAndDagMap.put(source.getConfiguration(), sharableExecutionDag);
          ((PhysicalSource)configExecutionVertexMap.get(source)).start();
        }
      }
    }
  }

  /**
   * Create the execution dag in dfs order.
   */
  private void dfsCreation(final ExecutionVertex parent,
                           final MISTEdge parentEdge,
                           final ConfigVertex currVertex,
                           final Map<ConfigVertex, ExecutionVertex> created,
                           final DAG<ConfigVertex, MISTEdge> configDag,
                           final ExecutionDag executionDag,
                           final URL[] urls,
                           final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final ExecutionVertex currExecutionVertex;
    if (created.get(currVertex) == null) {
      currExecutionVertex = executionVertexGenerator.generate(currVertex, urls, classLoader);
      created.put(currVertex, currExecutionVertex);
      executionVertexCountMap.put(currExecutionVertex, 1);
      executionVertexDagMap.put(currExecutionVertex, executionDag);
      executionDag.getDag().addVertex(currExecutionVertex);
      // do dfs creation
      for (final Map.Entry<ConfigVertex, MISTEdge> edges : configDag.getEdges(currVertex).entrySet()) {
        final ConfigVertex childVertex = edges.getKey();
        final MISTEdge edge = edges.getValue();
        dfsCreation(currExecutionVertex, edge, childVertex, created, configDag, executionDag, urls, classLoader);
      }
    } else {
      currExecutionVertex = created.get(currVertex);
    }
    configExecutionVertexMap.put(currVertex, currExecutionVertex);
    executionDag.getDag().addEdge(parent, currExecutionVertex, parentEdge);
  }

  /**
   * This generates a new execution dag from the configuration dag.
   */
  private ExecutionDag generate(final DAG<ConfigVertex, MISTEdge> configDag,
                                final URL[] urls,
                                final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    // For execution dag
    final ExecutionDag executionDag = new ExecutionDag(new AdjacentListConcurrentMapDAG<>());

    final Map<ConfigVertex, ExecutionVertex> created = new HashMap<>(configDag.numberOfVertices());
    for (final ConfigVertex source : configDag.getRootVertices()) {
      final ExecutionVertex currExecutionVertex = executionVertexGenerator.generate(source, urls, classLoader);
      created.put(source, currExecutionVertex);
      configExecutionVertexMap.put(source, currExecutionVertex);
      executionVertexCountMap.put(currExecutionVertex, 1);
      executionVertexDagMap.put(currExecutionVertex, executionDag);
      executionDag.getDag().addVertex(currExecutionVertex);
      // do dfs creation
      for (final Map.Entry<ConfigVertex, MISTEdge> edges : configDag.getEdges(source).entrySet()) {
        final ConfigVertex childVertex = edges.getKey();
        final MISTEdge edge = edges.getValue();
        dfsCreation(currExecutionVertex, edge, childVertex, created, configDag, executionDag, urls, classLoader);
      }
    }

    return executionDag;
  }

  /**
   * This function merges the submitted dag with the execution dag by traversing the dags in DFS order.
   * @param subDagMap a map that contains vertices of the sub-dag
   * @param visited a set that holds the visited vertices
   * @param parent parent (execution) vertex of the current vertex
   * @param parentEdge parent edge of the current vertex
   * @param currentVertex current (config) vertex
   * @param executionDag execution dag that merges the submitted dag
   * @param submittedDag submitted dag
   * @param urls urls for creating execution vertices
   * @param classLoader classLoader for creating execution vertices
   */
  private void dfsMerge(final Map<ConfigVertex, ExecutionVertex> subDagMap,
                        final Set<ConfigVertex> visited,
                        final ExecutionVertex parent,
                        final MISTEdge parentEdge,
                        final ConfigVertex currentVertex,
                        final ExecutionDag executionDag,
                        final DAG<ConfigVertex, MISTEdge> submittedDag,
                        final URL[] urls,
                        final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    if (visited.contains(currentVertex)) {
      executionDag.getDag().addEdge(parent, configExecutionVertexMap.get(currentVertex), parentEdge);
      return;
    }

    // Add to the visited set
    visited.add(currentVertex);

    // Traverse in DFS order
    ExecutionVertex correspondingVertex = subDagMap.get(currentVertex);

    if (correspondingVertex == null) {
      // it is not shared, so we need to create it
      correspondingVertex = executionVertexGenerator.generate(currentVertex, urls, classLoader);
      executionDag.getDag().addVertex(correspondingVertex);
      executionVertexCountMap.put(correspondingVertex, 1);
      executionVertexDagMap.put(correspondingVertex, executionDag);
    } else {
      // It is shared, so increase the reference count
      executionVertexCountMap.put(correspondingVertex, executionVertexCountMap.get(correspondingVertex) + 1);
    }

    configExecutionVertexMap.put(currentVertex, correspondingVertex);

    // Traverse
    boolean outputEmitterUpdateNeeded = false;
    for (final Map.Entry<ConfigVertex, MISTEdge> neighbor : submittedDag.getEdges(currentVertex).entrySet()) {
      final ConfigVertex child = neighbor.getKey();
      if (!subDagMap.containsKey(child)) {
        outputEmitterUpdateNeeded = true;
      }
      dfsMerge(subDagMap, visited, correspondingVertex, neighbor.getValue(),
        child, executionDag, submittedDag, urls, classLoader);
    }

    // [TODO:MIST-527] Integrate ExecutionVertex and PhysicalVertex
    // We need to integrate ExecutionVertex and PhysicalVertex
    // The output emitter of the current vertex of the execution dag needs to be updated
    if (outputEmitterUpdateNeeded) {
      if (correspondingVertex.getType() == ExecutionVertex.Type.SOURCE) {
        final PhysicalSource s = (PhysicalSource) correspondingVertex;
        final SourceOutputEmitter sourceOutputEmitter = s.getSourceOutputEmitter();
        s.setOutputEmitter(new NonBlockingQueueSourceOutputEmitter<>(
            executionDag.getDag().getEdges(correspondingVertex), sourceOutputEmitter.getQuery()));
      } else if (correspondingVertex.getType() == ExecutionVertex.Type.OPERATOR) {
        ((PhysicalOperator)correspondingVertex).getOperator().setOutputEmitter(
            new OperatorOutputEmitter(executionDag.getDag().getEdges(correspondingVertex)));
      }
    }

    executionDag.getDag().addEdge(parent, correspondingVertex, parentEdge);
  }

  /**
   * TODO:[MIST-538] Select a sharable DAG that minimizes merging cost in immediate merging
   * Select one execution dag for merging.
   * @param dags mergeable dags
   * @return a dag where all of the dags will be merged
   */
  private ExecutionDag selectLargestDag(
      final Collection<ExecutionDag> dags) {
    int count = 0;
    ExecutionDag largestExecutionDag = null;
    for (final ExecutionDag executionDag : dags) {
      if (executionDag.getDag().numberOfVertices() > count) {
        count = executionDag.getDag().numberOfVertices();
        largestExecutionDag = executionDag;
      }
    }
    return largestExecutionDag;
  }

  /**
   * Find mergeable dag with the submitted query.
   * @param configDag the configuration dag of the submitted query
   * @return mergeable dags
   */
  private Map<Map<String, String>, ExecutionDag> findMergeableDags(
      final DAG<ConfigVertex, MISTEdge> configDag) {
    final Set<ConfigVertex> sources = configDag.getRootVertices();
    final Map<Map<String, String>, ExecutionDag> mergeableDags = new HashMap<>(sources.size());
    for (final ConfigVertex source : sources) {
      final Map<String, String> srcConf = source.getConfiguration();
      final ExecutionDag executionDag = srcAndDagMap.get(srcConf);
      if (executionDag != null) {
        // Mergeable source
        mergeableDags.put(srcConf, executionDag);
      }
    }
    return mergeableDags;
  }
}
