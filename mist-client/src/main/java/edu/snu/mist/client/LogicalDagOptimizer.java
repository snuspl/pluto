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
package edu.snu.mist.client;

import edu.snu.mist.client.datastreams.ContinuousStreamImpl;
import edu.snu.mist.client.datastreams.MISTStream;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * This class implements a logical DAG optimizer.
 * Through this optimizer, a few DAG optimization techniques will be applied to the logical DAG in client-side.
 * TODO: [MIST-452] (Minor) handle corner case in conditional branch API
 */
public final class LogicalDagOptimizer {

  /**
   * The logical DAG of a query to getOptimizedDAG.
   */
  private final DAG<MISTStream, MISTEdge> dag;

  public LogicalDagOptimizer(final DAG<MISTStream, MISTEdge> dag) {
    this.dag = dag;
  }

  /**
   * Apply optimization techniques to the logical DAG.
   * @return the optimized DAG
   */
  public DAG<MISTStream, MISTEdge> getOptimizedDAG() {
    // check visited vertices
    final Set<MISTStream> visited = new HashSet<>();

    // it traverses the DAG of operators in DFS order
    // from the root operators which are following sources.
    for (final MISTStream source : dag.getRootVertices()) {
      final Map<MISTStream, MISTEdge> rootEdges = dag.getEdges(source);
      visited.add(source);
      for (final MISTStream nextVertex : rootEdges.keySet()) {
        optimizeSubDag(nextVertex, visited);
      }
    }
    return dag;
  }

  /**
   * Obtimize the operators and sinks recursively (DFS order) according to the mechanism.
   * @param currVertex  current vertex
   * @param visited visited vertices
   */
  private void optimizeSubDag(final MISTStream currVertex,
                              final Set<MISTStream> visited) {
    if (!visited.contains(currVertex)) {
      visited.add(currVertex);
      final Map<MISTStream, MISTEdge> edges = dag.getEdges(currVertex);

      // checking whether there is any conditionally branching edge diverged from current vertex
      if (!(currVertex instanceof ContinuousStreamImpl) ||
          ((ContinuousStreamImpl) currVertex).getCondBranchCount() == 0) {
        // current vertex is not a continuous stream or this edge is an ordinary (non-branch) edge
        for (final MISTStream nextVertex : edges.keySet()) {
          optimizeSubDag(nextVertex, visited);
        }
      } else {
        // current vertex has some conditionally branching edges
        final Map<Integer, ContinuousStreamImpl> branchStreams = new HashMap<>();

        // gather the branching streams
        for (final MISTStream nextVertex : edges.keySet()) {
          if (nextVertex instanceof ContinuousStreamImpl) {
            final ContinuousStreamImpl contNextVertex = (ContinuousStreamImpl) nextVertex;
            if (contNextVertex.getBranchIndex() > 0) {
              // this edge is a conditionally branching edge
              branchStreams.put(contNextVertex.getBranchIndex(), contNextVertex);
            }
          }
          optimizeSubDag(nextVertex, visited);
        }

        // gather condition udfs from each branch stream
        final List<String> udfs = new ArrayList<>(branchStreams.size());
        for (int i = 1; i <= branchStreams.size(); i++) {
          final ContinuousStreamImpl branchStream = branchStreams.get(i);
          final Map<String, String> conf = branchStream.getConfiguration();
          udfs.add(conf.get(ConfKeys.OperatorConf.UDF_STRING.name()));
        }

        // create a new conditional branch vertex to unify these branch streams
        final Map<String, String> opConf = new HashMap<>();
        try {
          opConf.put(ConfKeys.ConditionalBranchOperator.UDF_LIST_STRING.name(),
              SerializeUtils.serializeToString((Serializable)udfs));
        } catch (final IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        final ContinuousStreamImpl unifiedBranchStream = new ContinuousStreamImpl(dag, opConf);
        dag.addVertex(unifiedBranchStream);
        dag.addEdge(currVertex, unifiedBranchStream, new MISTEdge(Direction.LEFT));

        // merging all the branching vertices from the current vertex into a single conditional branch vertex
        for (final ContinuousStreamImpl branchStream : branchStreams.values()) {
          final List<MISTStream> branchDownStreams = new ArrayList<>();
          for (final Map.Entry<MISTStream, MISTEdge> edgeFromBranch : dag.getEdges(branchStream).entrySet()) {
            final MISTStream branchDownStream = edgeFromBranch.getKey();
            branchDownStreams.add(branchDownStream);
            dag.addEdge(unifiedBranchStream, branchDownStream,
                new MISTEdge(edgeFromBranch.getValue().getDirection(), branchStream.getBranchIndex()));
          }
          // to prevent the concurrent map modification, remove the edges from downStream separately
          for (final MISTStream branchDownStream : branchDownStreams) {
            dag.removeEdge(branchStream, branchDownStream);
          }
          dag.removeEdge(currVertex, branchStream);
          dag.removeVertex(branchStream);
        }
      }
    }
  }
}
