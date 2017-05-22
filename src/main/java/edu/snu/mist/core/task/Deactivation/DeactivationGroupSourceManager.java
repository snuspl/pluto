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
package edu.snu.mist.core.task.Deactivation;

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.operators.StateHandler;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.stores.AvroExecutionVertexStore;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.*;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;

import javax.inject.Inject;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a per-group implementation of the GroupSourceManager interface when query deactivation is enabled.
 */
public final class DeactivationGroupSourceManager implements GroupSourceManager {

  private static final Logger LOG = Logger.getLogger(DeactivationGroupSourceManager.class.getName());

  /**
   * Operator chain manager.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A configuration serializer.
   */
  private final AvroConfigurationSerializer avroConfigurationSerializer;

  /**
   * A ClassLoader Provider.
   */
  private final ClassLoaderProvider classLoaderProvider;

  /**
   * The group id for this GroupSourceManager.
   */
  private final String groupId;

  /**
   * This map contains the active ExecutionVertices.
   * The key is the source name, and the value is the ExecutionVertex.
   * Becauses sources are never truly deactivated, they are not removed from this map upon deactivation.
   */
  private final Map<String, ExecutionVertex> activeExecutionVertexIdMap;

  /**
   * Physical execution dags within the group.
   */
  private final ExecutionDags executionDags;

  /**
   * A store that stores deactivated ExecutionVertices.
   */
  private final AvroExecutionVertexStore avroExecutionVertexStore;

  /**
   * The map for the reference to the ExecutionDag that corresponds to the deactivated source's id.
   * This is used to find the dag that the deactivated source originally belonged to.
   */
  private final Map<String, DAG<ExecutionVertex, MISTEdge>> deactivatedSourceDagMap;

  @Inject
  private DeactivationGroupSourceManager(@Parameter(GroupId.class) final String groupId,
                                         final OperatorChainManager operatorChainManager,
                                         final QueryInfoStore planStore,
                                         final AvroExecutionVertexStore avroExecutionVertexStore,
                                         final AvroConfigurationSerializer avroConfigurationSerializer,
                                         final ExecutionDags executionDags,
                                         final ClassLoaderProvider classLoaderProvider) {
    this.groupId = groupId;
    this.operatorChainManager = operatorChainManager;
    this.planStore = planStore;
    this.avroExecutionVertexStore = avroExecutionVertexStore;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.activeExecutionVertexIdMap = new HashMap<>();
    this.executionDags = executionDags;
    this.deactivatedSourceDagMap = new HashMap<>();
    this.classLoaderProvider = classLoaderProvider;
  }

  @Override
  public void initializeActiveExecutionVertexIdMap() {
    for (final DAG<ExecutionVertex, MISTEdge> executionDag : executionDags.values()) {
      final Set<String> visitedVertexIds = new HashSet<>();
      for (final ExecutionVertex root : executionDag.getRootVertices()) {
        addActiveExecutionVertices(executionDag, root, visitedVertexIds);
      }
    }
  }

  /**
   * The function for recursively doing DFS and adding ActiveExecutionVertices.
   */
  private void addActiveExecutionVertices(final DAG<ExecutionVertex, MISTEdge> dag,
                                          final ExecutionVertex currentVertex,
                                          final Set<String> visitedVertexIds) {
    final String currentVertexId = currentVertex.getIdentifier();
    if (!visitedVertexIds.contains(currentVertexId)) {
      visitedVertexIds.add(currentVertexId);
      activeExecutionVertexIdMap.put(currentVertexId, currentVertex);
      for (final ExecutionVertex executionVertex : dag.getEdges(currentVertex).keySet()) {
          addActiveExecutionVertices(dag, executionVertex, visitedVertexIds);
      }
    }
  }

  @Override
  public void deactivateBasedOnSource(final String queryId, final String sourceId)
      throws AvroRemoteException {
    try {
      final Tuple<ExecutionVertex, DAG<ExecutionVertex, MISTEdge>> tuple = findSourceAndDag(sourceId);
      final ExecutionVertex root = tuple.getKey();
      final DAG<ExecutionVertex, MISTEdge> executionDag = tuple.getValue();
      if (!(root == null || executionDag == null)) {
        synchronized (executionDag) {
          final Map<ExecutionVertex, MISTEdge> needWatermarkVertices = new HashMap<>();
          // Update physical plan and save the deactivated execution vertices.
          dfsAndSaveExecutionVertices(executionDag, root, needWatermarkVertices);
          // Set the source's output emitter to the active vertices that need watermarks to continue processing.
          ((PhysicalSource) root).setOutputEmitter(
              new DeactivatedSourceOutputEmitter(queryId, needWatermarkVertices, this, (PhysicalSource) root));
          // Change the PhysicalSource's state to be deactivated.
          deactivatedSourceDagMap.put(sourceId, executionDag);
        }
      } else {
        LOG.log(Level.INFO, "Could not find the source to deactivate.");
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Find the source and the dag that the sourceId belongs to.
   * @param sourceId the source id of the source to be deactivated
   * @return Tuple of the source and the dag
   */
  private Tuple<ExecutionVertex, DAG<ExecutionVertex, MISTEdge>> findSourceAndDag(final String sourceId) {
    // Search executionDags to find the source to deactivate.
    for (final DAG<ExecutionVertex, MISTEdge> dag : executionDags.values()) {
      for (final ExecutionVertex root : dag.getRootVertices()) {
        if (root.getIdentifier().equals(sourceId)) {
          return new Tuple<>(root, dag);
        }
      }
    }
    LOG.log(Level.FINE, "The sourceId {0} could not be found within group {1}",
        new Object[] {groupId, sourceId});
    return new Tuple<>(null, null);
  }

  /**
   * Recursively conducts DFS throughout the executionDag.
   * It saves the ExecutionVertices which have inDegree of 0.
   */
  private void dfsAndSaveExecutionVertices(final DAG<ExecutionVertex, MISTEdge> executionDag,
                                           final ExecutionVertex currentVertex,
                                           final Map<ExecutionVertex, MISTEdge> needWatermarkVertices)
      throws IOException {
    if (activeExecutionVertexIdMap.containsKey(currentVertex.getIdentifier())) {
      final Map<ExecutionVertex, MISTEdge> nextVertices = executionDag.getEdges(currentVertex);
      try {
        switch (currentVertex.getType()) {
          case SOURCE: {
            // For the outgoingEdges of the source(to be deactivated), save the index and direction information.
            final Map<ExecutionVertex, MISTEdge> outgoingEdges = executionDag.getEdges(currentVertex);
            final Map<String, AvroMISTEdge> avroOutgoingEdges = new HashMap<>();
            for (final ExecutionVertex executionVertex : outgoingEdges.keySet()) {
              final int index = outgoingEdges.get(executionVertex).getIndex();
              final Direction direction = outgoingEdges.get(executionVertex).getDirection();
              final AvroMISTEdge.Builder avroMISTEdgeBuilder = AvroMISTEdge.newBuilder()
                  .setIndex(index)
                  .setDirection(direction == Direction.LEFT ? "LEFT" : "RIGHT");
              avroOutgoingEdges.put(executionVertex.getIdentifier(), avroMISTEdgeBuilder.build());
            }
            final AvroPhysicalSourceOutgoingEdgesInfo.Builder avroPhysicalSourceOutgoingEdgesInfoBuilder =
                AvroPhysicalSourceOutgoingEdgesInfo.newBuilder()
                    .setAvroPhysicalSourceId(currentVertex.getIdentifier())
                    .setOutgoingEdges(avroOutgoingEdges);
            final AvroPhysicalSourceOutgoingEdgesInfo avroPhysicalSourceOutgoingEdgesInfo =
                avroPhysicalSourceOutgoingEdgesInfoBuilder.build();
            // Save the OutgoingEdgesInfo of the source that is being deactivated.
            avroExecutionVertexStore.saveAvroPhysicalSourceOutgoingEdgesInfo(new Tuple<>(currentVertex.getIdentifier(),
                avroPhysicalSourceOutgoingEdgesInfo));
            // Remove the source from the executionDag.
            executionDag.removeVertex(currentVertex);
            break;
          }
          case OPERATOR_CHAIN: {
            // Save the physicalVertices if the operator chain is dependent on only the source to be deactivated.
            if (executionDag.getInDegree(currentVertex) == 0) {
              avroExecutionVertexStore.saveAvroPhysicalOperatorChain(
                  new Tuple<>(currentVertex.getIdentifier(), getAvroPhysicalOperatorChain((OperatorChain) currentVertex,
                      executionDag.getEdges(currentVertex))));
              activeExecutionVertexIdMap.remove(currentVertex.getIdentifier());
              // Remove the vertex from executionDag.
              executionDag.removeVertex(currentVertex);
              // Set the OutputEmitter to null.
              ((OperatorChain) currentVertex).setOutputEmitter(null);
              // Remove the chain from the operatorChainManager.
              operatorChainManager.delete((OperatorChain) currentVertex);
            }
            break;
          }
          case SINK: {
            activeExecutionVertexIdMap.remove(currentVertex.getIdentifier());
            // Remove the vertex from executionDag.
            executionDag.removeVertex(currentVertex);
            // Closes the channel of Sink.
            final Sink sink = ((PhysicalSink) currentVertex).getSink();
            try {
              sink.close();
            } catch (final Exception e) {
              e.printStackTrace();
            }
            break;
          }
          default: {
            throw new RuntimeException("Invalid Vertex Type: " + currentVertex.getType());
          }
        }
      } catch (RuntimeException e) {
        LOG.log(Level.SEVERE, "{0}", new Object[]{e.getMessage()});
      }
      // If the current vertex is not a sink, continue to do DFS.
      if (currentVertex.getType() != ExecutionVertex.Type.SINK) {
        for (final Map.Entry<ExecutionVertex, MISTEdge> nextVertexAndEdge : nextVertices.entrySet()) {
          final ExecutionVertex nextVertex = nextVertexAndEdge.getKey();
          // If nextVertex is the first OperatorChain that still has an active incoming edge,
          // DFS must be stopped and vertex must be saved in needWatermarkVertices for the DeactivatedSourceEmitter.
          if (executionDag.getInDegree(nextVertex) >= 1) {
            needWatermarkVertices.put(nextVertex, nextVertexAndEdge.getValue());
          } else {
            dfsAndSaveExecutionVertices(executionDag, nextVertex, needWatermarkVertices);
          }
        }
      }
    }
  }

  /**
   * Return a serialized AvroPhysicalOperatorChain from a operatorChain.
   * @param operatorChain
   * @param outgoingEdges
   * @return AvroPhysicalOperatorChain
   */
  private AvroPhysicalOperatorChain getAvroPhysicalOperatorChain(final OperatorChain operatorChain,
                                                                 final Map<ExecutionVertex, MISTEdge> outgoingEdges)
      throws IOException {
    final AvroPhysicalOperatorChain.Builder avroPhysicalOperatorChainBuilder = AvroPhysicalOperatorChain.newBuilder();
    final List<AvroPhysicalOperatorData> avroOperatorDataList = new LinkedList<>();
    for (int i = 0; i < operatorChain.size(); i++) {
      final PhysicalOperator physicalOperator = operatorChain.get(i);
      final AvroPhysicalOperatorData.Builder avroPhysicalOperatorDataBuilder = AvroPhysicalOperatorData.newBuilder()
          .setId(physicalOperator.getId())
          .setAvroPhysicalOperatorState(getPhysicalOperatorSerializedState(physicalOperator))
          .setConfigurations(physicalOperator.getConfiguration());
      avroOperatorDataList.add(avroPhysicalOperatorDataBuilder.build());
    }
    final Map<String, AvroMISTEdge> avroOutgoingEdges = new HashMap<>();
    for (final Map.Entry<ExecutionVertex, MISTEdge> entry : outgoingEdges.entrySet()) {
      final int index = entry.getValue().getIndex();
      final Direction direction = entry.getValue().getDirection();
      final AvroMISTEdge.Builder avroMISTEdgeBuilder = AvroMISTEdge.newBuilder()
          .setIndex(index)
          .setDirection(direction == Direction.LEFT ? "LEFT" : "RIGHT");
      avroOutgoingEdges.put(entry.getKey().getIdentifier(), avroMISTEdgeBuilder.build());
    }
    avroPhysicalOperatorChainBuilder
        .setAvroPhysicalOperatorChainId(operatorChain.getIdentifier())
        .setAvroPhysicalOperatorDataList(avroOperatorDataList)
        .setOutgoingEdges(avroOutgoingEdges);
    return avroPhysicalOperatorChainBuilder.build();
  }

  /**
   * This is an auxiliary method for the getAvroPhysicalOperatorChain method.
   * It gets the serialized state of the current physical operator.
   */
  private Map<String, Object> getPhysicalOperatorSerializedState(final PhysicalOperator physicalOperator) {
    final Operator operator = physicalOperator.getOperator();
    if (operator instanceof StateHandler) {
      return StateSerializer.serializeStateMap(((StateHandler) operator).getOperatorState());
    } else {
      // In case the operator is a stateless operator, return an empty map.
      return new HashMap<>();
    }
  }

  @Override
  public void activateBasedOnSource(final String queryId, final String sourceId)
      throws AvroRemoteException, MalformedURLException {
    try {
      final Tuple<URL[], ClassLoader> urlsAndClassLoader = getURLsAndClassLoader(queryId);
      if (activeExecutionVertexIdMap.get(sourceId) == null) {
        LOG.log(Level.INFO, "Wrong sourceId has been input. It does not exist.");
      } else {
        final DAG<ExecutionVertex, MISTEdge> executionDag = deactivatedSourceDagMap.get(sourceId);
        if (executionDag == null) {
          LOG.log(Level.INFO, "Source is already activated");
        } else {
          synchronized (executionDag) {
            dfsAndLoadExecutionVertices(urlsAndClassLoader, executionDag, sourceId);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets the URLs and ClassLoader from the DagGenerator based on the queryId.
   * @param queryId
   * @return a tuple containing URLs and ClassLoader for the queryId
   * @throws MalformedURLException, IOException
   */
  private Tuple<URL[], ClassLoader> getURLsAndClassLoader(final String queryId)
      throws IOException {
    final URL[] urls = SerializeUtils.getJarFileURLs(planStore.load(queryId).getJarFilePaths());
    return new Tuple<>(urls, classLoaderProvider.newInstance(urls));
  }

  /**
   * Recursively conducts DFS throughout the executionDag.
   * @param urlsAndClassLoader the urls and ClassLoader for the query that uses the source
   * @param executionDag the dag for the source to be activated
   * @param currentVertexIdToActivate the id of the current executionVertex to be activated
   */
  private void dfsAndLoadExecutionVertices(final Tuple<URL[], ClassLoader> urlsAndClassLoader,
                                           final DAG<ExecutionVertex, MISTEdge> executionDag,
                                           final String currentVertexIdToActivate)
      throws IOException, InjectionException, ClassNotFoundException, InterruptedException {
    // If the current ExecutionVertex is an inactive ExecutionVertex
    if (currentVertexIdToActivate.startsWith("source") ||
        !activeExecutionVertexIdMap.containsKey(currentVertexIdToActivate)) {
      // Get the outgoingEdgeMap holds the edges' indexes and directions for each edge connected to executionVertex.
      final Map<String, AvroMISTEdge> outgoingEdgeMap;
      if (currentVertexIdToActivate.startsWith("source")) {
        final AvroPhysicalSourceOutgoingEdgesInfo avroPhysicalSourceOutgoingEdgesInfo =
            avroExecutionVertexStore.loadAvroPhysicalSourceOutgoingEdgesInfo(currentVertexIdToActivate);
        outgoingEdgeMap = avroPhysicalSourceOutgoingEdgesInfo.getOutgoingEdges();
      } else if (currentVertexIdToActivate.startsWith("opChain")) {
        final AvroPhysicalOperatorChain avroPhysicalOperatorChain =
            avroExecutionVertexStore.loadAvroPhysicalOperatorChain(currentVertexIdToActivate);
        outgoingEdgeMap = avroPhysicalOperatorChain.getOutgoingEdges();
      } else {
        // TODO: [MIST-459] Currently, this part means sink, and it should not be reached.
        outgoingEdgeMap = new HashMap<>();
      }

      // Set up nextOps, which holds the vertices and edges that the current executionVertex(to activate) points to.
      final Map<ExecutionVertex, MISTEdge> nextOps = new HashMap<>();
      for (final Map.Entry<String, AvroMISTEdge> entry : outgoingEdgeMap.entrySet()) {
        final String nextVertexId = entry.getKey();
        dfsAndLoadExecutionVertices(urlsAndClassLoader, executionDag, nextVertexId);
        final AvroMISTEdge avroMISTEdge = entry.getValue();
        final MISTEdge mistEdge = new MISTEdge(
            avroMISTEdge.getDirection().equals("LEFT") ? Direction.LEFT : Direction.RIGHT, avroMISTEdge.getIndex());
        nextOps.put(activeExecutionVertexIdMap.get(nextVertexId), mistEdge);
      }

      // Bring back the ExecutionVertex into the executionDag.
      final ExecutionVertex revivedExecutionVertex;
      if (currentVertexIdToActivate.startsWith("source")) {
        // If the current vertex is a source, just get if from the activeExecutionVertexIdMap.
        revivedExecutionVertex = activeExecutionVertexIdMap.get(currentVertexIdToActivate);
      } else if (currentVertexIdToActivate.startsWith("opChain")) {
        // If the current vertex is an operator chain, it must be revived from the avroExecutionVertexStore.
        revivedExecutionVertex = new DefaultOperatorChainImpl(currentVertexIdToActivate);
        ((OperatorChain) revivedExecutionVertex).setOperatorChainManager(operatorChainManager);
        final AvroPhysicalOperatorChain avroPhysicalOperatorChain =
            avroExecutionVertexStore.loadAvroPhysicalOperatorChain(currentVertexIdToActivate);
        for (final AvroPhysicalOperatorData avroPhysicalOperatorData :
            avroPhysicalOperatorChain.getAvroPhysicalOperatorDataList()) {
          final Configuration conf = avroConfigurationSerializer.fromString(
              avroPhysicalOperatorData.getConfigurations(), new ClassHierarchyImpl(urlsAndClassLoader.getKey()));
          // Create operator from conf and classloader.
          final ClassLoader classLoader = urlsAndClassLoader.getValue();
          final Injector injector = Tang.Factory.getTang().newInjector(conf);
          injector.bindVolatileInstance(ClassLoader.class, classLoader);
          final Operator operator = injector.getInstance(Operator.class);
          // Create the physicalOperator and set the state.
          final PhysicalOperator physicalOperator = new DefaultPhysicalOperatorImpl(avroPhysicalOperatorData.getId(),
              avroPhysicalOperatorData.getConfigurations(), operator, (OperatorChain) revivedExecutionVertex);
          setPhysicalOperatorState(physicalOperator, avroPhysicalOperatorData.getAvroPhysicalOperatorState());
          // Add the operator to the OperatorChain.
          ((OperatorChain) revivedExecutionVertex).insertToTail(physicalOperator);
        }
      } else if (currentVertexIdToActivate.startsWith("sink")) {
        // The corresponding sink is already closed, and cannot be reopened as of now.
        // TODO: [MIST-459] Policy of Sink closing
        revivedExecutionVertex = null;
        LOG.log(Level.INFO, "Sink is already closed.");
      } else {
        throw new RuntimeException("A wrong id for the ExecutionVertex. It is: " + currentVertexIdToActivate);
      }

      // Setting the output emitter for the revived Execution Vertex
      switch (revivedExecutionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource) revivedExecutionVertex;
          // Set output emitter
          source.setOutputEmitter(new SourceOutputEmitter<>(nextOps));
          // Add vertex and edges to executionDag
          executionDag.addVertex(revivedExecutionVertex);
          for (final ExecutionVertex adjacentVertex : nextOps.keySet()) {
            executionDag.addVertex(adjacentVertex);
            executionDag.addEdge(revivedExecutionVertex, adjacentVertex, nextOps.get(adjacentVertex));
          }
          LOG.log(Level.INFO, revivedExecutionVertex.getIdentifier() + " has been activated.");
          activeExecutionVertexIdMap.put(currentVertexIdToActivate, revivedExecutionVertex);
          break;
        }
        case OPERATOR_CHAIN: {
          final OperatorChain operatorChain = (OperatorChain) revivedExecutionVertex;
          operatorChainManager.insert(operatorChain);
          // Set output emitter
          operatorChain.setOutputEmitter(new OperatorOutputEmitter(nextOps));
          // Add vertex and edges to executionDag.
          executionDag.addVertex(revivedExecutionVertex);
          for (final ExecutionVertex adjacentVertex : nextOps.keySet()) {
            executionDag.addVertex(adjacentVertex);
            executionDag.addEdge(revivedExecutionVertex, adjacentVertex, nextOps.get(adjacentVertex));
          }
          LOG.log(Level.INFO, revivedExecutionVertex.getIdentifier() + " has been activated.");
          activeExecutionVertexIdMap.put(currentVertexIdToActivate, revivedExecutionVertex);
          break;
        }
        case SINK: {
          // TODO: [MIST-459] Policy of Sink closing
          break;
        }
        default: {
          throw new RuntimeException("Invalid vertex type: " + revivedExecutionVertex.getType());
        }
      }
    }
  }

  /**
   * Set the state of the PhysicalOperator.
   */
  private void setPhysicalOperatorState(final PhysicalOperator physicalOperator,
                                        final Map<String, Object> loadedSerializedState) {
    final Operator operator = physicalOperator.getOperator();
    if (operator instanceof StateHandler) {
      ((StateHandler) operator).setState(StateSerializer.deserializeStateMap(loadedSerializedState));
    }
    // In case the operator is a stateless operator, do nothing.
  }

  @Override
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }
}