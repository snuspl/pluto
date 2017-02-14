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

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

/**
 * A default implementation of PlanGenerator.
 */
final class  DefaultPlanGeneratorImpl implements PlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultPlanGeneratorImpl.class.getName());

  private final OperatorIdGenerator operatorIdGenerator;
  private final String tmpFolderPath;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final StringIdentifierFactory identifierFactory;
  private final AvroConfigurationSerializer avroConfigurationSerializer;
  private final PhysicalVertexMap physicalVertexMap;

  @Inject
  private DefaultPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                   @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                   final StringIdentifierFactory identifierFactory,
                                   final ClassLoaderProvider classLoaderProvider,
                                   final AvroConfigurationSerializer avroConfigurationSerializer,
                                   final PhysicalObjectGenerator physicalObjectGenerator,
                                   final PhysicalVertexMap physicalVertexMap) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.tmpFolderPath = tmpFolderPath;
    this.classLoaderProvider = classLoaderProvider;
    this.identifierFactory = identifierFactory;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.physicalObjectGenerator = physicalObjectGenerator;
    this.physicalVertexMap = physicalVertexMap;
  }

  /**
   * This generates the logical and physical plan from the avro logical plan.
   * Note that the avro logical plan is already partitioned,
   * so we need to rewind the partition to generate the logical plan.
   * @param queryIdAndAvroLogicalPlan the tuple of queryId and avro logical plan
   * @return the logical and physical plan
   */
  @SuppressWarnings("unchecked")
  @Override
  public LogicalAndPhysicalPlan generate(
      final Tuple<String, AvroLogicalPlan> queryIdAndAvroLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InjectionException {
    final AvroLogicalPlan avroLogicalPlan = queryIdAndAvroLogicalPlan.getValue();
    // For physical plan
    final List<PhysicalVertex> deserializedVertices = new ArrayList<>(avroLogicalPlan.getAvroVertices().size());
    final DAG<PhysicalVertex, Tuple<Direction, Integer>> physicalDAG = new AdjacentListDAG<>();
    // This is for logical plan
    final List<List<LogicalVertex>> logicalVertices = new ArrayList<>(avroLogicalPlan.getAvroVertices().size());
    final DAG<LogicalVertex, Direction> logicalDAG = new AdjacentListDAG<>();

    // Get a class loader
    final URL[] urls = SerializeUtils.getURLs(avroLogicalPlan.getJarFilePaths());
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : avroLogicalPlan.getAvroVertices()) {
      switch (avroVertexChain.getAvroVertexChainType()) {
        case SOURCE: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
              new ClassHierarchyImpl(urls));
          // Create an event generator
          final EventGenerator eventGenerator = physicalObjectGenerator.newEventGenerator(conf, classLoader);
          // Create a data generator
          final DataGenerator dataGenerator = physicalObjectGenerator.newDataGenerator(conf, classLoader);
          // Create a source
          final String id = operatorIdGenerator.generate();
          final PhysicalSource source = new PhysicalSourceImpl<>(
              identifierFactory.getNewInstance(id),
              dataGenerator, eventGenerator);
          deserializedVertices.add(source);
          physicalDAG.addVertex(source);
          // Add the physical vertex to the physical map
          physicalVertexMap.getPhysicalVertexMap().put(id, source);

          // Create a logical vertex
          final LogicalVertex logicalVertex = new DefaultLogicalVertexImpl(id);
          logicalVertices.add(Arrays.asList(logicalVertex));
          logicalDAG.addVertex(logicalVertex);
          break;
        }
        case OPERATOR_CHAIN: {
          final PartitionedQuery partitionedQuery = new DefaultPartitionedQueryImpl();
          LogicalVertex firstLogicalVertex = null;
          LogicalVertex prevLogicalVertex = null;
          final List<LogicalVertex> logicalVertexList = new ArrayList<>(avroVertexChain.getVertexChain().size());
          deserializedVertices.add(partitionedQuery);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
                new ClassHierarchyImpl(urls));
            final String id = operatorIdGenerator.generate();
            final Operator operator = physicalObjectGenerator.newOperator(id, conf, classLoader);
            partitionedQuery.insertToTail(operator);
            // Add the physical vertex to the physical map
            physicalVertexMap.getPhysicalVertexMap().put(id,
                new DefaultPhysicalOperatorImpl(operator, partitionedQuery));

            // Create a logical vertex
            final LogicalVertex logicalVertex = new DefaultLogicalVertexImpl(id);
            if (firstLogicalVertex == null) {
              firstLogicalVertex = logicalVertex;
            }
            logicalDAG.addVertex(logicalVertex);

            // Connect logical vertices in the chain of operators
            // This is for adding the vertices in a partition.
            if (prevLogicalVertex == null) {
              prevLogicalVertex = logicalVertex;
            } else {
              logicalDAG.addEdge(prevLogicalVertex, logicalVertex, Direction.LEFT);
              prevLogicalVertex = logicalVertex;
            }
            logicalVertexList.add(logicalVertex);
          }
          physicalDAG.addVertex(partitionedQuery);

          // Add the logical partitioned vertex
          logicalVertices.add(logicalVertexList);
          break;
        }
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
              new ClassHierarchyImpl(urls));
          final String id = operatorIdGenerator.generate();
          final PhysicalSink sink = new PhysicalSinkImpl<>(physicalObjectGenerator.newSink(id, conf, classLoader));
          deserializedVertices.add(sink);
          physicalDAG.addVertex(sink);
          // Add the physical vertex to the physical map
          physicalVertexMap.getPhysicalVertexMap().put(id, sink);

          // Create a logical vertex
          final LogicalVertex logicalVertex = new DefaultLogicalVertexImpl(id);
          logicalDAG.addVertex(logicalVertex);
          logicalVertices.add(Arrays.asList(logicalVertex));
          break;
        }
        default: {
          throw new IllegalArgumentException("MISTTask: Invalid vertex detected in AvroLogicalPlan!");
        }
      }
    }

    // Add edge info to physical plan and logical plan
    for (final Edge edge : avroLogicalPlan.getEdges()) {
      final int srcIndex = edge.getFrom();
      final int dstIndex = edge.getTo();

      // Add edge to physical plan
      final PhysicalVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final PhysicalVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      physicalDAG.addEdge(deserializedSrcVertex, deserializedDstVertex,
          new Tuple<>(edge.getDirection(), edge.getBranchIndex()));

      // Add edge to logical plan
      final List<LogicalVertex> srcLogicalVertices = logicalVertices.get(srcIndex);
      final List<LogicalVertex> dstLogicalVertices = logicalVertices.get(dstIndex);
      final LogicalVertex srcLogicalVertex = srcLogicalVertices.get(srcLogicalVertices.size() - 1);
      final LogicalVertex dstLogicalVertex = dstLogicalVertices.get(0);
      logicalDAG.addEdge(srcLogicalVertex, dstLogicalVertex, edge.getDirection());
    }

    return new DefaultLogicalAndPhysicalPlanImpl(logicalDAG, physicalDAG);
  }
}