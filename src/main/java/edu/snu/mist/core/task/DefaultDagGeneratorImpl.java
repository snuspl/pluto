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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;
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
import java.util.List;
import java.util.logging.Logger;

/**
 * A default implementation of DagGenerator.
 */
final class DefaultDagGeneratorImpl implements DagGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultDagGeneratorImpl.class.getName());

  private final IdGenerator idGenerator;
  private final String tmpFolderPath;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final StringIdentifierFactory identifierFactory;
  private final AvroConfigurationSerializer avroConfigurationSerializer;
  private final PhysicalVertexMap physicalVertexMap;

  @Inject
  private DefaultDagGeneratorImpl(final IdGenerator idGenerator,
                                  @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                  final StringIdentifierFactory identifierFactory,
                                  final ClassLoaderProvider classLoaderProvider,
                                  final AvroConfigurationSerializer avroConfigurationSerializer,
                                  final PhysicalObjectGenerator physicalObjectGenerator,
                                  final PhysicalVertexMap physicalVertexMap) {
    this.idGenerator = idGenerator;
    this.tmpFolderPath = tmpFolderPath;
    this.classLoaderProvider = classLoaderProvider;
    this.identifierFactory = identifierFactory;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.physicalObjectGenerator = physicalObjectGenerator;
    this.physicalVertexMap = physicalVertexMap;
  }

  /**
   * This generates the logical and physical plan from the avro operator chain dag.
   * Note that the avro operator chain dag is already partitioned,
   * so we need to rewind the partition to generate the logical dag.
   * @param queryIdAndAvroOperatorChainDag the tuple of queryId and avro operator chain dag
   * @return the logical and execution dag
   */
  @SuppressWarnings("unchecked")
  @Override
  public DAG<ExecutionVertex, MISTEdge> generate(
      final Tuple<String, AvroOperatorChainDag> queryIdAndAvroOperatorChainDag)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InjectionException {
    final AvroOperatorChainDag avroOpChainDag = queryIdAndAvroOperatorChainDag.getValue();
    // For execution dag
    final List<ExecutionVertex> deserializedVertices = new ArrayList<>(avroOpChainDag.getAvroVertices().size());
    final DAG<ExecutionVertex, MISTEdge> executionDAG = new AdjacentListDAG<>();

    // Get a class loader
    final URL[] urls = SerializeUtils.getJarFileURLs(avroOpChainDag.getJarFilePaths());
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : avroOpChainDag.getAvroVertices()) {
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
          final String id = idGenerator.generateSourceId();
          final PhysicalSource source = new PhysicalSourceImpl<>(id, vertex.getConfiguration(),
              dataGenerator, eventGenerator);
          deserializedVertices.add(source);
          executionDAG.addVertex(source);
          // Add the physical vertex to the physical map
          physicalVertexMap.getPhysicalVertexMap().put(id, source);
          break;
        }
        case OPERATOR_CHAIN: {
          final OperatorChain operatorChain = new DefaultOperatorChainImpl();
          deserializedVertices.add(operatorChain);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
                new ClassHierarchyImpl(urls));
            final String id = idGenerator.generateOperatorId();
            final Operator operator = physicalObjectGenerator.newOperator(conf, classLoader);
            final PhysicalOperator physicalOperator = new DefaultPhysicalOperatorImpl(id, vertex.getConfiguration(),
                operator, operatorChain);
            operatorChain.insertToTail(physicalOperator);

            // Add the physical vertex to the physical map
            physicalVertexMap.getPhysicalVertexMap().put(id, physicalOperator);
          }
          executionDAG.addVertex(operatorChain);
          break;
        }
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
              new ClassHierarchyImpl(urls));
          final String id = idGenerator.generateSinkId();
          final PhysicalSink sink = new PhysicalSinkImpl<>(id, vertex.getConfiguration(),
              physicalObjectGenerator.newSink(conf, classLoader));
          deserializedVertices.add(sink);
          executionDAG.addVertex(sink);
          // Add the physical vertex to the physical map
          physicalVertexMap.getPhysicalVertexMap().put(id, sink);
          break;
        }
        default: {
          throw new IllegalArgumentException("MISTTask: Invalid vertex detected in AvroLogicalPlan!");
        }
      }
    }

    // Add edge info to the execution dag and logical dag
    for (final Edge edge : avroOpChainDag.getEdges()) {
      final int srcIndex = edge.getFrom();
      final int dstIndex = edge.getTo();

      // Add edge to the execution dag
      final ExecutionVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final ExecutionVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      executionDAG.addEdge(deserializedSrcVertex, deserializedDstVertex,
          new MISTEdge(edge.getDirection(), edge.getBranchIndex()));
    }

    return executionDAG;
  }
}