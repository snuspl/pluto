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
import java.util.List;
import java.util.logging.Logger;

/**
 * A default implementation of PhysicalPlanGenerator.
 */
final class DefaultPhysicalPlanGeneratorImpl implements PhysicalPlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultPhysicalPlanGeneratorImpl.class.getName());

  private final OperatorIdGenerator operatorIdGenerator;
  private final String tmpFolderPath;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final StringIdentifierFactory identifierFactory;
  private final AvroConfigurationSerializer avroConfigurationSerializer;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                           @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                           final StringIdentifierFactory identifierFactory,
                                           final ClassLoaderProvider classLoaderProvider,
                                           final AvroConfigurationSerializer avroConfigurationSerializer,
                                           final PhysicalObjectGenerator physicalObjectGenerator) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.tmpFolderPath = tmpFolderPath;
    this.classLoaderProvider = classLoaderProvider;
    this.identifierFactory = identifierFactory;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.physicalObjectGenerator = physicalObjectGenerator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<PhysicalVertex, Direction> generate(
      final Tuple<String, AvroLogicalPlan> queryIdAndAvroLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InjectionException {
    final AvroLogicalPlan logicalPlan = queryIdAndAvroLogicalPlan.getValue();
    final List<PhysicalVertex> deserializedVertices = new ArrayList<>();
    final DAG<PhysicalVertex, Direction> dag = new AdjacentListDAG<>();

    // Get a class loader
    final URL[] urls = SerializeUtils.getURLs(logicalPlan.getJarFilePaths());
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : logicalPlan.getAvroVertices()) {
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
          final PhysicalSource source = new PhysicalSourceImpl<>(
              identifierFactory.getNewInstance(operatorIdGenerator.generate()),
              dataGenerator, eventGenerator);
          deserializedVertices.add(source);
          dag.addVertex(source);
          break;
        }
        case OPERATOR_CHAIN: {
          final PartitionedQuery partitionedQuery = new DefaultPartitionedQuery();
          deserializedVertices.add(partitionedQuery);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
                new ClassHierarchyImpl(urls));
            final Operator operator = physicalObjectGenerator.newOperator(
                operatorIdGenerator.generate(), conf, classLoader);
            partitionedQuery.insertToTail(operator);
          }
          dag.addVertex(partitionedQuery);
          break;
        }
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final Configuration conf = avroConfigurationSerializer.fromString(vertex.getConfiguration(),
              new ClassHierarchyImpl(urls));
          final PhysicalSink sink = new PhysicalSinkImpl<>(physicalObjectGenerator.newSink(
              operatorIdGenerator.generate(), conf, classLoader));
          deserializedVertices.add(sink);
          dag.addVertex(sink);
          break;
        }
        default: {
          throw new IllegalArgumentException("MISTTask: Invalid vertex detected in AvroLogicalPlan!");
        }
      }
    }

    // Add edge info to physical plan
    for (final Edge edge : logicalPlan.getEdges()) {
      final int srcIndex = edge.getFrom();
      final PhysicalVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final int dstIndex = edge.getTo();
      final PhysicalVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      dag.addEdge(deserializedSrcVertex, deserializedDstVertex, edge.getDirection());
    }
    return dag;
  }
}