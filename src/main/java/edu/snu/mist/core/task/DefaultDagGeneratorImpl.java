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
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;

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

  private final IdGenerator idGenerator;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final AvroConfigurationSerializer avroConfigurationSerializer;
  private final OperatorChainFactory operatorChainFactory;

  @Inject
  private DefaultDagGeneratorImpl(final IdGenerator idGenerator,
                                  final ClassLoaderProvider classLoaderProvider,
                                  final AvroConfigurationSerializer avroConfigurationSerializer,
                                  final PhysicalObjectGenerator physicalObjectGenerator,
                                  final OperatorChainFactory operatorChainFactory) {
    this.idGenerator = idGenerator;
    this.classLoaderProvider = classLoaderProvider;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.physicalObjectGenerator = physicalObjectGenerator;
    this.operatorChainFactory = operatorChainFactory;
  }

  private ExecutionVertex createExecutionVertex(final ConfigVertex configVertex,
                                                final URL[] urls,
                                                final ClassLoader classLoader) throws IOException, InjectionException {
    switch (configVertex.getType()) {
      case SOURCE: {
        final String strConf = configVertex.getConfiguration().get(0);
        final Configuration conf = avroConfigurationSerializer.fromString(strConf, new ClassHierarchyImpl(urls));
        // Create an event generator
        final EventGenerator eventGenerator = physicalObjectGenerator.newEventGenerator(conf, classLoader);
        // Create a data generator
        final DataGenerator dataGenerator = physicalObjectGenerator.newDataGenerator(conf, classLoader);
        // Create a source
        final String id = idGenerator.generateSourceId();
        return new PhysicalSourceImpl<>(id, strConf, dataGenerator, eventGenerator);
      }
      case OPERATOR_CHAIN: {
        final String opChainId = idGenerator.generateOperatorChainId();
        final OperatorChain operatorChain = operatorChainFactory.newInstance(opChainId);
        for (final String strConf : configVertex.getConfiguration()) {
          final Configuration conf = avroConfigurationSerializer.fromString(strConf,
              new ClassHierarchyImpl(urls));
          final String id = idGenerator.generateOperatorId();
          final Operator operator = physicalObjectGenerator.newOperator(conf, classLoader);
          final PhysicalOperator physicalOperator = new DefaultPhysicalOperatorImpl(id, strConf,
              operator, operatorChain);
          operatorChain.insertToTail(physicalOperator);
        }
        return operatorChain;
      }
      case SINK:
        final String strConf = configVertex.getConfiguration().get(0);
        final Configuration conf = avroConfigurationSerializer.fromString(strConf,
            new ClassHierarchyImpl(urls));
        final String id = idGenerator.generateSinkId();
        final PhysicalSink sink = new PhysicalSinkImpl<>(id, strConf,
            physicalObjectGenerator.newSink(conf, classLoader));
        return sink;
      default:
        throw new IllegalArgumentException("Invalid vertex type: " + configVertex.getType());
    }
  }

  private void dfsCreation(final ExecutionVertex parent,
                           final MISTEdge parentEdge,
                           final ConfigVertex currVertex,
                           final Map<ConfigVertex, ExecutionVertex> created,
                           final DAG<ConfigVertex, MISTEdge> configDag,
                           final DAG<ExecutionVertex, MISTEdge> executionDag,
                           final URL[] urls,
                           final ClassLoader classLoader) throws IOException, InjectionException {
    final ExecutionVertex currExecutionVertex;
    if (created.get(currVertex) == null) {
      currExecutionVertex = createExecutionVertex(currVertex, urls, classLoader);
      created.put(currVertex, currExecutionVertex);
      executionDag.addVertex(currExecutionVertex);
      // do dfs creation
      for (final Map.Entry<ConfigVertex, MISTEdge> edges : configDag.getEdges(currVertex).entrySet()) {
        final ConfigVertex childVertex = edges.getKey();
        final MISTEdge edge = edges.getValue();
        dfsCreation(currExecutionVertex, edge, childVertex, created, configDag, executionDag, urls, classLoader);
      }
    } else {
      currExecutionVertex = created.get(currVertex);
    }
    executionDag.addEdge(parent, currExecutionVertex, parentEdge);
  }

  /**
   * This generates the logical and physical plan from the avro operator chain dag.
   * Note that the avro operator chain dag is already partitioned,
   * so we need to rewind the partition to generate the logical dag.
   * @param configDag the tuple of queryId and avro operator chain dag
   * @return the logical and execution dag
   */
  @Override
  public DAG<ExecutionVertex, MISTEdge> generate(final DAG<ConfigVertex, MISTEdge> configDag,
                                                 final List<String> jarFilePaths)
      throws IOException, ClassNotFoundException, InjectionException {
    // For execution dag
    final DAG<ExecutionVertex, MISTEdge> executionDag = new AdjacentListConcurrentMapDAG<>();

    // Get a class loader
    final URL[] urls = SerializeUtils.getJarFileURLs(jarFilePaths);
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    final Map<ConfigVertex, ExecutionVertex> created = new HashMap<>(configDag.numberOfVertices());
    for (final ConfigVertex source : configDag.getRootVertices()) {
      final ExecutionVertex currExecutionVertex = createExecutionVertex(source, urls, classLoader);
      executionDag.addVertex(currExecutionVertex);
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