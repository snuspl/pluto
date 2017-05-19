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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.core.task.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Logger;

/**
 * A default implementation of ExecutionVertexGenerator.
 */
final class DefaultExecutionVertexGeneratorImpl implements ExecutionVertexGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultExecutionVertexGeneratorImpl.class.getName());

  private final IdGenerator idGenerator;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final AvroConfigurationSerializer avroConfigurationSerializer;
  private final OperatorChainFactory operatorChainFactory;

  @Inject
  private DefaultExecutionVertexGeneratorImpl(final IdGenerator idGenerator,
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

  @Override
  public ExecutionVertex generate(final ConfigVertex configVertex,
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
}