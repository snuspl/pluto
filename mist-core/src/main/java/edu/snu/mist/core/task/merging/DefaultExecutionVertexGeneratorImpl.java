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

import edu.snu.mist.common.operators.StateHandler;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.core.task.*;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A default implementation of ExecutionVertexGenerator.
 */
final class DefaultExecutionVertexGeneratorImpl implements ExecutionVertexGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultExecutionVertexGeneratorImpl.class.getName());

  private final IdGenerator idGenerator;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final AvroConfigurationSerializer avroConfigurationSerializer;

  @Inject
  private DefaultExecutionVertexGeneratorImpl(final IdGenerator idGenerator,
                                              final AvroConfigurationSerializer avroConfigurationSerializer,
                                              final PhysicalObjectGenerator physicalObjectGenerator) {
    this.idGenerator = idGenerator;
    this.avroConfigurationSerializer = avroConfigurationSerializer;
    this.physicalObjectGenerator = physicalObjectGenerator;
  }

  @Override
  public ExecutionVertex generate(final ConfigVertex configVertex,
                                  final URL[] urls,
                                  final ClassLoader classLoader)
      throws IOException, ClassNotFoundException {
    switch (configVertex.getType()) {
      case SOURCE: {
        final Map<String, String> conf = configVertex.getConfiguration();
        // Create an event generator
        final EventGenerator eventGenerator = physicalObjectGenerator.newEventGenerator(conf, classLoader);
        // Create a data generator
        final DataGenerator dataGenerator = physicalObjectGenerator.newDataGenerator(conf, classLoader);
        // Create a source
        final String id = idGenerator.generateSourceId();
        return new PhysicalSourceImpl<>(id, conf, dataGenerator, eventGenerator);
      }
      case OPERATOR: {
        final String operatorId = idGenerator.generateOperatorId();
        final Map<String, String> conf = configVertex.getConfiguration();
        final PhysicalOperator operator = new DefaultPhysicalOperatorImpl(operatorId, conf,
            physicalObjectGenerator.newOperator(conf, classLoader));
        if (configVertex.getState().size() != 0) {
          ((StateHandler) operator.getOperator()).setState(
              StateSerializer.deserializeStateMap(configVertex.getState()));
        }
        if (configVertex.getLatestCheckpointTimestamp() != 0) {
          ((StateHandler) operator.getOperator()).setRecoveredTimestamp(
              configVertex.getLatestCheckpointTimestamp());
        }
        return operator;
      }
      case SINK:
        final Map<String, String> conf = configVertex.getConfiguration();
        final String id = idGenerator.generateSinkId();
        final PhysicalSink sink = new PhysicalSinkImpl<>(id, conf,
            physicalObjectGenerator.newSink(conf, classLoader));
        return sink;
      default:
        throw new IllegalArgumentException("Invalid vertex type: " + configVertex.getType());
    }
  }
}