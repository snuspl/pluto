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

import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.parameters.ReplayServerAddress;
import edu.snu.mist.core.parameters.ReplayServerPort;
import edu.snu.mist.core.task.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This query starter does not merge queries.
 * Instead, it executes them separately.
 */
public final class NoMergingQueryStarter extends BaseQueryStarter implements QueryStarter {

  private static final Logger LOG = Logger.getLogger(NoMergingQueryStarter.class.getName());

  /**
   * The map that has a query id as a key and an execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  /**
   * The dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * The map of sources and queryIds. This is only used for replaying events.
   */
  private final Map<PhysicalSource, String> srcAndQueryIdMap;

  /**
   * The address of the replay server.
   */
  private final String replayServerAddress;

  /**
   * The port number of the replay server.
   */
  private final int replayServerPort;

  @Inject
  private NoMergingQueryStarter(final ExecutionPlanDagMap executionPlanDagMap,
                                final DagGenerator dagGenerator,
                                @Parameter(ReplayServerAddress.class) final String replayServerAddress,
                                @Parameter(ReplayServerPort.class) final int replayServerPort) {
    super(replayServerAddress, replayServerPort);
    this.executionPlanDagMap = executionPlanDagMap;
    this.dagGenerator = dagGenerator;
    this.srcAndQueryIdMap = new HashMap<>();
    this.replayServerAddress = replayServerAddress;
    this.replayServerPort = replayServerPort;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   */
  @Override
  public Set<Tuple<String, String>> startOrSetupForReplay(final boolean isStart,
                                                          final String queryId,
                                                          final Query query,
                                                          final DAG<ConfigVertex, MISTEdge> configDag,
                                                          final List<String> jarFilePaths)
      throws IOException, ClassNotFoundException {
    // The set to contain the tuples (topic and broker uri) of the submitted query.
    final Set<Tuple<String, String>> topicAndBrokerURISet = new HashSet<>();

    final ExecutionDag submittedExecutionDag = dagGenerator.generate(configDag, jarFilePaths);
    if (!isStart) {
      for (final ExecutionVertex source : submittedExecutionDag.getDag().getRootVertices()) {
        srcAndQueryIdMap.put((PhysicalSource) source, queryId);
      }
      for (final ConfigVertex source : configDag.getRootVertices()) {
        final Map<String, String> srcConfMap = source.getConfiguration();

        final String mqttTopic = srcConfMap.get(ConfKeys.MQTTSourceConf.MQTT_SRC_TOPIC.name());
        final String mqttBrokerAddressAndPort = srcConfMap.get(ConfKeys.MQTTSourceConf.MQTT_SRC_BROKER_URI.name());

        if (mqttTopic != null && mqttBrokerAddressAndPort != null) {
          final Tuple<String, String> brokerTopic = new Tuple<>(mqttTopic, mqttBrokerAddressAndPort);
          topicAndBrokerURISet.add(brokerTopic);
        } else {
          LOG.log(Level.WARNING, "No mqttTopic or mqttBrokerAddressAndPort were found");
        }
      }
    }
    executionPlanDagMap.put(queryId, submittedExecutionDag);
    QueryStarterUtils.setUpOutputEmitters(submittedExecutionDag, query);
    if (isStart) {
      // starts to receive input data stream from the sources
      final DAG<ExecutionVertex, MISTEdge> dag = submittedExecutionDag.getDag();
      for (final ExecutionVertex source : dag.getRootVertices()) {
        final PhysicalSource ps = (PhysicalSource)source;
        ps.start();
      }
    }
    return topicAndBrokerURISet;
  }

  @Override
  public void replayAndStart(final Map<String, Set<Tuple<String, String>>> queryIdAndBrokerTopicMap,
                             final long minTimestamp)
      throws InjectionException, IOException, ClassNotFoundException {
    super.replayAndStart(queryIdAndBrokerTopicMap, minTimestamp, executionPlanDagMap.getExecutionDags());
  }
}
