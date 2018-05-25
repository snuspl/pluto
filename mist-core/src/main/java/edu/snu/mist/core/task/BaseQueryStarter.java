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
package edu.snu.mist.core.task;

import edu.snu.mist.core.replay.EventReplayResult;
import edu.snu.mist.core.replay.EventReplayUtils;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseQueryStarter {

  private static final Logger LOG = Logger.getLogger(BaseQueryStarter.class.getName());

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

  protected BaseQueryStarter(final String replayServerAddress,
                             final int replayServerPort) {
    this.srcAndQueryIdMap = new HashMap<>();
    this.replayServerAddress = replayServerAddress;
    this.replayServerPort = replayServerPort;
  }

  protected void replayAndStart(final Map<String, Set<Tuple<String, String>>> queryIdAndBrokerTopicMap,
                                final long minTimestamp,
                                final Collection<ExecutionDag> executionDagCollection)
      throws InjectionException, IOException, ClassNotFoundException {
    if (replayServerAddress.equals("noReplay")) {
      LOG.log(Level.WARNING, "Replay server is not up.");
      // Start the sources.
      for (final ExecutionDag executionDag : executionDagCollection) {
        for (final ExecutionVertex executionVertex : executionDag.getDag().getRootVertices()) {
          final PhysicalSource src = (PhysicalSource) executionVertex;
          src.start();
        }
      }
    } else {
      final Set<Tuple<String, String>> replayedBrokerTopicSet = new HashSet<>();
      for (final ExecutionDag dag : executionDagCollection) {
        final Map<PhysicalSource, List<Tuple<Long, MqttMessage>>> srcAndMqttMessageListMap = new HashMap<>();
        for (final ExecutionVertex source : dag.getDag().getRootVertices()) {
          final PhysicalSource physicalSource = (PhysicalSource) source;
          final String queryId = srcAndQueryIdMap.remove(source);
          final Set<Tuple<String, String>> brokerURIAndTopicSet = queryIdAndBrokerTopicMap.get(queryId);
          for (final Tuple<String, String> brokerURIAndTopic : brokerURIAndTopicSet) {
            if (!replayedBrokerTopicSet.contains(brokerURIAndTopic)) {
              final EventReplayResult eventReplayResult =
                  EventReplayUtils.replay(replayServerAddress, replayServerPort,
                      brokerURIAndTopic.getValue(), brokerURIAndTopic.getKey(), minTimestamp);
              if (eventReplayResult.isSuccess()) {
                final List<Tuple<Long, MqttMessage>> mqttMessageList = eventReplayResult.getMqttMessages();
                srcAndMqttMessageListMap.put(physicalSource, mqttMessageList);
              } else {
                LOG.log(Level.WARNING, "Replay server is not up and/or replaying events has failed.");
                LOG.log(Level.WARNING,
                    "Sources for query " + queryId + " will be started without or partial replaying of events.");
              }
              replayedBrokerTopicSet.add(brokerURIAndTopic);
            }
          }
        }
        EventReplayUtils.sendMsgs(srcAndMqttMessageListMap);
      }
      EventReplayUtils.startSources(executionDagCollection);
    }
  }
}
