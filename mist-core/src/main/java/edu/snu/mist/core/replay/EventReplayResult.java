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
package edu.snu.mist.core.replay;

import org.apache.reef.io.Tuple;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.List;
import java.util.Map;

/**
 * A class that contains the success status and the messages of event replay requests.
 */
public final class EventReplayResult {

  private final boolean success;

  private final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> brokerAndTopicMqttMsgMap;

  public EventReplayResult(final boolean success,
                           final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> brokerAndTopicMqttMsgMap) {
    this.success = success;
    this.brokerAndTopicMqttMsgMap = brokerAndTopicMqttMsgMap;
  }

  public boolean isSuccess() {
    return success;
  }

  public Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> getBrokerAndTopicMqttMessageMap() {
    return brokerAndTopicMqttMsgMap;
  }
}
