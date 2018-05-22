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

/**
 * A class that contains the success status and the messages related to a topic of an event replay request.
 */
public final class EventReplayResult {

  private final boolean success;

  /**
   * The returned events for replay.
   * The tuple is consisted of the timestamp and the MqttMessage.
   */
  private final List<Tuple<Long, MqttMessage>> mqttMessages;

  public EventReplayResult(final boolean success,
                           final List<Tuple<Long, MqttMessage>> mqttMessages) {
    this.success = success;
    this.mqttMessages = mqttMessages;
  }

  public boolean isSuccess() {
    return success;
  }

  public List<Tuple<Long, MqttMessage>> getMqttMessages() {
    return mqttMessages;
  }
}
