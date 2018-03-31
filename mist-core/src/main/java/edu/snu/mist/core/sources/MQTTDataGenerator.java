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
package edu.snu.mist.core.sources;

import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * This class receives data stream via MQTTSubscribeClient.
 */
public final class MQTTDataGenerator implements DataGenerator<MqttMessage> {
  private static final Logger LOG = Logger.getLogger(MQTTDataGenerator.class.getName());

  /**
   * A flag for start.
   */
  private final AtomicBoolean started;

  /**
   * A flag for close.
   */
  private final AtomicBoolean closed;

  /**
   * The topic of connected MQTT broker to subscribe.
   */
  private final String topic;

  /**
   * The MQTT client subscribing a broker.
   */
  private final MQTTSubscribeClient subClient;

  /**
   * Event generator which is the destination of fetched data.
   */
  private EventGenerator<MqttMessage> eventGenerator;

  public MQTTDataGenerator(final MQTTSubscribeClient subClient,
                           final String topic) {
    this.subClient = subClient;
    this.topic = topic;
    this.started = new AtomicBoolean(false);
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Emit the given MQTT message toward the EventGenerator.
   * This function would be called by MQTTSubscribeClient when it received message of the target topic from it's broker.
   * @param message the message to emit
   */
  void emitData(final MqttMessage message) {
    if (!closed.get() && eventGenerator != null) {
      eventGenerator.emitData(message);
    }
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      subClient.subscribe(topic);
    }
  }

  @Override
  public void close() {
    closed.compareAndSet(false, true);
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
  }

  @Override
  public void setEventGenerator(final EventGenerator eventGenerator) {
    this.eventGenerator = eventGenerator;
  }
}