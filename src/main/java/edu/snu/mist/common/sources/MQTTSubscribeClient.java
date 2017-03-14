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
package edu.snu.mist.common.sources;

import org.eclipse.paho.client.mqttv3.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class represents MQTT clients implemented with eclipse Paho.
 * It will subscribe a MQTT broker and send the received data toward appropriate DataGenerator.
 */
public final class MQTTSubscribeClient implements MqttCallback {
  /**
   * A flag for start.
   */
  private final AtomicBoolean started;
  /**
   * The actual Paho MQTT client.
   */
  private MqttClient client;
  /**
   * The URI of broker to connect.
   */
  private final String brokerURI;
  /**
   * The id of client.
   */
  private final String clientId;
  /**
   * The map coupling MQTT topic name and MQTTDataGenerator.
   */
  private final ConcurrentMap<String, MQTTDataGenerator> dataGeneratorMap;
  /**
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final ConcurrentMap<String, MQTTSubscribeClient> mqttSubscribeClientMap;

  /**
   * Construct a client connected with target MQTT broker.
   * @param brokerURI the URI of broker to connect
   */
  public MQTTSubscribeClient(final String brokerURI,
                             final String clientId,
                             final ConcurrentMap<String, MQTTSubscribeClient> mqttSubscribeClientMap) {
    this.started = new AtomicBoolean(false);
    this.brokerURI = brokerURI;
    this.clientId = clientId;
    this.dataGeneratorMap = new ConcurrentHashMap<>();
    this.mqttSubscribeClientMap = mqttSubscribeClientMap;
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * When the start() method of the DataGenerator is called, the client will start to subscribe the requested topic.
   * If a DataGenerator having topic of connected broker is requested multiple-time,
   * already constructed DataGenerator will be returned.
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator connectToTopic(final String topic) {
    MQTTDataGenerator dataGenerator = dataGeneratorMap.get(topic);
    if (dataGenerator == null) {
      // there was not any MQTTDataGenerator subscribing target topic
      dataGenerator = new MQTTDataGenerator(this, topic);
      dataGeneratorMap.put(topic, dataGenerator);
    }
    return dataGenerator;
  }

  /**
   * Start to subscribe a topic.
   */
  void subscribe(final String topic) throws MqttException {
    if (started.compareAndSet(false, true)) {
      client = new MqttClient(brokerURI, clientId);
      client.connect();
      client.setCallback(this);
    }
    client.subscribe(topic);
  }

  /**
   * Unsubscribe a topic.
   */
  void unsubscribe(final String topic) throws MqttException {
    client.unsubscribe(topic);
    dataGeneratorMap.remove(topic);
  }

  /**
   * Close the connection between target MQTT broker.
   */
  public void disconnect() {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
  }

  @Override
  public void connectionLost(final Throwable cause) {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
  }

  @Override
  public void messageArrived(final String topic, final MqttMessage message) {
    final MQTTDataGenerator dataGenerator = dataGeneratorMap.get(topic);
    if (dataGenerator != null) {
      dataGenerator.emitData(message);
    }
  }

  @Override
  public void deliveryComplete(final IMqttDeliveryToken token) {
    // do nothing because this class does not publish
  }
}
