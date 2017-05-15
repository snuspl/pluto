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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class represents MQTT clients implemented with eclipse Paho.
 * It will subscribe a MQTT broker and send the received data toward appropriate DataGenerator.
 */
public final class MQTTSubscribeClient implements MqttCallback {
  /**
   * A flag for start.
   */
  private boolean started;
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
   * The map coupling MQTT topic name and list of MQTTDataGenerators.
   */
  private final ConcurrentMap<String, List<MQTTDataGenerator>> dataGeneratorListMap;
  /**
   * The lock used when a DataGenerator want to start subscription.
   */
  private final Object subscribeLock;

  /**
   * Construct a client connected with target MQTT broker.
   * @param brokerURI the URI of broker to connect
   */
  public MQTTSubscribeClient(final String brokerURI,
                             final String clientId) {
    this.started = false;
    this.brokerURI = brokerURI;
    this.clientId = clientId;
    this.dataGeneratorListMap = new ConcurrentHashMap<>();
    this.subscribeLock = new Object();
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * When the start() method of the DataGenerator is called, the client will start to subscribe the requested topic.
   * If a DataGenerator having topic of connected broker is requested multiple-time,
   * already constructed DataGenerator will be returned.
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public synchronized MQTTDataGenerator connectToTopic(final String topic) {
    List<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList == null) {
      dataGeneratorList = new ArrayList<>();
      dataGeneratorListMap.put(topic, dataGeneratorList);
    }
    final MQTTDataGenerator dataGenerator = new MQTTDataGenerator(this, topic);
    dataGeneratorListMap.get(topic).add(dataGenerator);
    return dataGenerator;
  }

  /**
   * Start to subscribe a topic.
   */
  synchronized void subscribe(final String topic) throws MqttException {
    synchronized (subscribeLock) {
      if (!started) {
        client = new MqttClient(brokerURI, clientId);
        client.connect();
        client.setCallback(this);
        started = true;
      }
      client.subscribe(topic);
    }
  }

  /**
   * Unsubscribe a topic.
   */
  synchronized void unsubscribe(final String topic) throws MqttException {
    client.unsubscribe(topic);
    dataGeneratorListMap.remove(topic);
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
    final List<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList != null) {
      dataGeneratorList.forEach(dataGenerator -> dataGenerator.emitData(message));
    }
  }

  @Override
  public void deliveryComplete(final IMqttDeliveryToken token) {
    // do nothing because this class does not publish
  }
}
