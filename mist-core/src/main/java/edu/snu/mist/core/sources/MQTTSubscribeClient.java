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

import org.eclipse.paho.client.mqttv3.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class represents MQTT clients implemented with eclipse Paho.
 * It will subscribe a MQTT broker and send the received data toward appropriate DataGenerator.
 */
public final class MQTTSubscribeClient implements MqttCallback {
  private static final Logger LOG = Logger.getLogger(MQTTSubscribeClient.class.getName());

  /**
   * A flag for start.
   */
  private boolean started;
  /**
   * The actual Paho MQTT client.
   */
  private IMqttAsyncClient client;
  /**
   * The URI of broker to connect.
   */
  private final String brokerURI;
  /**
   * The id of client.
   */
  private String clientId;
  /**
   * The map coupling MQTT topic name and list of MQTTDataGenerators.
   */
  private final ConcurrentMap<String, Queue<MQTTDataGenerator>> dataGeneratorListMap;
  /**
   * The lock used when a DataGenerator want to start subscription.
   */
  private final Object subscribeLock;

  /**
   * Mqtt sink keep-alive time in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * Topics to subscribe.
   */
  private final List<String> topics;

  /**
   * Construct a client connected with target MQTT broker.
   * @param brokerURI the URI of broker to connect
   */
  public MQTTSubscribeClient(final String brokerURI,
                             final String clientId,
                             final int mqttSourceKeepAliveSec) {
    this.started = false;
    this.brokerURI = brokerURI;
    this.clientId = clientId;
    this.dataGeneratorListMap = new ConcurrentHashMap<>();
    this.subscribeLock = new Object();
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.topics = new LinkedList<>();
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
    Queue<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList == null) {
      dataGeneratorList = new ConcurrentLinkedQueue<>();
      dataGeneratorListMap.putIfAbsent(topic, dataGeneratorList);
    }
    final MQTTDataGenerator dataGenerator = new MQTTDataGenerator(this, topic);
    dataGeneratorListMap.get(topic).add(dataGenerator);
    return dataGenerator;
  }

  /**
   * Connect to client.
   */
  private void connect() {
    while (true) {
      try {
        client = new MqttAsyncClient(brokerURI, clientId);
        final MqttConnectOptions mqttConnectOptions = new MqttConnectOptions();
        mqttConnectOptions.setKeepAliveInterval(mqttSourceKeepAliveSec);
        client.connect(mqttConnectOptions).waitForCompletion();
        client.setCallback(this);
        break;
      } catch (final MqttException e) {
        try {
          client.close();
        } catch (final Exception e1) {
          // do nothing
        }
        // Reconnect mqtt
        LOG.log(Level.SEVERE, "Connection for broker {0} with id {1} failed ... Retry connection",
            new Object[] {brokerURI, clientId});
        try {
          Thread.sleep(1000);
        } catch (final InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  /**
   * Start to subscribe a topic.
   */
  void subscribe(final String topic) {
    synchronized (subscribeLock) {
      if (!started) {
        connect();
        started = true;
      }

      try {
        topics.add(topic);
        client.subscribe(topic, 0);
      } catch (final MqttException e) {
        LOG.log(Level.SEVERE, "MQTT exception for subscribing {0}... {1}...{2}",
            new Object[] {topic, e, clientId});
        clientId = clientId + "a";
        try {
          client.close();
        } catch (final MqttException e1) {
          // do nothing
          LOG.log(Level.SEVERE, "Close error {0}... {1}...{2}",
              new Object[] {topic, e, clientId});
        }

        // Restart
        connect();
        resubscribe();
      }
    }
  }

  /**
   * Resubscribe topics.
   */
  private void resubscribe() {
    LOG.log(Level.SEVERE, "Resubscribe topics for {0}...",
        new Object[] {clientId});
    try {
      for (final String topic : topics) {
        client.subscribe(topic, 0);
      }
    } catch (final MqttException e) {
      try {
        client.close();
      } catch (final MqttException e1) {
        // do nothing
      }

      clientId = clientId + "a";
      connect();
      resubscribe();
    }
  }

  /**
   * Unsubscribe a topic.
   */
  void unsubscribe(final String topic) throws MqttException {
    throw new UnsupportedOperationException("Unsubscribing is not supported now!");
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
    final Queue<MQTTDataGenerator> dataGeneratorList = dataGeneratorListMap.get(topic);
    if (dataGeneratorList != null) {
      dataGeneratorList.forEach(dataGenerator -> dataGenerator.emitData(message));
    }
  }

  @Override
  public void deliveryComplete(final IMqttDeliveryToken token) {
    // do nothing because this class does not publish
  }
}