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
package edu.snu.mist.common.shared;

import edu.snu.mist.common.sources.MQTTDataGenerator;
import edu.snu.mist.common.sources.MQTTSubscribeClient;
import edu.snu.mist.core.parameters.MaxInflightMqttEventNum;
import edu.snu.mist.core.parameters.MaxMqttSinkNumPerClient;
import edu.snu.mist.core.parameters.MaxMqttSourceNumPerClient;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * This class manages MQTT clients.
 */
public final class MQTTSharedResource implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(MQTTSharedResource.class.getName());

  /**
   * MQTT publisher id.
   */
  public static final String MQTT_PUBLISHER_ID_PREFIX = "MIST_MQTT_PUBLISHER_";

  /**
   * MQTT subscriber id.
   */
  public static final String MQTT_SUBSCRIBER_ID_PREFIX = "MIST_MQTT_SUBSCRIBER_";

  /**
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final Map<String, List<MQTTSubscribeClient>> mqttSubscriberMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final Map<MQTTSubscribeClient, Integer> subscriberSinkNumMap;

  /**
   * The map that has broker URI as a key and list of mqtt clients as a value.
   */
  private final Map<String, List<MqttClient>> mqttPublisherMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final Map<MqttClient, Integer> publisherSinkNumMap;

  /**
   * The number of maximum mqtt sources per client.
   */
  private final int maxMqttSourceNumPerClient;

  /**
   * The number of maximum mqtt sinks per client.
   */
  private final int maxMqttSinkNumPerClient;

  /**
   * The maximum number of mqtt inflight events, which is waiting inside the mqtt client queue.
   */
  private final int maxInflightMqttEventNum;

  /**
   * The lock used to synchronize subscriber creation.
   */
  private final Lock subscriberLock;

  /**
   * The lock used to sychronize publisher creation.
   */
  private final Lock publisherLock;

  @Inject
  private MQTTSharedResource(
      @Parameter(MaxMqttSourceNumPerClient.class) final int maxMqttSourceNumPerClientParam,
      @Parameter(MaxMqttSinkNumPerClient.class) final int maxMqttSinkNumPerClientParam,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam) {
    this.mqttSubscriberMap = new HashMap<>();
    this.subscriberSinkNumMap = new HashMap<>();
    this.mqttPublisherMap = new HashMap<>();
    this.publisherSinkNumMap = new HashMap<>();
    this.maxMqttSourceNumPerClient = maxMqttSourceNumPerClientParam;
    this.maxMqttSinkNumPerClient = maxMqttSinkNumPerClientParam;
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
    this.subscriberLock = new ReentrantLock();
    this.publisherLock = new ReentrantLock();
  }

  /**
   * Get the mqtt client for the sink with the target broker.
   * @param brokerURI the mqtt broker uri
   * @return mqtt client
   * @throws MqttException
   * @throws IOException
   */
  public MqttClient getMqttSinkClient(final String brokerURI) throws MqttException, IOException {
    this.publisherLock.lock();
    final List<MqttClient> mqttClientList = mqttPublisherMap.get(brokerURI);
    if (mqttClientList == null) {
      mqttPublisherMap.put(brokerURI, new ArrayList<>());
      final MqttClient client = createSinkClient(brokerURI, mqttPublisherMap.get(brokerURI));
      this.publisherLock.unlock();
      return client;
    } else {
      // Pick the last client for the candidate.
      final MqttClient clientCandidate = mqttClientList.get(mqttClientList.size() - 1);
      final int sinkNum = publisherSinkNumMap.get(clientCandidate);
      if (sinkNum < maxMqttSinkNumPerClient) {
        // It is okay to share already created mqtt client.
        publisherSinkNumMap.replace(clientCandidate, sinkNum + 1);
        this.publisherLock.unlock();
        return clientCandidate;
      } else {
        // We need to make a new mqtt client.
        final MqttClient newClientCandidate = createSinkClient(brokerURI, mqttClientList);
        this.publisherLock.unlock();
        return newClientCandidate;
      }
    }
  }

  /**
   * A helper function which creates create sink client. Should be called with publisherLock acquired.
   * @param brokerURI broker URI
   * @param mqttClientList the client list which broker URI belongs to
   * @return newly created sink client
   * @throws MqttException
   * @throws IOException
   */
  private MqttClient createSinkClient(final String brokerURI, final List<MqttClient> mqttClientList)
      throws MqttException, IOException {
    final MqttClient client = new MqttClient(brokerURI, MQTT_PUBLISHER_ID_PREFIX + brokerURI + mqttClientList.size());
    final MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setMaxInflight(maxInflightMqttEventNum);
    client.connect(connectOptions);
    mqttClientList.add(client);
    publisherSinkNumMap.put(client, 1);
    return client;
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * @param brokerURI the URI of broker to subscribe
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator getDataGenerator(final String brokerURI,
                                            final String topic) {
    this.subscriberLock.lock();
    final List<MQTTSubscribeClient> subscribeClientList = mqttSubscriberMap.get(brokerURI);
    if (subscribeClientList == null) {
      mqttSubscriberMap.put(brokerURI, new ArrayList<>());
      final MQTTSubscribeClient subscribeClient = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX +
          brokerURI + "_0");
      mqttSubscriberMap.get(brokerURI).add(subscribeClient);
      subscriberSinkNumMap.put(subscribeClient, 1);
      this.subscriberLock.unlock();
      return subscribeClient.connectToTopic(topic);
    } else {
      // Pick the last client for the candidate.
      final MQTTSubscribeClient subscribeClientCandidate = subscribeClientList.get(subscribeClientList.size() - 1);
      final int sourceNum = subscriberSinkNumMap.get(subscribeClientCandidate);
      if (sourceNum < maxMqttSourceNumPerClient) {
        // Let's reuse already existing one.
        subscriberSinkNumMap.replace(subscribeClientCandidate, sourceNum + 1);
        this.subscriberLock.unlock();
        return subscribeClientCandidate.connectToTopic(topic);
      } else {
        final MQTTSubscribeClient newSubscribeClientCandidate = new MQTTSubscribeClient(brokerURI,
            MQTT_SUBSCRIBER_ID_PREFIX + brokerURI + "_" + subscribeClientList.size());
        subscribeClientList.add(newSubscribeClientCandidate);
        subscriberSinkNumMap.put(newSubscribeClientCandidate, 1);
        this.subscriberLock.unlock();
        return subscribeClientCandidate.connectToTopic(topic);
      }
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
    mqttSubscriberMap.forEach((brokerURI, subClientList) -> subClientList.forEach(subClient -> subClient.disconnect()));
    mqttPublisherMap.forEach((address, mqttClientList) -> mqttClientList.forEach(mqttClient -> {
          try {
            mqttClient.disconnect();
          } catch (final Exception e) {
            // do nothing
          }
        }
      ));
    mqttPublisherMap.clear();
  }
}
