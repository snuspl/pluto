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
import edu.snu.mist.core.parameters.MqttSinkClientNumPerBroker;
import edu.snu.mist.core.parameters.MqttSourceClientNumPerBroker;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.*;

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
   * The map containing topic-subscriber information.
   */
  private final Map<String, Map<String, MQTTSubscribeClient>> topicSubscriberMap;

  /**
   * The map containing group-subscriber information.
   */
  private final Map<String, Map<String, MQTTSubscribeClient>> groupSubscriberMap;

  /**
   * The map containing group-publisher information.
   */
  private final Map<String, Map<String, IMqttAsyncClient>> groupPublisherMap;

  /**
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final Map<String, List<MQTTSubscribeClient>> brokerSubscriberMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final Map<MQTTSubscribeClient, Integer> subscriberSourceNumMap;

  /**
   * The map that has broker URI as a key and list of mqtt clients as a value.
   */
  private final Map<String, List<IMqttAsyncClient>> brokerPublisherMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final Map<IMqttAsyncClient, Integer> publisherSinkNumMap;

  /**
   * The number of maximum mqtt sources per client.
   */
  private final int mqttSourceClientNumPerBroker;

  /**
   * The number of maximum mqtt sinks per client.
   */
  private final int mqttSinkClientNumPerBroker;

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
      @Parameter(MqttSourceClientNumPerBroker.class) final int mqttSourceClientNumPerBrokerParam,
      @Parameter(MqttSinkClientNumPerBroker.class) final int mqttSinkClientNumPerBrokerParam,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam) {
    this.brokerSubscriberMap = new HashMap<>();
    this.subscriberSourceNumMap = new HashMap<>();
    this.brokerPublisherMap = new HashMap<>();
    this.publisherSinkNumMap = new HashMap<>();
    this.topicSubscriberMap = new HashMap<>();
    this.groupPublisherMap = new HashMap<>();
    this.groupSubscriberMap = new HashMap<>();
    this.subscriberLock = new ReentrantLock();
    this.publisherLock = new ReentrantLock();
    this.mqttSourceClientNumPerBroker = mqttSourceClientNumPerBrokerParam;
    this.mqttSinkClientNumPerBroker = mqttSinkClientNumPerBrokerParam;
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
  }

  /**
   * Get the mqtt client for the sink with the target broker.
   * @param brokerURI the mqtt broker uri
   * @return mqtt client
   * @throws MqttException
   * @throws IOException
   */
  public IMqttAsyncClient getMqttSinkClient(
      final String brokerURI,
      final String topic) throws MqttException, IOException {
    this.publisherLock.lock();
    // TODO: Provide group information from QueryManager
    final String group = getGroupName(topic);
    final List<IMqttAsyncClient> mqttAsyncClientList = brokerPublisherMap.get(brokerURI);
    if (mqttAsyncClientList == null) {
      // Initialize the broker list
      brokerPublisherMap.put(brokerURI, new ArrayList<>());
      for (int i = 0; i < mqttSinkClientNumPerBroker; i++) {
        createSinkClient(brokerURI, brokerPublisherMap.get(brokerURI));
      }
      // Initialize the group-client list
      final HashMap<String, IMqttAsyncClient> myGroupPublisherMap = new HashMap<>();
      groupPublisherMap.put(brokerURI, myGroupPublisherMap);
      // Get the first client...
      final IMqttAsyncClient client = brokerPublisherMap.get(brokerURI).get(0);
      publisherSinkNumMap.replace(client, publisherSinkNumMap.get(client) + 1);
      myGroupPublisherMap.put(group, client);
      this.publisherLock.unlock();
      return client;
    } else {
      final Map<String, IMqttAsyncClient> myGroupPublisherMap = groupPublisherMap.get(brokerURI);
      if (myGroupPublisherMap.containsKey(group)) {
        final IMqttAsyncClient client = myGroupPublisherMap.get(group);
        //publisherSinkNumMap.replace(client, publisherSinkNumMap.get(client) + 1);
        this.publisherLock.unlock();
        return client;
      } else {
        int minSinkNum = Integer.MAX_VALUE;
        IMqttAsyncClient client = null;
        for (final IMqttAsyncClient mqttAsyncClient: brokerPublisherMap.get(brokerURI)) {
          if (minSinkNum > publisherSinkNumMap.get(mqttAsyncClient)) {
            minSinkNum = publisherSinkNumMap.get(mqttAsyncClient);
            client = mqttAsyncClient;
          }
        }
        publisherSinkNumMap.replace(client, publisherSinkNumMap.get(client) + 1);
        myGroupPublisherMap.put(group, client);
        this.publisherLock.unlock();
        return client;
      }
    }
  }

  /**
   * A helper function which creates create sink client. Should be called with publisherLock acquired.
   * @param brokerURI broker URI
   * @param mqttAsyncClientList the client list which broker URI belongs to
   * @return newly created sink client
   * @throws MqttException
   * @throws IOException
   */
  private void createSinkClient(final String brokerURI, final List<IMqttAsyncClient> mqttAsyncClientList)
      throws MqttException, IOException {
    final IMqttAsyncClient client = new MqttAsyncClient(brokerURI, MQTT_PUBLISHER_ID_PREFIX + brokerURI +
        mqttAsyncClientList.size());
    final MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setMaxInflight(maxInflightMqttEventNum);
    client.connect(connectOptions).waitForCompletion();
    mqttAsyncClientList.add(client);
    publisherSinkNumMap.put(client, 0);
  }

  private String getGroupName(final String mqttTopic) {
    for (final String candidate: mqttTopic.split("/")) {
      if (candidate.startsWith("group")) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * @param brokerURI the URI of broker to subscribe
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator getDataGenerator(
      final String brokerURI,
      final String topic) {
    this.subscriberLock.lock();
    // TODO: Provide group information from QueryManager
    final String group = getGroupName(topic);
    final List<MQTTSubscribeClient> subscribeClientList = brokerSubscriberMap.get(brokerURI);
    if (subscribeClientList == null) {
      // Initialize the client list...
      final List<MQTTSubscribeClient> newSubscribeClientList = new ArrayList<>();
      for (int i = 0; i < this.mqttSourceClientNumPerBroker; i++) {
        final MQTTSubscribeClient subscribeClient = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX +
            brokerURI + "_" + i);
        subscriberSourceNumMap.put(subscribeClient, 0);
        newSubscribeClientList.add(subscribeClient);
      }
      brokerSubscriberMap.put(brokerURI, newSubscribeClientList);
      // Initialize the group-sub map
      final Map<String, MQTTSubscribeClient> myGroupSubscriberMap = new HashMap<>();
      groupSubscriberMap.put(brokerURI, myGroupSubscriberMap);
      // Initialize the topic-sub map
      final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = new HashMap<>();
      topicSubscriberMap.put(brokerURI, myTopicSubscriberMap);
      final MQTTSubscribeClient client = newSubscribeClientList.get(0);
      myGroupSubscriberMap.put(group, client);
      myTopicSubscriberMap.put(topic, client);
      subscriberSourceNumMap.replace(client, subscriberSourceNumMap.get(client) + 1);
      this.subscriberLock.unlock();
      return client.connectToTopic(topic);
    } else {
      final Map<String, MQTTSubscribeClient> myGroupSubscriberMap = groupSubscriberMap.get(brokerURI);
      final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = topicSubscriberMap.get(brokerURI);
      if (myGroupSubscriberMap.containsKey(group)) {
        // This is for group-sharing.
        final MQTTSubscribeClient client = myGroupSubscriberMap.get(group);
        myTopicSubscriberMap.putIfAbsent(topic, client);
        this.subscriberLock.unlock();
        return client.connectToTopic(topic);
      } else if (myTopicSubscriberMap.containsKey(topic)) {
        // This is for topic sharing.
        final MQTTSubscribeClient client = myTopicSubscriberMap.get(group);
        subscriberSourceNumMap.replace(client, subscriberSourceNumMap.get(client) + 1);
        myGroupSubscriberMap.put(group, client);
        this.subscriberLock.unlock();
        return client.connectToTopic(topic);
      } else {
        int minSourceNum = Integer.MAX_VALUE;
        MQTTSubscribeClient client = null;
        for (final MQTTSubscribeClient mqttSubcribeClient: subscribeClientList) {
          if (minSourceNum > subscriberSourceNumMap.get(mqttSubcribeClient)) {
            minSourceNum = subscriberSourceNumMap.get(mqttSubcribeClient);
            client = mqttSubcribeClient;
          }
        }
        subscriberSourceNumMap.replace(client, subscriberSourceNumMap.get(client) + 1);
        myGroupSubscriberMap.put(group, client);
        myTopicSubscriberMap.put(topic, client);
        this.subscriberLock.unlock();
        return client.connectToTopic(topic);
      }
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
    brokerSubscriberMap.forEach(
        (brokerURI, subClientList) -> subClientList.forEach(subClient -> subClient.disconnect())
    );
    brokerPublisherMap.forEach((address, mqttAsyncClientList) -> mqttAsyncClientList.forEach(MqttAsyncClient -> {
          try {
            MqttAsyncClient.disconnect();
          } catch (final Exception e) {
            // do nothing
          }
        }
    ));
    brokerPublisherMap.clear();
  }
}
