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
package edu.snu.mist.core.shared;

import edu.snu.mist.core.parameters.TaskId;
import edu.snu.mist.core.shared.parameters.MaxInflightMqttEventNum;
import edu.snu.mist.core.shared.parameters.MqttSinkClientNumPerBroker;
import edu.snu.mist.core.shared.parameters.MqttSinkKeepAliveSec;
import edu.snu.mist.core.shared.parameters.MqttSourceClientNumPerBroker;
import edu.snu.mist.core.shared.parameters.MqttSourceKeepAliveSec;
import edu.snu.mist.core.sources.MQTTDataGenerator;
import edu.snu.mist.core.sources.MQTTSubscribeClient;
import org.apache.reef.tang.annotations.Parameter;
import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class manages MQTT clients.
 */
public final class MQTTSharedResource implements MQTTResource {
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
   * The map containing topic-publisher information.
   */
  private final Map<String, Map<String, IMqttAsyncClient>> topicPublisherMap;

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

  /**
   * Mqtt source keep-alive time in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * Mqtt sink keep-alive time in seconds.
   */
  private final int mqttSinkKeepAliveSec;

  /**
   * The hostname of this MistTask.
   */
  private String taskId;

  /**
   * MQTT client id generator.
   */
  private final AtomicInteger idGen = new AtomicInteger(0);

  @Inject
  private MQTTSharedResource(
      @Parameter(MqttSourceClientNumPerBroker.class) final int mqttSourceClientNumPerBrokerParam,
      @Parameter(MqttSinkClientNumPerBroker.class) final int mqttSinkClientNumPerBrokerParam,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam,
      @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
      @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec,
      @Parameter(TaskId.class) final String taskId) {
    this.brokerSubscriberMap = new HashMap<>();
    this.subscriberSourceNumMap = new HashMap<>();
    this.brokerPublisherMap = new HashMap<>();
    this.publisherSinkNumMap = new HashMap<>();
    this.topicPublisherMap = new HashMap<>();
    this.topicSubscriberMap = new HashMap<>();
    this.subscriberLock = new ReentrantLock();
    this.publisherLock = new ReentrantLock();
    this.mqttSourceClientNumPerBroker = mqttSourceClientNumPerBrokerParam;
    this.mqttSinkClientNumPerBroker = mqttSinkClientNumPerBrokerParam;
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
    this.taskId = taskId;
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
    final List<IMqttAsyncClient> mqttAsyncClientList = brokerPublisherMap.get(brokerURI);
    if (mqttAsyncClientList == null) {
      // Initialize the broker list
      brokerPublisherMap.put(brokerURI, new ArrayList<>());
      for (int i = 0; i < mqttSinkClientNumPerBroker; i++) {
        createSinkClient(brokerURI, brokerPublisherMap.get(brokerURI));
      }
      // Initialize the topic-client list
      final HashMap<String, IMqttAsyncClient> myTopicPublisherMap = new HashMap<>();
      topicPublisherMap.put(brokerURI, myTopicPublisherMap);
      // Get the first client...
      final IMqttAsyncClient client = brokerPublisherMap.get(brokerURI).get(0);
      final Integer c = publisherSinkNumMap.get(client);
      publisherSinkNumMap.replace(client, c + 1);
      myTopicPublisherMap.put(topic, client);
      this.publisherLock.unlock();
      return client;
    } else {
      final Map<String, IMqttAsyncClient> myTopicPublisherMap = topicPublisherMap.get(brokerURI);
      if (myTopicPublisherMap.containsKey(topic)) {
        final IMqttAsyncClient client = myTopicPublisherMap.get(topic);
        publisherSinkNumMap.replace(client, publisherSinkNumMap.get(client) + 1);
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
        myTopicPublisherMap.put(topic, client);
        this.publisherLock.unlock();
        return client;
      }
    }
  }

  @Override
  public void deleteMqttSinkClient(final String brokerURI,
                                   final String topic,
                                   final IMqttAsyncClient client) {
    this.publisherLock.lock();
    try {
      client.close();
    } catch (MqttException e) {
      e.printStackTrace();
    }
    final List<IMqttAsyncClient> mqttAsyncClientList = brokerPublisherMap.get(brokerURI);
    mqttAsyncClientList.remove(client);
    topicPublisherMap.get(brokerURI).remove(topic);
    publisherSinkNumMap.remove(client);
    this.publisherLock.unlock();
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
    while (true) {
      try {
        final IMqttAsyncClient client = new MqttAsyncClient(brokerURI, MQTT_PUBLISHER_ID_PREFIX + taskId
            + brokerURI + idGen.getAndIncrement());
        final MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setMaxInflight(maxInflightMqttEventNum);
        connectOptions.setKeepAliveInterval(mqttSinkKeepAliveSec);
        client.connect(connectOptions).waitForCompletion();
        mqttAsyncClientList.add(client);
        publisherSinkNumMap.put(client, 0);
        break;
      } catch (final MqttException e) {
        // Retry
        LOG.log(Level.SEVERE, "Retry mqtt sink connection");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
      }
    }
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
    final List<MQTTSubscribeClient> subscribeClientList = brokerSubscriberMap.get(brokerURI);
    if (subscribeClientList == null) {
      // Initialize the client list...
      final List<MQTTSubscribeClient> newSubscribeClientList = new ArrayList<>();
      for (int i = 0; i < this.mqttSourceClientNumPerBroker; i++) {
        final MQTTSubscribeClient subscribeClient = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX +
            taskId + brokerURI + "_" + i, mqttSourceKeepAliveSec);
        subscriberSourceNumMap.put(subscribeClient, 0);
        newSubscribeClientList.add(subscribeClient);
      }
      brokerSubscriberMap.put(brokerURI, newSubscribeClientList);
      // Initialize the topic-sub map
      final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = new HashMap<>();
      topicSubscriberMap.put(brokerURI, myTopicSubscriberMap);
      final MQTTSubscribeClient client = newSubscribeClientList.get(0);
      myTopicSubscriberMap.put(topic, client);
      subscriberSourceNumMap.replace(client, subscriberSourceNumMap.get(client) + 1);
      final MQTTDataGenerator gen = client.connectToTopic(topic);
      this.subscriberLock.unlock();
      return gen;
    } else {
      final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = topicSubscriberMap.get(brokerURI);
      if (myTopicSubscriberMap.containsKey(topic)) {
        // This is for group-sharing.
        final MQTTSubscribeClient client = myTopicSubscriberMap.get(topic);
        final MQTTDataGenerator gen = client.connectToTopic(topic);
        this.subscriberLock.unlock();
        return gen;
      } else {
        // This is a new group.
        int minSourceNum = Integer.MAX_VALUE;
        MQTTSubscribeClient client = null;
        for (final MQTTSubscribeClient mqttSubcribeClient: subscribeClientList) {
          if (minSourceNum > subscriberSourceNumMap.get(mqttSubcribeClient)) {
            minSourceNum = subscriberSourceNumMap.get(mqttSubcribeClient);
            client = mqttSubcribeClient;
          }
        }
        subscriberSourceNumMap.replace(client, subscriberSourceNumMap.get(client) + 1);
        myTopicSubscriberMap.put(topic, client);
        final MQTTDataGenerator gen = client.connectToTopic(topic);
        this.subscriberLock.unlock();
        return gen;
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
