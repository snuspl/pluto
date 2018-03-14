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
package edu.snu.mist.common.shared;

import edu.snu.mist.common.shared.parameters.*;
import edu.snu.mist.common.sources.MQTTDataGenerator;
import edu.snu.mist.common.sources.MQTTSubscribeClient;
import org.apache.reef.io.Tuple;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final ConcurrentMap<String, Map<String, MQTTSubscribeClient>> topicSubscriberMap;

  /**
   * The map containing topic-publisher information.
   */
  private final ConcurrentMap<String, Map<String, IMqttAsyncClient>> topicPublisherMap;

  /**
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final ConcurrentMap<String, Tuple<List<MQTTSubscribeClient>, CountDownLatch>> brokerSubscriberMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final ConcurrentMap<MQTTSubscribeClient, AtomicInteger> subscriberSourceNumMap;

  /**
   * The map that has broker URI as a key and list of mqtt clients as a value.
   */
  private final ConcurrentMap<String, Tuple<List<IMqttAsyncClient>, CountDownLatch>> brokerPublisherMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final ConcurrentMap<IMqttAsyncClient, AtomicInteger> publisherSinkNumMap;

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
   * Mqtt source keep-alive time in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * Mqtt sink keep-alive time in seconds.
   */
  private final int mqttSinkKeepAliveSec;

  @Inject
  private MQTTSharedResource(
      @Parameter(MqttSourceClientNumPerBroker.class) final int mqttSourceClientNumPerBrokerParam,
      @Parameter(MqttSinkClientNumPerBroker.class) final int mqttSinkClientNumPerBrokerParam,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam,
      @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
      @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec) {
    this.brokerSubscriberMap = new ConcurrentHashMap<>();
    this.subscriberSourceNumMap = new ConcurrentHashMap<>();
    this.brokerPublisherMap = new ConcurrentHashMap<>();
    this.publisherSinkNumMap = new ConcurrentHashMap<>();
    this.topicPublisherMap = new ConcurrentHashMap<>();
    this.topicSubscriberMap = new ConcurrentHashMap<>();
    this.mqttSourceClientNumPerBroker = mqttSourceClientNumPerBrokerParam;
    this.mqttSinkClientNumPerBroker = mqttSinkClientNumPerBrokerParam;
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
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

    Tuple<List<IMqttAsyncClient>, CountDownLatch> tuple = brokerPublisherMap.get(brokerURI);
    if (tuple == null) {
      // Initialize the broker list
      final CountDownLatch latch = new CountDownLatch(0);
      final List<IMqttAsyncClient> publisherClientList = new ArrayList<>();
      if (brokerPublisherMap.putIfAbsent(brokerURI, new Tuple<>(publisherClientList, latch)) == null) {
        for (int i = 0; i < mqttSinkClientNumPerBroker; i++) {
          createSinkClient(brokerURI, publisherClientList);
        }

        // Initialize the topic-client list
        final HashMap<String, IMqttAsyncClient> myTopicPublisherMap = new HashMap<>();
        topicPublisherMap.put(brokerURI, myTopicPublisherMap);
        latch.countDown();
      }
    }

    tuple = brokerPublisherMap.get(brokerURI);
    final CountDownLatch latch = tuple.getValue();
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final List<IMqttAsyncClient> publishClientList = tuple.getKey();
    final Map<String, IMqttAsyncClient> myTopicPublisherMap = topicPublisherMap.get(brokerURI);
    if (myTopicPublisherMap.containsKey(topic)) {
      final IMqttAsyncClient client = myTopicPublisherMap.get(topic);
      publisherSinkNumMap.get(client).getAndIncrement();
      return client;
    } else {
      int minSinkNum = Integer.MAX_VALUE;
      IMqttAsyncClient client = null;
      for (final IMqttAsyncClient mqttAsyncClient: publishClientList) {
        if (minSinkNum > publisherSinkNumMap.get(mqttAsyncClient).get()) {
          minSinkNum = publisherSinkNumMap.get(mqttAsyncClient).get();
          client = mqttAsyncClient;
        }
      }
      publisherSinkNumMap.get(client).getAndIncrement();
      myTopicPublisherMap.put(topic, client);
      return client;
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
    connectOptions.setKeepAliveInterval(mqttSinkKeepAliveSec);
    client.connect(connectOptions).waitForCompletion();
    mqttAsyncClientList.add(client);
    publisherSinkNumMap.put(client, new AtomicInteger(0));
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

    // TODO: Provide group information from QueryManager
    Tuple<List<MQTTSubscribeClient>, CountDownLatch> tuple = brokerSubscriberMap.get(brokerURI);

    if (tuple == null) {
      // Initialize the client list...
      final List<MQTTSubscribeClient> newSubscribeClientList = new ArrayList<>();
      final CountDownLatch latch = new CountDownLatch(1);
      if (brokerSubscriberMap.putIfAbsent(brokerURI, new Tuple<>(newSubscribeClientList, latch)) == null) {
        for (int i = 0; i < this.mqttSourceClientNumPerBroker; i++) {
          final MQTTSubscribeClient subscribeClient = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX +
              brokerURI + "_" + i, mqttSourceKeepAliveSec);
          subscriberSourceNumMap.put(subscribeClient, new AtomicInteger(0));
          newSubscribeClientList.add(subscribeClient);
        }

        // Initialize the topic-sub map
        final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = new HashMap<>();
        topicSubscriberMap.put(brokerURI, myTopicSubscriberMap);
        //final MQTTSubscribeClient client = newSubscribeClientList.get(0);
        //myTopicSubscriberMap.put(topic, client);
        //subscriberSourceNumMap.get(client).incrementAndGet();

        latch.countDown();
      }
    }

    tuple = brokerSubscriberMap.get(brokerURI);
    final CountDownLatch latch = tuple.getValue();
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    final List<MQTTSubscribeClient> subscribeClientList = tuple.getKey();
    final Map<String, MQTTSubscribeClient> myTopicSubscriberMap = topicSubscriberMap.get(brokerURI);
    if (myTopicSubscriberMap.containsKey(topic)) {
      // This is for group-sharing.
      final MQTTSubscribeClient client = myTopicSubscriberMap.get(topic);
      subscriberSourceNumMap.get(client).getAndIncrement();
      return client.connectToTopic(topic);
    } else {
      // This is a new group.
      int minSourceNum = Integer.MAX_VALUE;
      MQTTSubscribeClient client = null;
      for (final MQTTSubscribeClient mqttSubcribeClient: subscribeClientList) {
        if (minSourceNum > subscriberSourceNumMap.get(mqttSubcribeClient).get()) {
          minSourceNum = subscriberSourceNumMap.get(mqttSubcribeClient).get();
          client = mqttSubcribeClient;
        }
      }
      subscriberSourceNumMap.get(client).getAndIncrement();
      myTopicSubscriberMap.put(topic, client);
      return client.connectToTopic(topic);
    }
  }

  @Override
  public void close() throws Exception {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
    brokerSubscriberMap.forEach(
        (brokerURI, subClientList) -> subClientList.getKey().forEach(subClient -> subClient.disconnect())
    );
    brokerPublisherMap.forEach((address, mqttAsyncClientList) ->
        mqttAsyncClientList.getKey().forEach(MqttAsyncClient -> {
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