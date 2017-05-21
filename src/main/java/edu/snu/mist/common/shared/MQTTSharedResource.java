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
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final ConcurrentMap<String, Deque<MQTTSubscribeClient>> mqttSubscriberMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final ConcurrentMap<MQTTSubscribeClient, AtomicInteger> subscriberSrcNumMap;

  /**
   * The map that has broker URI as a key and list of mqtt clients as a value.
   */
  private final ConcurrentMap<String, Deque<MqttClient>> mqttPublisherMap;

  /**
   * The map that has the number of sinks each mqtt clients support.
   */
  private final ConcurrentMap<MqttClient, AtomicInteger> publisherSinkNumMap;

  /**
   * The map which contains the number of subscribers per broker. This exists not to call ConcurrentLinkedDequeue
   * .size(), which takes O(n) to finish.
   */
  private final ConcurrentMap<String, Integer> brokerSubNumMap;

  /**
   * The map which contains the number of publishers per broker. This exists not to call ConcurrentLinkedDequeue.size
   * (), which takes O(n) to finish.
   */
  private final ConcurrentMap<String, Integer> brokerPubNumMap;

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

  @Inject
  private MQTTSharedResource(
      @Parameter(MaxMqttSourceNumPerClient.class) final int maxMqttSourceNumPerClientParam,
      @Parameter(MaxMqttSinkNumPerClient.class) final int maxMqttSinkNumPerClientParam,
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam) {
    this.mqttSubscriberMap = new ConcurrentHashMap<>();
    this.subscriberSrcNumMap = new ConcurrentHashMap<>();
    this.mqttPublisherMap = new ConcurrentHashMap<>();
    this.publisherSinkNumMap = new ConcurrentHashMap<>();
    this.brokerSubNumMap = new ConcurrentHashMap<>();
    this.brokerPubNumMap = new ConcurrentHashMap<>();
    this.maxMqttSourceNumPerClient = maxMqttSourceNumPerClientParam;
    this.maxMqttSinkNumPerClient = maxMqttSinkNumPerClientParam;
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
  }

  /**
   * Get the mqtt client for the sink with the target broker.
   * @param brokerURI the mqtt broker uri
   * @return mqtt client
   * @throws MqttException
   * @throws IOException
   */
  public MqttClient getMqttSinkClient(final String brokerURI) throws MqttException, IOException {
    Deque<MqttClient> mqttClientDeque = mqttPublisherMap.get(brokerURI);
    if (mqttClientDeque == null) {
      // Try to put when the client list is empty.
      mqttPublisherMap.putIfAbsent(brokerURI, new ConcurrentLinkedDeque<>());
      // This line should succeed because someone will put to the map.
      mqttClientDeque = mqttPublisherMap.get(brokerURI);
    }

    if (mqttClientDeque.isEmpty()) {
      // Acquire lock for modifying deque.
      synchronized (mqttClientDeque) {
        // Am I the first one to enter? Else, then another already created the first element.
        if (mqttClientDeque.isEmpty()) {
          createSinkClient(brokerURI, mqttClientDeque);
        }
      }
    }

    // Pick the last client until I'm allowed to in.
    int sinkNum;
    do {
      MqttClient clientCandidate = mqttClientDeque.peekLast();
      sinkNum = publisherSinkNumMap.get(clientCandidate).getAndIncrement();
      if (sinkNum <= maxMqttSinkNumPerClient) {
        // I'm allowed to enter. Reuse existing client.
        return clientCandidate;
      } else {
        // I'm not allowed to enter and we should make a new client.
        synchronized (mqttClientDeque) {
          // Am I the first one to get in? Then make a new client. If not, others already have made it.
          if (mqttClientDeque.peekLast() == clientCandidate) {
            createSinkClient(brokerURI, mqttClientDeque);
          }
          // Try again to avoid starvation.
          clientCandidate = mqttClientDeque.peekLast();
          sinkNum = publisherSinkNumMap.get(clientCandidate).getAndIncrement();
          if (sinkNum <= maxMqttSinkNumPerClient) {
            return clientCandidate;
          }
        }
      }
    } while(sinkNum > maxMqttSinkNumPerClient);
    // Should not reach here
    throw new IllegalStateException("getMqttSinkClient has entered the forbidden state!");
  }

  /**
   * A helper function which creates create sink client. Should be called with lock on mqttClientDeque acquired.
   * @param brokerURI broker URI
   * @param mqttClientDeque the client queue which broker URI belongs to
   * @return newly created sink client
   * @throws MqttException
   * @throws IOException
   */
  private void createSinkClient(final String brokerURI, final Deque<MqttClient> mqttClientDeque)
      throws MqttException, IOException {
    if (!brokerPubNumMap.containsKey(brokerURI)) {
      brokerPubNumMap.put(brokerURI, 0);
    }
    final int currentDequeSize = brokerPubNumMap.get(brokerURI);
    final MqttClient client = new MqttClient(brokerURI, MQTT_PUBLISHER_ID_PREFIX + brokerURI + currentDequeSize);
    final MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setMaxInflight(maxInflightMqttEventNum);
    client.connect(connectOptions);
    mqttClientDeque.add(client);
    publisherSinkNumMap.put(client, new AtomicInteger(0));
    brokerPubNumMap.replace(brokerURI, currentDequeSize + 1);
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * @param brokerURI the URI of broker to subscribe
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator getDataGenerator(final String brokerURI,
                                            final String topic) {
    Deque<MQTTSubscribeClient> subClientDeque = mqttSubscriberMap.get(brokerURI);
    if (subClientDeque == null) {
      mqttSubscriberMap.putIfAbsent(brokerURI, new ConcurrentLinkedDeque<>());
      subClientDeque = mqttSubscriberMap.get(brokerURI);
    }

    if (subClientDeque.isEmpty()) {
      synchronized(subClientDeque) {
        // Am I first?
        if (subClientDeque.isEmpty()) {
          createSubClient(brokerURI, subClientDeque);
        }
      }
    }

    int srcNum;
    do {
      // Pick the last client for the candidate.
      MQTTSubscribeClient subClientCandidate = subClientDeque.peekLast();
      srcNum = subscriberSrcNumMap.get(subClientCandidate).getAndIncrement();
      if (srcNum <= maxMqttSourceNumPerClient) {
        // I'm allowed to enter.
        return subClientCandidate.connectToTopic(topic);
      } else {
        // I'm not allowed to enter.
        synchronized(subClientDeque) {
          // Am I first to enter?
          if (subClientDeque.peekLast() == subClientCandidate) {
            createSubClient(brokerURI, subClientDeque);
          }
          // Try again to prevent starvation.
          subClientCandidate = subClientDeque.peekLast();
          srcNum = subscriberSrcNumMap.get(subClientCandidate).getAndIncrement();
          if (srcNum <= maxMqttSourceNumPerClient) {
            // I'm allowed to enter.
            return subClientCandidate.connectToTopic(topic);
          }
        }
      }
    } while (srcNum > maxMqttSourceNumPerClient);
    throw new IllegalStateException("getDataGenerator is in illegal state!");
  }

  /**
   * A helper function which creates create source client. Should be called with lock on subClientDeque acquired.
   * @param brokerURI broker URI
   * @param subClientDeque the client queue which broker URI belongs to
   */
  private void createSubClient(final String brokerURI, final Deque<MQTTSubscribeClient> subClientDeque) {
    if (!brokerSubNumMap.containsKey(brokerURI)) {
      brokerSubNumMap.put(brokerURI, 0);
    }
    final int currentDequeueSize = brokerSubNumMap.get(brokerURI);
    final MQTTSubscribeClient subClient = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX + brokerURI
      + "_" + currentDequeueSize);
    subClientDeque.add(subClient);
    subscriberSrcNumMap.put(subClient, new AtomicInteger(0));
    brokerSubNumMap.replace(brokerURI, currentDequeueSize + 1);
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
