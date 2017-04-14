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
import org.eclipse.paho.client.mqttv3.MqttClient;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * This class manages MQTT clients.
 */
public final class MQTTSharedResource implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(MQTTSharedResource.class.getName());

  /**
   * MQTT publisher id.
   */
  public static final String MQTT_PUBLISHER_ID = "MIST_MQTT_PUBLISHER";

  /**
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final ConcurrentMap<String, MQTTSubscribeClient> mqttSubscribeClientMap;

  /**
   * The map that has broker URI as a key and mqtt client as a value.
   */
  private final ConcurrentMap<String, MqttClient> mqttPublisherMap;

  @Inject
  private MQTTSharedResource() {
    this.mqttSubscribeClientMap = new ConcurrentHashMap<>();
    this.mqttPublisherMap = new ConcurrentHashMap<>();
  }

  /**
   * Get the MQTT publisher client map.
   * @return MQTT publisher client map
   */
  public ConcurrentMap<String, MqttClient> getMqttPublisherMap() {
    return mqttPublisherMap;
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * @param brokerURI the URI of broker to subscribe
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator getDataGenerator(final String brokerURI,
                                            final String topic) {
    MQTTSubscribeClient subscribeClient = mqttSubscribeClientMap.get(brokerURI);
    if (subscribeClient == null) {
      subscribeClient = new MQTTSubscribeClient(brokerURI, "MISTClient", mqttSubscribeClientMap);
      mqttSubscribeClientMap.putIfAbsent(brokerURI, subscribeClient);
      subscribeClient = mqttSubscribeClientMap.get(brokerURI);
    }
    return subscribeClient.connectToTopic(topic);
  }

  @Override
  public void close() throws Exception {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
    for (final MQTTSubscribeClient subClient : mqttSubscribeClientMap.values()) {
      subClient.disconnect();
    }

    for (final MqttClient mqttClient : mqttPublisherMap.values()) {
      try {
        mqttClient.disconnect();
      } catch (final Exception e) {
        // do nothing
      }
    }
    mqttPublisherMap.clear();
  }
}
