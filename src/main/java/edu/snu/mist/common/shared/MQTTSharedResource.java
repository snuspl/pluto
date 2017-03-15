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
   * The map coupling MQTT broker URI and MQTTSubscribeClient.
   */
  private final ConcurrentMap<String, MQTTSubscribeClient> mqttSubscribeClientMap;

  @Inject
  private MQTTSharedResource() {
    this.mqttSubscribeClientMap = new ConcurrentHashMap<>();
  }

  /**
   * Return the MQTTDataGenerator having MQTT client connected with the target broker.
   * @param brokerURI the URI of broker to subscribe
   * @param topic the topic of connected broker to subscribe
   * @return requested MQTTDataGenerator connected with the target broker and topic
   */
  public MQTTDataGenerator getDataGenerator(final String brokerURI,
                                            final String topic) {
    MQTTSubscribeClient subscribeClient
        = new MQTTSubscribeClient(brokerURI, "MISTClient", mqttSubscribeClientMap);
    mqttSubscribeClientMap.putIfAbsent(brokerURI, subscribeClient);
    return mqttSubscribeClientMap.get(brokerURI).connectToTopic(topic);
  }

  @Override
  public void close() throws Exception {
    // TODO: [MIST-489] Deal with close and connection problem in MQTT source
    synchronized (this) {
      for (final MQTTSubscribeClient subClient : mqttSubscribeClientMap.values()) {
        subClient.disconnect();
      }
    }
  }
}
