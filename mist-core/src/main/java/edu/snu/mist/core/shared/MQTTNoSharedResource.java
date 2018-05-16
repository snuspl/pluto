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

import edu.snu.mist.core.shared.parameters.MaxInflightMqttEventNum;
import edu.snu.mist.core.shared.parameters.MqttSinkKeepAliveSec;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MQTT ClientManager which does not share MQTT Shared Resource.
 */
public final class MQTTNoSharedResource implements MQTTResource {

  /**
   * MQTT publisher id.
   */
  private static final String MQTT_PUBLISHER_ID_PREFIX = "MIST_MQTT_PUBLISHER_";

  /**
   * MQTT subscriber id.
   */
  private static final String MQTT_SUBSCRIBER_ID_PREFIX = "MIST_MQTT_SUBSCRIBER_";

  private final AtomicInteger sourceClientCounter;

  private final AtomicInteger sinkClientCounter;

  private final int maxInflightMqttEventNum;

  private final int mqttSourceKeepAliveSec;

  private final int mqttSinkKeepAliveSec;

  @Inject
  private MQTTNoSharedResource(
      @Parameter(MaxInflightMqttEventNum.class) final int maxInflightMqttEventNumParam,
      @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
      @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec) {
    this.maxInflightMqttEventNum = maxInflightMqttEventNumParam;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
    this.sourceClientCounter = new AtomicInteger(0);
    this.sinkClientCounter = new AtomicInteger(0);
  }

  @Override
  public IMqttAsyncClient getMqttSinkClient(final String brokerURI, final String topic)
      throws MqttException, IOException {
    final IMqttAsyncClient client = new MqttAsyncClient(brokerURI, MQTT_PUBLISHER_ID_PREFIX + sinkClientCounter
        .getAndIncrement());
    final MqttConnectOptions connectOptions = new MqttConnectOptions();
    connectOptions.setMaxInflight(maxInflightMqttEventNum);
    connectOptions.setKeepAliveInterval(mqttSinkKeepAliveSec);
    client.connect(connectOptions).waitForCompletion();
    return client;
  }

  @Override
  public MQTTDataGenerator getDataGenerator(final String brokerURI, final String topic) {
    final MQTTSubscribeClient client = new MQTTSubscribeClient(brokerURI, MQTT_SUBSCRIBER_ID_PREFIX +
        sourceClientCounter.getAndIncrement(), mqttSourceKeepAliveSec);
   return client.connectToTopic(topic);
  }

  @Override
  public void deleteMqttSinkClient(final String brokerURI, final String topic, final IMqttAsyncClient client) {
    // do nothing
  }

  @Override
  public void close() throws Exception {

  }
}