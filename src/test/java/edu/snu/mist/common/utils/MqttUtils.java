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
package edu.snu.mist.common.utils;

import io.moquette.server.Server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * MQTT utility class.
 */
public final class MqttUtils {

  /**
   * The host address of the broker.
   */
  public static final String HOST = "127.0.0.1";

  /**
   * The port of the broker.
   */
  public static final String PORT = "9121";

  /**
   * The web socket port of the broker.
   */
  public static final String WEBSOKET_PORT = "9122";

  /**
   * The broker URI.
   */
  public static final String BROKER_URI =
      new StringBuilder().append("tcp://").append(HOST).append(":").append(PORT).toString();

  /**
   * The directory path prefix of the broker setting.
   */
  private static final String TMP_DIR = "/tmp/mist";

  public static final String DIR_PATH_PREFIX = TMP_DIR + "/MQTTTest-";

  private MqttUtils() {

  }

  /**
   * Create an Mqtt broker.
   * @return mqtt broker
   * @throws IOException
   */
  public static Server createMqttBroker() throws IOException {
    // Create tmp folder
    final File tmpDir = new File(TMP_DIR);

    if (!tmpDir.exists()) {
      System.out.println("creating directory: " + tmpDir.getName());
      boolean result = false;

      try {
        tmpDir.mkdir();
        result = true;
      } catch (final SecurityException se) {
        // do nothing
      }

      if (result) {
        System.out.println(tmpDir.getName() + " created");
      }
    }

    // create local mqtt broker
    final Properties brokerProps = new Properties();
    brokerProps.put("port", PORT);
    brokerProps.put("host", HOST);
    brokerProps.put("websocket_port", WEBSOKET_PORT);
    brokerProps.put("allow_anonymous", "true");
    brokerProps.put("persistent_store", new StringBuilder()
        .append(DIR_PATH_PREFIX)
        .append((Long)System.currentTimeMillis())
        .append(".mapdb")
        .toString());
    final Server mqttBroker = new Server();
    mqttBroker.startServer(brokerProps);
    return mqttBroker;
  }
}
