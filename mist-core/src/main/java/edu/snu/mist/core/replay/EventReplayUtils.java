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
package edu.snu.mist.core.replay;

import org.apache.reef.io.Tuple;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is used for replaying events with the replay server.
 * It sends REST API calls to the replay server.
 * The server must support these functions accordingly.
 */
public final class EventReplayUtils {

  private static final Logger LOG = Logger.getLogger(EventReplayUtils.class.getName());

  private EventReplayUtils() {
    // empty constructor
  }

  /**
   * Sends a POST to make an mqtt connection and subscribe request topic.
   * This method is the equivalent to :
   * > curl -X POST http://(replayAddress):(replayPort)/subscribe \
   *     -H 'Content-Type: application/json' \
   *     -d '{
   *        "address": "(address)",
   *        "port": (port),
   *        "topic": "(topic)"
   *        }'
   * @return true on success, else false
   */
  public static boolean subscribe(final String replayAddress, final int replayPort,
                                  final String address, final int port, final String topic) {
    try {
      final URL url = new URL(getReplayServerUrl(replayAddress, replayPort) + "/subscribe");
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);

      final OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
      writer.write(getSubscribeRequestBody(address, port, topic));
      writer.close();

      if (conn.getResponseCode() != 200) {
        LOG.log(Level.WARNING, "Failed : HTTP error code is {0}. Failure Response message : {1}",
            new Object[]{conn.getResponseCode(), conn.getResponseMessage()});

        // End the connection.
        conn.disconnect();
        return false;
      }

      // End the connection.
      conn.disconnect();
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Sends a GET to retrieve data within a timestamp period.
   * This method is the equivalent to :
   * > curl -X GET http://(replayAddress):(replayPort)/replay \
   *     -H 'Content-Type: application/json'
   */
  public static EventReplayResult replay(final String replayAddress,
                                         final int replayPort) {
    return replay(replayAddress, replayPort, -1, -1);
  }

  /**
   * Sends a GET to retrieve data within a timestamp period.
   * This method is the equivalent to :
   * > curl -X GET http://(replayAddress):(replayPort)/replay/(startTimestamp) \
   *     -H 'Content-Type: application/json'
   */
  public static EventReplayResult replay(final String replayAddress,
                                         final int replayPort,
                                         final long startTimestamp) {
    return replay(replayAddress, replayPort, startTimestamp, -1);
  }

  /**
   * Sends a GET to retrieve data within a timestamp period.
   * This method is the equivalent to :
   * > curl -X GET http://(replayAddress):(replayPort)/replay/(startTimestamp)/(endTimestamp) \
   *     -H 'Content-Type: application/json'
   */
  public static EventReplayResult replay(final String replayAddress,
                                         final int replayPort,
                                         final long startTimestamp,
                                         final long endTimestamp) {
    try {
      final String urlString = getReplayServerUrl(replayAddress, replayPort) + "/replay";
      final URL url;

      if (startTimestamp == -1) {
        url = new URL(urlString);
      } else {
        if (endTimestamp == -1) {
          url = new URL(urlString + "/" + startTimestamp);
        } else {
          url = new URL(urlString + "/" + startTimestamp + "/" + endTimestamp);
        }
      }

      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Content-Type", "application/json");

      if (conn.getResponseCode() != 200) {
        LOG.log(Level.WARNING, "Failed : HTTP error code is {0}. Failure Response message : {1}",
            new Object[]{conn.getResponseCode(), conn.getResponseMessage()});

        // End the connection.
        conn.disconnect();
        return new EventReplayResult(false, null);
      }

      // Get the output from the server
      final BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
      StringBuilder concatenatedOutput = new StringBuilder();
      String tmpOutput;

      LOG.log(Level.INFO, "Starting to get events to replay from the server.");
      while ((tmpOutput = br.readLine()) != null) {
        concatenatedOutput.append(tmpOutput);
      }
      br.close();

      final JSONParser parser = new JSONParser();
      final JSONObject resultObject = (JSONObject) parser.parse(concatenatedOutput.toString());

      // The map that has a broker(address:port) as a key, and a list of events as a value.
      final Map<String, Object> brokerEventMap;
      Object uncastBrokerEventMap = resultObject.get("result");
      if (uncastBrokerEventMap instanceof Map) {
        brokerEventMap = (Map<String, Object>) uncastBrokerEventMap;
      } else {
        LOG.log(Level.WARNING, "Backup server does not return the broker and events as expected.");
        return new EventReplayResult(false, null);
      }

      // The map that has a broker(address:port) as a key, and a list of events as a value.
      final Map<String, Map<String, List<Tuple<Long, MqttMessage>>>> brokerMqttMessageMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : brokerEventMap.entrySet()) {
        final JSONArray eventList = (JSONArray) entry.getValue();
        final Map<String, List<Tuple<Long, MqttMessage>>> mqttMessages = new HashMap<>();
        for (final Object eventObject : eventList) {
          final JSONArray event = (JSONArray) eventObject;
          final String topic = event.get(1).toString();
          final MqttMessage mqttMessage = new MqttMessage(event.get(2).toString().getBytes());
          List<Tuple<Long, MqttMessage>> timestampAndMqttMessageList = mqttMessages.get(topic);
          if (timestampAndMqttMessageList == null) {
            timestampAndMqttMessageList = new ArrayList<>();
            mqttMessages.put(topic, timestampAndMqttMessageList);
          }
          timestampAndMqttMessageList.add(new Tuple<>(Long.valueOf(event.get(0).toString()), mqttMessage));
        }
        brokerMqttMessageMap.put(entry.getKey(), mqttMessages);
      }

      // End the connection.
      conn.disconnect();
      return new EventReplayResult(true, brokerMqttMessageMap);
    } catch (Exception e) {
      LOG.log(Level.WARNING, "No server was found, or another error has occurred.");
      return new EventReplayResult(false, null);
    }
  }

  /**
   * Removes the data with timestamps faster than the current time.
   * This method is the equivalent to :
   * > curl -X GET http://(replayAddress):(replayPort)/checkpoint \
   *     -H 'Content-Type: application/json'
   */
  public static boolean removeOnCheckpoint(final String replayAddress, final int replayPort) {
    return removeOnCheckpoint(replayAddress, replayPort, -1);
  }


  /**
   * Removes the data with timestamps faster than the given timestamp.
   * This method is the equivalent to :
   * > curl -X GET http://(replayAddress):(replayPort)/checkpoint \
   *     -H 'Content-Type: application/json' \
   *     -d '{
   *        "timestamp": "(timestamp)"
   *        }'
   */
  public static boolean removeOnCheckpoint(final String replayAddress, final int replayPort, final long timestamp) {
    try {
      final URL url = new URL(getReplayServerUrl(replayAddress, replayPort) + "/checkpoint");
      final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json");
      conn.setDoOutput(true);

      if (timestamp != -1L) {
        final OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream(), "UTF-8");
        writer.write("{\"timestamp\": \"" + timestamp + "\"}");
        writer.close();
      }

      if (conn.getResponseCode() != 200) {
        LOG.log(Level.WARNING, "Failed : HTTP error code is {0}. Failure Response message : {1}",
            new Object[]{conn.getResponseCode(), conn.getResponseMessage()});

        // End the connection.
        conn.disconnect();
        return false;
      }

      // The returned content can be ignored.

      // End the connection.
      conn.disconnect();
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  private static String getReplayServerUrl(final String replayAddress, final int replayPort) {
    return "http://" + replayAddress + ":" + replayPort;
  }

  private static String getSubscribeRequestBody(final String address, final int port, final String topic) {
    return "{\"address\": \"" + address + "\", \"port\": " +  port + ", \"topic\": \"" + topic + "\"}";
  }
}
