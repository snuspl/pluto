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

import edu.snu.mist.core.utils.MqttUtils;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class MockReplayServer {

  private static final Logger LOG = Logger.getLogger(MockReplayServer.class.getName());

  /**
   * The port number of this mock replay server.
   */
  private int portNum;

  /**
   * The server socket of this server.
   */
  private ServerSocket serverSocket;

  /**
   * The broker address and stored events.
   */
  private Map<String, List<List<Object>>> brokerEventListMap;

  /**
   * The Mqtt client to subscribe to the broker.
   */
  private MqttClient subClient;

  public MockReplayServer(final int portNum) {
    this.portNum = portNum;
    this.brokerEventListMap = new HashMap<>();
  }

  /**
   * Method to start and run the server.
   */
  public void startServer() {
    try {
      subClient = new MqttClient(MqttUtils.BROKER_URI, "testSubClient");

      serverSocket = new ServerSocket(portNum);
      LOG.log(Level.INFO, "The mock server was successfully started.");
      while (!serverSocket.isClosed()) {
        // If the socket is "close"d by another thread, the accept() method throws a SocketException.
        final Socket client = serverSocket.accept();
        // Get input and output streams to talk to the client.
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        final PrintWriter printWriter = new PrintWriter(client.getOutputStream(), true);

        // Parse the input.
        String request = bufferedReader.readLine(); // The request line, such as "GET /replay HTTP/1.1".
        String[] requestParam = request.split(" ");
        String url = requestParam[1];
        if (url.startsWith("/replay")) {
          handleReplay(url, printWriter);
        } else if (url.equals("/checkpoint")) {
          handleCheckpoint(bufferedReader, printWriter);
        } else if (url.equals("/subscribe")) {
          handleSubscribe(bufferedReader, printWriter);
        }
        Thread.sleep(1000);
        bufferedReader.close();
        printWriter.close();
      }
    } catch (final SocketException e) {
      LOG.log(Level.INFO, "The mock server was closed.");
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Method to close the server.
   */
  public void closeServer() {
    try {
      if (subClient.isConnected()) {
        subClient.disconnect();
      }
      serverSocket.close();
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * A method to handle replay requests.
   */
  private void handleReplay(final String url,
                           final PrintWriter printWriter) throws IOException {
    sendHeader(printWriter);

    // Send the mqtt messages according to the url parameters.
    final String[] urlParams = url.substring(1).split("/");
    final Map<String, Map<String, List<List<Object>>>> eventMap = new HashMap<>();
    if (urlParams.length == 1) {
      // replay all events
      eventMap.put("result", brokerEventListMap);
    } else if (urlParams.length >= 2) {
      // The starting timestamp.
      final Long startTimestamp = Long.parseLong(urlParams[1]);
      Long endTimestamp = null;
      if (urlParams.length == 3) {
        endTimestamp = Long.parseLong(urlParams[2]);
      }
      final Map<String, List<List<Object>>> filteredBrokerEventListMap = new HashMap<>();
      for (final Map.Entry<String, List<List<Object>>> entry : brokerEventListMap.entrySet()) {
        final String brokerAddress = entry.getKey();
        final List<List<Object>> allEvents = entry.getValue();
        final List<List<Object>> filteredEvents = new ArrayList<>();
        for (final List<Object> event : allEvents) {
          final Long timestamp = (Long) event.get(0);
          if (timestamp >= startTimestamp) {
            if (endTimestamp == null) {
              filteredEvents.add(event);
            } else if (timestamp <= endTimestamp) {
              filteredEvents.add(event);
            }
          }
        }
        filteredBrokerEventListMap.put(brokerAddress, filteredEvents);
      }
      eventMap.put("result", filteredBrokerEventListMap);
    }
    // Output the finished JSON response.
    JSONObject.writeJSONString(eventMap, printWriter);
    printWriter.flush();
  }

  /**
   * A method to handle checkpoint requests.
   */
  private void handleCheckpoint(final BufferedReader bufferedReader,
                                final PrintWriter printWriter) {
    try {
      sendHeader(printWriter);
      String currentLine = bufferedReader.readLine();
      while (currentLine != null && !currentLine.equals("")) {
        currentLine = bufferedReader.readLine();
      }
      // The line after the empty line is the body.
      currentLine = bufferedReader.readLine();
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> contentMap = mapper.readValue(currentLine,
          new TypeReference<Map<String, String>>() {
          });
      final Long checkpointTimestamp = Long.parseLong(contentMap.get("timestamp"));
      for (final Map.Entry<String, List<List<Object>>> me : brokerEventListMap.entrySet()) {
        final List<List<Object>> newEventList = new ArrayList<>();
        final List<List<Object>> eventList = me.getValue();
        for (final List<Object> event : eventList) {
          final Long eventTimestamp = (Long) event.get(0);
          if (eventTimestamp > checkpointTimestamp) {
            newEventList.add(event);
          }
        }
        if (newEventList.size() == 0) {
          brokerEventListMap.remove(me.getKey());
        } else {
          brokerEventListMap.put(me.getKey(), newEventList);
        }
      }
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * A method to handle subscribe reqeusts.
   */
  private void handleSubscribe(final BufferedReader bufferedReader,
                              final PrintWriter printWriter) {
    try {
      sendHeader(printWriter);
      String currentLine = bufferedReader.readLine();
      while (currentLine != null && !currentLine.equals("")) {
        currentLine = bufferedReader.readLine();
      }
      // The line after the empty line is the body.
      currentLine = bufferedReader.readLine();
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> contentMap = mapper.readValue(currentLine,
          new TypeReference<Map<String, String>>() { });

      if (!subClient.isConnected()) {
        subClient.connect();
        subClient.setCallback(new TestMqttSubscriber());
      }
      subClient.subscribe(contentMap.get("topic"));
    } catch (final Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Add an event to this replay server.
   */
  private void addEvent(final Long timestamp,
                        final String topic,
                        final String mqttMessage) {
    final List<Object> newEvent = new ArrayList<>();
    newEvent.add(timestamp);
    newEvent.add(topic);
    final StringBuilder message = new StringBuilder();
    message.append("{\"text\": ");
    message.append("\"" + mqttMessage + "\", ");
    message.append("\"start_timestamp\": ");
    message.append(timestamp.toString() + "}");
    newEvent.add(message.toString());
    List<List<Object>> eventList = brokerEventListMap.getOrDefault(MqttUtils.BROKER_URI, new ArrayList<>());
    eventList.add(newEvent);
    brokerEventListMap.put(MqttUtils.BROKER_URI, eventList);
  }

  public boolean isClosed() {
    return serverSocket.isClosed();
  }

  /**
   * Send a header that indicates success.
   */
  private void sendHeader(final PrintWriter printWriter) {
    final StringBuilder header = new StringBuilder();
    header.append("HTTP/1.1 200 OK\n");
    header.append("Content-Type: application/json\n");
    printWriter.write(header.toString());
    printWriter.println();
  }

  /**
   * Mqtt subscriber that is used in the sink test.
   */
  private final class TestMqttSubscriber implements MqttCallback {
    @Override
    public void connectionLost(final Throwable cause) {
      // do nothing
    }

    @Override
    public void messageArrived(final String topic, final MqttMessage message) throws Exception {
      final ObjectMapper mapper = new ObjectMapper();
      final Map<String, String> payloadMap = mapper.readValue(new String(message.getPayload()),
          new TypeReference<Map<String, String>>() { });
      final Long timestamp = Long.parseLong(payloadMap.get("start_timestamp"));
      final String word = payloadMap.get("word");
      addEvent(timestamp, topic, word);
    }

    @Override
    public void deliveryComplete(final IMqttDeliveryToken token) {
      // do nothing
    }
  }
}
