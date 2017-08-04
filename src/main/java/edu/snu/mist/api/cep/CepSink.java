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
package edu.snu.mist.api.cep;

import java.util.HashMap;
import java.util.Map;

/**
 * An immutable sink. It corresponds to Sink in MIST stream query.
 */
public final class CepSink {

  private final CepSinkType cepSinkType;
  private final Map<String, Object> sinkConfigs;
  private final String separator;
  private static final String DEFAULT_SEPARATOR = ",";

  /**
   * Creates an immutable sink called from ActionBuilder.
   * @param cepSinkType
   * @param sinkConfigs
   * @param separator
   */
  private CepSink(final CepSinkType cepSinkType, final Map<String, Object> sinkConfigs, final String separator) {
    this.cepSinkType = cepSinkType;
    this.sinkConfigs = sinkConfigs;
    this.separator = separator;
  }

  private CepSink(final CepSinkType cepSinkType, final Map<String, Object> sinkConfigs) {
      this(cepSinkType, sinkConfigs, DEFAULT_SEPARATOR);
  }

  /**
   * @return Sink type
   */
  public CepSinkType getCepSinkType() {
    return cepSinkType;
  }

  /**
   * @return Sink configuration values
   */
  public Map<String, Object> getSinkConfigs() {
    return sinkConfigs;
  }

  /**
   * @return Sink separator
   */
  public String getSeparator() {
      return separator;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepSink)) {
      return false;
    }
    final CepSink sink = (CepSink) o;
    return this.cepSinkType == sink.cepSinkType
            && this.sinkConfigs.equals(sink.sinkConfigs)
            && this.separator.equals(sink.separator);
  }

  @Override
  public int hashCode() {
    return cepSinkType.hashCode() * 100 + sinkConfigs.hashCode() * 10 + separator.hashCode();
  }

  /**
   * A builder class for CepSink.
   */
  private static final class InnerBuilder {
    private CepSinkType cepSinkType;
    private Map<String, Object> actionConfigurations;
    private String separator;
    private static final String DEFAULT_SEPARATOR = ",";

    private InnerBuilder() {
      this.actionConfigurations = new HashMap<>();
      this.separator = DEFAULT_SEPARATOR;
    }

    /**
     * @param cepSinkType the type of this sink
     * @return builder
     */
    private InnerBuilder setCepSinkType(final CepSinkType cepSinkType) {
      if (this.cepSinkType != null) {
        throw new IllegalStateException("Sink type cannot be defined twice!");
      }
      this.cepSinkType = cepSinkType;
      return this;
    }

    private InnerBuilder setSeparator(final String separator) {
        this.separator = separator;
        return this;
    }

    /**
     * @param key configuration key
     * @param value configuration value
     * @return builder
     */
    private InnerBuilder addSinkConfigValue(final String key, final Object value) {
      if (actionConfigurations.containsKey(key)) {
        throw new IllegalStateException("Cannot define the same configuration value more than once!");
      }
      this.actionConfigurations.put(key, value);
      return this;
    }

    private CepSink build() {
      return new CepSink(cepSinkType, actionConfigurations, separator);
    }
  }

  /**
   * A builder for CepSink which uses Text Socket as its output.
   */
  public static final class TextSocketBuilder {

    private final String socketSinkAddressKey = "SOCKET_SINK_ADDRESS";
    private final String socketSinkPortKey = "SOCKET_SINK_PORT";
    private InnerBuilder builder;

    public TextSocketBuilder() {
      this.builder = new InnerBuilder()
          .setCepSinkType(CepSinkType.TEXT_SOCKET_OUTPUT);
    }

    /**
     * Sets socket address.
     * @param socketAddress socket address
     * @return builder
     */
    public TextSocketBuilder setSocketAddress(final String socketAddress) {
      this.builder.addSinkConfigValue(socketSinkAddressKey, socketAddress);
      return this;
    }

    /**
     * Sets the socket port.
     * @param socketPort socket port
     * @return builder
     */
    public TextSocketBuilder setSocketPort(final int socketPort) {
      this.builder.addSinkConfigValue(socketSinkPortKey, socketPort);
      return this;
    }

    /**
     * Sets the separator.
     * @param separatorParam separator parameter
     * @return builder
     */
    public TextSocketBuilder setSeparator(final String separatorParam) {
        this.builder.setSeparator(separatorParam);
        return this;
    }

    /**
     * Creates an immutable CepSink.
     * @return a new CepSink
     */
    public CepSink build() {
      return builder.build();
    }
  }

    /**
     * A builder for CepSink which uses MQTT as its output.
     */
    public static final class MqttBuilder {

        private final String mqttSinkBrokerURI = "MQTT_SINK_BROKER_URI";
        private final String mqttSinkTopic = "MQTT_SINK_TOPIC";
        private InnerBuilder builder;

        public MqttBuilder() {
            this.builder = new InnerBuilder()
                    .setCepSinkType(CepSinkType.MQTT_OUTPUT);
        }

        /**
         * Sets mqtt broker URI.
         * @param mqttBrokerURI mqtt broker URI
         * @return builder
         */
        public MqttBuilder setMqttBrokerURI(final String mqttBrokerURI) {
            this.builder.addSinkConfigValue(mqttSinkBrokerURI, mqttBrokerURI);
            return this;
        }

        /**
         * Sets mqtt topic.
         * @param mqttTopic mqtt topic
         * @return builder
         */
        public MqttBuilder setMqttTopic(final String mqttTopic) {
            this.builder.addSinkConfigValue(mqttSinkTopic, mqttTopic);
            return this;
        }

        /**
         * Sets the separator.
         * @param separatorParam separator parameter
         * @return builder
         */
        public MqttBuilder setSeparator(final String separatorParam) {
            this.builder.setSeparator(separatorParam);
            return this;
        }

        /**
         * Creates an immutable CepSink.
         * @return a new CepSink
         */
        public CepSink build() {
            return builder.build();
        }
    }
}