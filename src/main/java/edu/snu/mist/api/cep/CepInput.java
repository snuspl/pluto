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

import edu.snu.mist.common.types.*;

import java.util.*;

/**
 * Default implementation class for CepInput.
 */
public final class CepInput {

  private final CepInputType cepInputType;
  private final Map<String, Object> cepInputConfiguration;
  private final List<Tuple2<String, CepValueType>> fields;
  private final String separator;
  private static final String DEFAULT_SEPARATOR = ",";

  /**
   * Makes an immutable CepInput from InnerBuilder. Should not be exposed to public.
   * @param cepInputTypeParam cep input type given by builder
   * @param cepInputConfigurationParam eep input configuration given by builder
   * @param fieldsParam properties given by builder
   * @param separatorParam cep input separator
   */
  private CepInput(final CepInputType cepInputTypeParam,
                   final Map<String, Object> cepInputConfigurationParam,
                   final List<Tuple2<String, CepValueType>> fieldsParam,
                   final String separatorParam) {
    this.cepInputType = cepInputTypeParam;
    this.cepInputConfiguration = cepInputConfigurationParam;
    this.fields = fieldsParam;
    this.separator = separatorParam;
  }

  /**
   * Makes an immutable CepInput from InnerBuilder. Should not be exposed to public.
   * The default separator is comma.
   * @param cepInputTypeParam cep input type given by builder
   * @param cepInputConfigurationParam eep input configuration given by builder
   * @param fieldsParam properties given by builder
   */
  private CepInput(final CepInputType cepInputTypeParam,
                   final Map<String, Object> cepInputConfigurationParam,
                   final List<Tuple2<String, CepValueType>> fieldsParam) {
      this(cepInputTypeParam, cepInputConfigurationParam, fieldsParam, DEFAULT_SEPARATOR);
  }

  /**
   * @return input type of this input
   */
  public CepInputType getInputType() {
    return cepInputType;
  }

  /**
   * @return source configuration values
   */
  public Map<String, Object> getSourceConfiguration() {
    return cepInputConfiguration;
  }

  /**
   * @return input field fields name and their types
   */
  public List<Tuple2<String, CepValueType>> getFields() {
    return fields;
  }

  /**
   * @return separator of this input
   */
  public String getSeparator() {
      return separator;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepInput)) {
      return false;
    }
    final CepInput input = (CepInput) o;
    return this.cepInputType.equals(input.cepInputType)
        && this.cepInputConfiguration.equals(input.cepInputConfiguration)
        && this.fields.equals(input.fields)
        && this.separator.equals(input.separator);
  }

  @Override
  public int hashCode() {
    return cepInputType.hashCode() * 1000 + cepInputConfiguration.hashCode() * 100
            + fields.hashCode() * 10 + separator.hashCode();
  }

  /**
   * A builder class for CepInput.
   */
  private static final class InnerBuilder {

    private CepInputType cepInputType;
    private final Map<String, Object> cepInputConfiguration;
    private final List<Tuple2<String, CepValueType>> fields;
    private String separator;
    private final Set<String> propertyNames;

    private static final String DEFAULT_SEPARATOR = ",";

    private InnerBuilder() {
      this.cepInputType = null;
      this.cepInputConfiguration = new HashMap<>();
      this.fields = new ArrayList<>();
      this.separator = DEFAULT_SEPARATOR;
      this.propertyNames = new HashSet<>();
    }

    /**
     * Add input source type information (Kafka, Socket, ...).
     * @param cepInputTypeParam
     * @return updated cep input builder
     */
    private InnerBuilder setSourceType(final CepInputType cepInputTypeParam) {
      if (cepInputType != null) {
        throw new IllegalStateException("Cep input type cannot be declared twice!");
      }
      this.cepInputType = cepInputTypeParam;
      return this;
    }

      /**
       * Set separator information (, : blank ...).
       * @param separatorParam
       * @return cep input builder
       */
    private InnerBuilder setSeparator(final String separatorParam) {
        this.separator = separatorParam;
        return this;
    }

    /**
     * Add configuration values eligible for the input source type.
     * @param key configuration key
     * @param value configuration value
     * @return cep input builder
     */
    private InnerBuilder addInputConfigValue(final String key, final Object value) {
      if (cepInputConfiguration.containsKey(key)) {
        throw new IllegalStateException("Duplicated cep input key! Key: " + key);
      }
      cepInputConfiguration.put(key, value);
      return this;
    }

    /**
     * Add property information for inputs.
     * @param fieldName name of the field
     * @param valueType type of the field value
     * @return cep input builder
     */
    private InnerBuilder addField(final String fieldName, final CepValueType valueType) {
      if (propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      fields.add(new Tuple2<>(fieldName, valueType));
      return this;
    }

    /**
     * Creates an immutable Cep input.
     * @return new cep input
     */
    private CepInput build() {
      return new CepInput(this.cepInputType, this.cepInputConfiguration, this.fields, this.separator);
    }
  }

  /*
   * A builder class for Inputs using Text Sockets as inputs.
   */
  public static final class TextSocketBuilder {

    private final String socketInputAddressKey = "SOCKET_INPUT_ADDRESS";
    private final String socketInputPortKey = "SOCKET_INPUT_PORT";
    private InnerBuilder builder;

    public TextSocketBuilder() {
      this.builder = new InnerBuilder()
          .setSourceType(CepInputType.TEXT_SOCKET_SOURCE);
    }

    /**
     * A helper method for setting socket address name configuration.
     * @param socketStreamAddress socket address
     * @return cep socket input builder
     */
    public TextSocketBuilder setSocketAddress(final String socketStreamAddress) {
      builder.addInputConfigValue(socketInputAddressKey, socketStreamAddress);
      return this;
    }

    /**
     * A helper method for setting socket address port configuration.
     * @param socketStreamPort socket port
     * @return cep socket input builder
     */
    public TextSocketBuilder setSocketPort(final int socketStreamPort) {
      builder.addInputConfigValue(socketInputPortKey, socketStreamPort);
      return this;
    }

    /**
     * A helper method for setting separator.
     * @param separator
     * @return cep socket input builder
     */
    public TextSocketBuilder setSeparator(final String separator) {
      builder.setSeparator(separator);
      return this;
    }

    /**
     * Add property information for inputs.
     * @param fieldName name of the field
     * @param valueType type of the field value
     * @return cep input builder
     */
    public TextSocketBuilder addField(final String fieldName, final CepValueType valueType) {
      if (builder.propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      builder.addField(fieldName, valueType);
      return this;
    }

    /**
     * @return a new CepInput
     */
    public CepInput build() {
      return builder.build();
    }
  }

    /*
     * A builder class for Inputs using MQTT as inputs.
     */
    public static final class MqttBuilder {

        private final String mqttInputBrokerURI = "MQTT_INPUT_BROKER_URI";
        private final String mqttInputTopic = "MQTT_INPUT_TOPIC";
        private final InnerBuilder builder;

        public MqttBuilder() {
            this.builder = new InnerBuilder()
                    .setSourceType(CepInputType.MQTT_SOURCE);
        }

        /**
         * A helper method for setting mqtt broker URI configuration.
         * @param mqttBrokerURI mqtt broker URI
         * @return cep mqtt input builder
         */
        public MqttBuilder setMqttBrokerURI(final String mqttBrokerURI) {
            builder.addInputConfigValue(mqttInputBrokerURI, mqttBrokerURI);
            return this;
        }

        /**
         * A helper method for setting mqtt topic configuration.
         * @param mqttTopic mqtt topic
         * @return cep mqtt input builder
         */
        public MqttBuilder setMqttTopic(final String mqttTopic) {
            builder.addInputConfigValue(mqttInputTopic, mqttTopic);
            return this;
        }

        /**
         * A helper method for setting separator.
         * @param separator
         * @return cep mqtt input builder
         */
        public MqttBuilder setSeparator(final String separator) {
            builder.setSeparator(separator);
            return this;
        }

        /**
         * Add property information for inputs.
         * @param fieldName name of the field
         * @param valueType type of the field value
         * @return cep input builder
         */
        public MqttBuilder addField(final String fieldName, final CepValueType valueType) {
            if (builder.propertyNames.contains(fieldName)) {
                throw new IllegalStateException("Duplicated property name");
            }
            builder.addField(fieldName, valueType);
            return this;
        }

        /**
         * @return a new CepInput
         */
        public CepInput build() {
            return builder.build();
        }
    }
}
