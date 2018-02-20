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
package edu.snu.mist.api.rulebased;

import edu.snu.mist.common.types.Tuple2;

import java.util.*;

/**
 * Default implementation class for RuleBasedInput.
 */
public final class RuleBasedInput {

  private final RuleBasedInputType inputType;
  private final Map<String, Object> inputConfiguration;
  private final List<Tuple2<String, RuleBasedValueType>> fields;
  private final String separator;
  private static final String DEFAULT_SEPARATOR = ",";

  /**
   * Makes an immutable RuleBasedInput from InnerBuilder. Should not be exposed to public.
   * @param inputTypeParam rule-based input type given by builder
   * @param inputConfigurationParam eep input configuration given by builder
   * @param fieldsParam properties given by builder
   * @param separatorParam rule-based input separator
   */
  private RuleBasedInput(final RuleBasedInputType inputTypeParam,
                   final Map<String, Object> inputConfigurationParam,
                   final List<Tuple2<String, RuleBasedValueType>> fieldsParam,
                   final String separatorParam) {
    this.inputType = inputTypeParam;
    this.inputConfiguration = inputConfigurationParam;
    this.fields = fieldsParam;
    this.separator = separatorParam;
  }

  /**
   * Makes an immutable RuleBasedInput from InnerBuilder. Should not be exposed to public.
   * The default separator is comma.
   * @param inputTypeParam rule-based input type given by builder
   * @param inputConfigurationParam rule-based input configuration given by builder
   * @param fieldsParam properties given by builder
   */
  private RuleBasedInput(final RuleBasedInputType inputTypeParam,
                   final Map<String, Object> inputConfigurationParam,
                   final List<Tuple2<String, RuleBasedValueType>> fieldsParam) {
      this(inputTypeParam, inputConfigurationParam, fieldsParam, DEFAULT_SEPARATOR);
  }

  /**
   * @return input type of this input
   */
  public RuleBasedInputType getInputType() {
    return inputType;
  }

  /**
   * @return source configuration values
   */
  public Map<String, Object> getSourceConfiguration() {
    return inputConfiguration;
  }

  /**
   * @return input field fields name and their types
   */
  public List<Tuple2<String, RuleBasedValueType>> getFields() {
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
    if (!(o instanceof RuleBasedInput)) {
      return false;
    }
    final RuleBasedInput input = (RuleBasedInput) o;
    return this.inputType.equals(input.inputType)
        && this.inputConfiguration.equals(input.inputConfiguration)
        && this.fields.equals(input.fields)
        && this.separator.equals(input.separator);
  }

  @Override
  public int hashCode() {
    return inputType.hashCode() * 1000 + inputConfiguration.hashCode() * 100
            + fields.hashCode() * 10 + separator.hashCode();
  }

  /**
   * A builder class for RuleBasedInput.
   */
  private static final class InnerBuilder {

    private RuleBasedInputType inputType;
    private final Map<String, Object> inputConfiguration;
    private final List<Tuple2<String, RuleBasedValueType>> fields;
    private String separator;
    private final Set<String> propertyNames;

    private static final String DEFAULT_SEPARATOR = ",";

    private InnerBuilder() {
      this.inputType = null;
      this.inputConfiguration = new HashMap<>();
      this.fields = new ArrayList<>();
      this.separator = DEFAULT_SEPARATOR;
      this.propertyNames = new HashSet<>();
    }

    /**
     * Add input source type information (Kafka, Socket, ...).
     * @param inputTypeParam
     * @return updated rule-based input builder
     */
    private InnerBuilder setSourceType(final RuleBasedInputType inputTypeParam) {
      if (inputType != null) {
        throw new IllegalStateException("Rule-based input type cannot be declared twice!");
      }
      this.inputType = inputTypeParam;
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
     * @return rule-based input builder
     */
    private InnerBuilder addInputConfigValue(final String key, final Object value) {
      if (inputConfiguration.containsKey(key)) {
        throw new IllegalStateException("Duplicated rule-based input key! Key: " + key);
      }
      inputConfiguration.put(key, value);
      return this;
    }

    /**
     * Add property information for inputs.
     * @param fieldName name of the field
     * @param valueType type of the field value
     * @return rule-based input builder
     */
    private InnerBuilder addField(final String fieldName, final RuleBasedValueType valueType) {
      if (propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      fields.add(new Tuple2<>(fieldName, valueType));
      return this;
    }

    /**
     * Creates an immutable rule-based input.
     * @return new rule-based input
     */
    private RuleBasedInput build() {
      return new RuleBasedInput(this.inputType, this.inputConfiguration, this.fields, this.separator);
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
          .setSourceType(RuleBasedInputType.TEXT_SOCKET_SOURCE);
    }

    /**
     * A helper method for setting socket address name configuration.
     * @param socketStreamAddress socket address
     * @return rule-based socket input builder
     */
    public TextSocketBuilder setSocketAddress(final String socketStreamAddress) {
      builder.addInputConfigValue(socketInputAddressKey, socketStreamAddress);
      return this;
    }

    /**
     * A helper method for setting socket address port configuration.
     * @param socketStreamPort socket port
     * @return rule-based socket input builder
     */
    public TextSocketBuilder setSocketPort(final int socketStreamPort) {
      builder.addInputConfigValue(socketInputPortKey, socketStreamPort);
      return this;
    }

    /**
     * A helper method for setting separator.
     * @param separator
     * @return rule-based socket input builder
     */
    public TextSocketBuilder setSeparator(final String separator) {
      builder.setSeparator(separator);
      return this;
    }

    /**
     * Add property information for inputs.
     * @param fieldName name of the field
     * @param valueType type of the field value
     * @return rule-based input builder
     */
    public TextSocketBuilder addField(final String fieldName, final RuleBasedValueType valueType) {
      if (builder.propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      builder.addField(fieldName, valueType);
      return this;
    }

    /**
     * @return a new RuleBasedInput
     */
    public RuleBasedInput build() {
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
                    .setSourceType(RuleBasedInputType.MQTT_SOURCE);
        }

        /**
         * A helper method for setting mqtt broker URI configuration.
         * @param mqttBrokerURI mqtt broker URI
         * @return rule-based mqtt input builder
         */
        public MqttBuilder setMqttBrokerURI(final String mqttBrokerURI) {
            builder.addInputConfigValue(mqttInputBrokerURI, mqttBrokerURI);
            return this;
        }

        /**
         * A helper method for setting mqtt topic configuration.
         * @param mqttTopic mqtt topic
         * @return rule-based mqtt input builder
         */
        public MqttBuilder setMqttTopic(final String mqttTopic) {
            builder.addInputConfigValue(mqttInputTopic, mqttTopic);
            return this;
        }

        /**
         * A helper method for setting separator.
         * @param separator
         * @return rule-based mqtt input builder
         */
        public MqttBuilder setSeparator(final String separator) {
            builder.setSeparator(separator);
            return this;
        }

        /**
         * Add property information for inputs.
         * @param fieldName name of the field
         * @param valueType type of the field value
         * @return rule-based input builder
         */
        public MqttBuilder addField(final String fieldName, final RuleBasedValueType valueType) {
            if (builder.propertyNames.contains(fieldName)) {
                throw new IllegalStateException("Duplicated property name");
            }
            builder.addField(fieldName, valueType);
            return this;
        }

        /**
         * @return a new RuleBasedInput
         */
        public RuleBasedInput build() {
            return builder.build();
        }
    }
}
