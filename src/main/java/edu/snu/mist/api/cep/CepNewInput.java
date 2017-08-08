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

import java.util.*;

/**
 * Default implementation class for CepInput.
 */
public final class CepNewInput {

    private final CepInputType cepInputType;
    private final Map<String, Object> cepInputConfiguration;
    private final CepClassType cepClass;

    /**
     * Makes an immutable CepInput from InnerBuilder. Should not be exposed to public.
     * @param cepInputTypeParam cep input type given by builder
     * @param cepInputConfigurationParam cep input configuration given by builder
     * @param cepClass User-defined cep class
     */
    private CepNewInput(
            final CepInputType cepInputTypeParam,
            final Map<String, Object> cepInputConfigurationParam,
            final CepClassType cepClass) {
        this.cepInputType = cepInputTypeParam;
        this.cepInputConfiguration = cepInputConfigurationParam;
        this.cepClass = cepClass;
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
     * @return User-defined class
     */
    public CepClassType getCepClass() {
        return cepClass;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CepNewInput that = (CepNewInput) o;

        if (cepInputType != that.cepInputType) {
            return false;
        }
        if (!cepInputConfiguration.equals(that.cepInputConfiguration)) {
            return false;
        }
        return cepClass.equals(that.cepClass);
    }

    @Override
    public int hashCode() {
        int result = cepInputType.hashCode();
        result = 31 * result + cepInputConfiguration.hashCode();
        result = 31 * result + cepClass.hashCode();
        return result;
    }

    /**
     * A builder class for CepInput.
     */
    private static final class InnerBuilder {

        private CepInputType cepInputType;
        private final Map<String, Object> cepInputConfiguration;
        private CepClassType cepClass;

        private InnerBuilder() {
            this.cepInputType = null;
            this.cepInputConfiguration = new HashMap<>();
            this.cepClass = null;
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
            cepInputType = cepInputTypeParam;
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
         * Set user-defined cep class.
         * @param cepClassParam user-defined class
         * @return cep input builder
         */
        private InnerBuilder setClass(final CepClassType cepClassParam) {
            if (cepClass != null) {
                throw new IllegalStateException("Cep class cannot be declared twice!");
            }
            cepClass = cepClassParam;
            return this;
        }

        /**
         * Creates an immutable Cep input.
         * @return new cep input
         */
        private CepNewInput build() {
            return new CepNewInput(cepInputType, cepInputConfiguration, cepClass);
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

        public TextSocketBuilder setClass(final CepClassType cepClass) {
            builder.setClass(cepClass);
            return this;
        }

        /**
         * @return a new CepInput
         */
        public CepNewInput build() {
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

        public MqttBuilder setClass(final CepClassType cepClass) {
            builder.setClass(cepClass);
            return this;
        }

        /**
         * @return a new CepInput
         */
        public CepNewInput build() {
            return builder.build();
        }
    }
}
