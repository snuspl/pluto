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

import org.apache.reef.io.Tuple;

import java.util.*;

/**
 * Default implementation class for CepInput.
 */
public final class CepInput {

  private final CepInputType cepInputType;
  private final Map<String, Object> cepInputConfiguration;
  private final List<Tuple<String, CepValueType>> properties;

  /**
   * This constructor makes immutable CepInput.
   * @param innerBuilder Cep input builder.
   */
  private CepInput(final InnerBuilder innerBuilder) {
    this.cepInputType = innerBuilder.cepInputType;
    this.cepInputConfiguration = innerBuilder.cepInputConfiguration;
    this.properties = innerBuilder.fields;
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
  public List<Tuple<String, CepValueType>> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepInput)) {
      return false;
    }
    final CepInput input = (CepInput) o;
    return this.cepInputType.equals(input.cepInputType)
        && this.cepInputConfiguration.equals(input.cepInputConfiguration)
        && this.properties.equals(input.properties);
  }

  @Override
  public int hashCode() {
    return cepInputType.hashCode() * 100 + cepInputConfiguration.hashCode() * 100 + properties.hashCode();
  }

  /**
   * A builder class for CepInput.
   */
  private static final class InnerBuilder {

    private CepInputType cepInputType;
    private final Map<String, Object> cepInputConfiguration;
    private final List<Tuple<String, CepValueType>> fields;
    private final Set<String> propertyNames;

    private InnerBuilder() {
      this.cepInputType = null;
      this.cepInputConfiguration = new HashMap<>();
      this.fields = new ArrayList<>();
      this.propertyNames = new HashSet<>();
    }

    /**
     * Add input source type information (Kafka, Socket, ...).
     * @param cepInputTypeParam
     * @return updated cep input builder
     */
    public InnerBuilder setSourceType(final CepInputType cepInputTypeParam) {
      if (cepInputType != null) {
        throw new IllegalStateException("Cep input type cannot be declared twice!");
      }
      this.cepInputType = cepInputTypeParam;
      return this;
    }

    /**
     * Add configuration values eligible for the input source type.
     * @param key configuration key
     * @param value configuration value
     * @return cep input builder
     */
    public InnerBuilder addInputConfigValue(final String key, final Object value) {
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
    public InnerBuilder addField(final String fieldName, final CepValueType valueType) {
      if (propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      fields.add(new Tuple<>(fieldName, valueType));
      return this;
    }

    /**
     * Creates an immutable Cep input.
     * @return new cep input
     */
    public CepInput build() {
      return new CepInput(this);
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
     * Add property information for inputs.
     * @param fieldName name of the field
     * @param valueType type of the field value
     * @return cep input builder
     */
    public TextSocketBuilder addField(final String fieldName, final CepValueType valueType) {
      if (builder.propertyNames.contains(fieldName)) {
        throw new IllegalStateException("Duplicated property name");
      }
      builder.fields.add(new Tuple<>(fieldName, valueType));
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
