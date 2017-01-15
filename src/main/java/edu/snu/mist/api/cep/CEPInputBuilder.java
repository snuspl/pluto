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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder class for CEPInput.
 */
public final class CepInputBuilder {

  private CepInputType cepInputType;
  private Map<String, Object> cepInputConfiguration;
  private List<Tuple<String, CepValueType>> properties;

  // Should not be exposed to public
  private CepInputBuilder() {
    cepInputType = null;
    cepInputConfiguration = new HashMap<>();
    properties = new ArrayList<>();
  }

  /**
   * Making a new builder.
   * @return
   */
  public static CepInputBuilder newBuilder() {
    return new CepInputBuilder();
  }

  /**
   * Add input source type information (Kafka, Socket, ...).
   * @param cepInputTypeParam
   * @return
   */
  public CepInputBuilder setSourceType(final CepInputType cepInputTypeParam) {
    this.cepInputType = cepInputTypeParam;
    return this;
  }

  /**
   * Add configuration values eligible for the input source type.
   * @param key
   * @param value
   * @return
   */
  public CepInputBuilder addInputConfigValue(final String key, final Object value) {
    cepInputConfiguration.put(key, value);
    return this;
  }

  /**
   * Add property information for inputs.
   * @param propertyName
   * @param valueType
   * @return
   */
  public CepInputBuilder addProperty(final String propertyName, final CepValueType valueType) {
    properties.add(new Tuple<>(propertyName, valueType));
    return this;
  }

  public CepInput build() {
    return new CepInput(cepInputType, cepInputConfiguration, properties);
  }
}
