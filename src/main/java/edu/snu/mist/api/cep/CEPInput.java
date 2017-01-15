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

import java.util.List;
import java.util.Map;

/**
 * Default implementation class for CepInput.
 */
public class CepInput {

  private final CepInputType cepInputType;
  private final Map<String, Object> cepInputConfiguration;
  private final List<Tuple<String, CepValueType>> properties;

  /**
   * This constructor makes immutable CepInput.
   * @param cepInputType source type
   * @param cepInputConfiguration input configuration
   * @param properties property fields
   */
  public CepInput(
      final CepInputType cepInputType,
      final Map<String, Object> cepInputConfiguration,
      final List<Tuple<String, CepValueType>> properties) {
    this.cepInputType = cepInputType;
    this.cepInputConfiguration = cepInputConfiguration;
    this.properties = properties;
  }

  public CepInputType getInputType() {
    return cepInputType;
  }

  public Map<String, Object> getSourceConfiguration() {
    return cepInputConfiguration;
  }

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
}
