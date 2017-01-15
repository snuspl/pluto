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
 * A builder class for CepAction.
 */
public final class CepSinkBuilder {

  private CepSinkType cepSinkType;
  private Map<String, Object> actionConfigurations;

  // Should not be exposed to public
  private CepSinkBuilder() {
    this.cepSinkType = null;
    actionConfigurations = new HashMap<>();
  }

  public static CepSinkBuilder newBuilder() {
    return new CepSinkBuilder();
  }

  public CepSinkBuilder setCepSinkType(final CepSinkType cepSinkType) {
    if (this.cepSinkType != null) {
      throw new IllegalStateException("Sink type cannot be defined twice!");
    }
    this.cepSinkType = cepSinkType;
    return this;
  }

  public CepSinkBuilder addActionConfigValue(final String key, final Object value) {
    if (actionConfigurations.containsKey(key)) {
      throw new IllegalStateException("Cannot define the same configuration value more than once!");
    }
    this.actionConfigurations.put(key, value);
    return this;
  }

  public CepSink build() {
    return new CepSink(cepSinkType, actionConfigurations);
  }
}