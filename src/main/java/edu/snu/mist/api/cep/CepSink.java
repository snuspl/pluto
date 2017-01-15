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

import java.util.Map;

/**
 * An immutable action created by @CepActionBuilder. It corresponds to Sink in MIST stream query.
 */
public class CepSink {

  private final CepSinkType cepSinkType;
  private final Map<String, Object> actionConfigs;

  /**
   * Creates an immutable action called from ActionBuilder.
   * @param cepSinkType
   * @param actionConfigs
   */
  public CepSink(final CepSinkType cepSinkType, final Map<String, Object> actionConfigs) {
    this.cepSinkType = cepSinkType;
    this.actionConfigs = actionConfigs;
  }

  /**
   * @return Action type
   */
  public CepSinkType getCepSinkType() {
    return cepSinkType;
  }

  /**
   * @return Action configuration values
   */
  public Map<String, Object> getActionConfigs() {
    return actionConfigs;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepSink)) {
      return false;
    }
    final CepSink sink = (CepSink) o;
    return this.cepSinkType == sink.cepSinkType && this.actionConfigs.equals(sink.actionConfigs);
  }

  @Override
  public int hashCode() {
    return cepSinkType.hashCode() * 100 + actionConfigs.hashCode() * 100;
  }
}