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
 * An immutable action created by @CepActionBuilder. It corresponds to Sink in MIST stream query.
 */
public final class CepSink {

  private final CepSinkType cepSinkType;
  private final Map<String, Object> actionConfigs;

  /**
   * Creates an immutable sink called from ActionBuilder.
   * @param cepSinkType
   * @param actionConfigs
   */
  private CepSink(final CepSinkType cepSinkType, final Map<String, Object> actionConfigs) {
    this.cepSinkType = cepSinkType;
    this.actionConfigs = actionConfigs;
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

  /**
   * A builder class for CepSink.
   */
  public static final class InnerBuilder {
    private CepSinkType cepSinkType;
    private Map<String, Object> actionConfigurations;

    private InnerBuilder() {
      this.actionConfigurations = new HashMap<>();
    }

    /**
     * @param cepSinkType the type of this sink
     * @return builder
     */
    public InnerBuilder setCepSinkType(final CepSinkType cepSinkType) {
      if (this.cepSinkType != null) {
        throw new IllegalStateException("Sink type cannot be defined twice!");
      }
      this.cepSinkType = cepSinkType;
      return this;
    }

    /**
     * @param key configuration key
     * @param value configuration value
     * @return
     */
    public InnerBuilder addSinkConfigValue(final String key, final Object value) {
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

  public static final class TextSocketBuilder {

    private final String socketSinkAddressKey = "SOCKET_SINK_ADDRESS";
    private final String socketSinkPortKey = "SOCKET_SINK_PORT";
    private InnerBuilder builder;

    public TextSocketBuilder() {
      this.builder = new InnerBuilder()
          .setCepSinkType(CepSinkType.TEXT_SOCKET_OUTPUT);
    }

    public TextSocketBuilder setSocketAddress(final String socketAddress) {
      this.builder.addSinkConfigValue(socketSinkAddressKey, socketAddress);
      return this;
    }

    public TextSocketBuilder setSocketPort(final int socketPort) {
      this.builder.addSinkConfigValue(socketSinkPortKey, socketPort);
      return this;
    }

    public CepSink build() {
      return builder.build();
    }
  }
}