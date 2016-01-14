/*
 * Copyright (C) 2016 Seoul National University
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

package edu.snu.mist.api.sources.parameters;

/**
 * This class contains the list of necessary parameters for NCSSourceConfigurations.
 */
public final class NCSSourceParameters {

  private NCSSourceParameters() {
    // not called.
  }

  /**
   * Hostname of the name server.
   */
  public static final String NAME_SERVER_HOSTNAME = "NameServerHostName";

  /**
   * Port where name service is running.
   */
  public static final String NAME_SERVICE_PORT = "NameServicePort";

  /**
   * String identification of the stream sender.
   */
  public static final String SENDER_ID = "SenderID";

  /**
   * String identification of the target connection.
   */
  public static final String CONNECTION_ID = "ConnectionID";

  /**
   * Codec used for serializing/deserializing in REEF NetworkConnectionService.
   */
  public static final String CODEC = "Codec";
}