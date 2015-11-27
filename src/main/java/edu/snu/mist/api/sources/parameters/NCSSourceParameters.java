/*
 * Copyright (C) 2015 Seoul National University
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
 * Parameters for NCSSourceConfigurations.
 */
public final class NCSSourceParameters {

  private NCSSourceParameters() {
    // not called.
  }

  public static final String NAME_SERVER_HOMSTNAME = "NameServerHostName";

  public static final String NAME_SERVICE_PORT = "NameServicePort";

  public static final String SENDER_ID = "SenderID";

  public static final String CONNECTION_ID = "ConnectionID";

  public static final String CODEC = "Codec";
}