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
package edu.snu.mist.core.driver;

import org.apache.reef.wake.remote.Codec;

import javax.inject.Inject;

/**
 * A codec for DriverTaskMessage.
 */
final class DriverTaskMessageCodec implements Codec<DriverTaskMessage> {

  @Inject
  private DriverTaskMessageCodec() {
    // TODO[MIST-#]: Implement DriverTaskMessage, its codec and handler
  }

  @Override
  public DriverTaskMessage decode(final byte[] bytes) {
    // TODO[MIST-#]: Implement DriverTaskMessage, its codec and handler
    throw new RuntimeException("ControlMessageCodec.decode is not implemented yet.");
  }

  @Override
  public byte[] encode(final DriverTaskMessage driverTaskMessage) {
    // TODO[MIST-#]: Implement DriverTaskMessage, its codec and handler
    throw new RuntimeException("ControlMessageCodec.encode is not implemented yet.");
  }
}

