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

import org.apache.reef.io.network.Message;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class receives DriverTaskMessages and handles the messages.
 * This can collect MistTask's information.
 */
final class DriverTaskMessageHandler implements EventHandler<Message<DriverTaskMessage>> {
  private static final Logger LOG = Logger.getLogger(DriverTaskMessageHandler.class.getName());

  @Inject
  private DriverTaskMessageHandler() {
    // TODO[MIST-#]: Implement DriverTaskMessage, its codec and handler
  }

  @Override
  public void onNext(final Message<DriverTaskMessage> controlMessageMessage) {
    LOG.log(Level.INFO, "Receives a message {0} from {1}",
        new Object[] {controlMessageMessage.getData(), controlMessageMessage.getSrcId()});
    // TODO[MIST-#]: Implement DriverTaskMessage, its codec and handler
    throw new RuntimeException("DriverTaskMessageHandler is not implemented yet");
  }
}
