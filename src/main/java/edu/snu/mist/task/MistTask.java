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
package edu.snu.mist.task;

import edu.snu.mist.formats.avro.ClientToTaskMessage;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * The actual query submission logic is performed by QuerySubmitter.
 */
public final class MistTask implements Task {
  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  /**
   * Default constructor of MistTask.
   * @param clientToTaskMessage logical plan receiver
   * @throws InjectionException
   */
  @Inject
  private MistTask(final ClientToTaskMessage clientToTaskMessage) throws InjectionException {
    this.countDownLatch = new CountDownLatch(1);
  }


  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    return new byte[0];
  }
}
