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

import org.apache.avro.ipc.Server;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * The submitted queries are handled by QueryReceiver.
 */
@Unit
public final class MistTask implements Task {
  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  private final Server server;
  private final QueryReceiver receiver;

  /**
   * Default constructor of MistTask.
   * @param server rpc server for receiving queries
   * @throws InjectionException
   */
  @Inject
  private MistTask(final Server server,
                   final QueryReceiver receiver) throws InjectionException {
    this.countDownLatch = new CountDownLatch(1);
    this.server = server;
    this.receiver = receiver;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    server.close();
    receiver.close();
    return new byte[0];
  }

  /**
   * A handler for closing the task.
   */
  public final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOG.log(Level.INFO, "Closing Task");
      countDownLatch.countDown();
    }
  }
}
