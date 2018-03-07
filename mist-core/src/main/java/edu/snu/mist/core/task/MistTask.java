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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.MasterHostAddress;
import edu.snu.mist.core.parameters.MasterToTaskPort;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * The submitted queries are handled by QueryManager.
 */
@Unit
public final class MistTask implements Task {
  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  private final QueryManager queryManager;
  private final CheckpointManager checkpointManager;

  /**
   * The avro server for master-to-task communication.
   */
  private final Server masterToTaskServer;

  /**
   * Task-to-master message proxy.
   */
  private final TaskToMasterMessage proxyToMaster;

  /**
   * Default constructor of MistTask.
   * @throws InjectionException
   */
  @Inject
  private MistTask(final QueryManager queryManager,
                   final CheckpointManager checkpointManager,
                   @Parameter(MasterToTaskPort.class) final int masterToTaskPort,
                   @Parameter(MasterHostAddress.class) final String masterHostAddress,
                   @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
                   final MasterToTaskMessage masterToTaskMessage) throws InjectionException, IOException {
    this.countDownLatch = new CountDownLatch(1);
    this.queryManager = queryManager;
    this.masterToTaskServer = AvroUtils.createAvroServer(MasterToTaskMessage.class, masterToTaskMessage,
        new InetSocketAddress(masterToTaskPort));
    final NettyTransceiver nettyTransceiver = new NettyTransceiver(
        new InetSocketAddress(masterHostAddress, taskToMasterPort));
    this.proxyToMaster = SpecificRequestor.getClient(TaskToMasterMessage.class, nettyTransceiver);
    this.checkpointManager = checkpointManager;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    masterToTaskServer.close();
    queryManager.close();
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
