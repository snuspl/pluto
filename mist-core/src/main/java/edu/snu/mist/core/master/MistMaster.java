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
package edu.snu.mist.core.master;

import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.ipc.Server;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The MistMaster which coordinates MistTasks in distributed environments.
 */
@Unit
public final class MistMaster implements Task {

  private static final Logger LOG = Logger.getLogger(MistMaster.class.getName());

  private final CountDownLatch countDownLatch;

  /**
   * Shared tang object.
   */
  private Tang tang = Tang.Factory.getTang();

  /**
   * Avro RPC servers.
   */
  private final Server driverToMasterServer;

  private final Server clientToMasterServer;

  private final Server taskToMasterServer;

  @Inject
  private MistMaster(
      @Parameter(DriverToMasterPort.class) final int driverToMasterPort,
      @Parameter(ClientToMasterPort.class) final int clientToMasterPort,
      @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
      @Parameter(MasterToTaskPort.class) final int masterToTaskPort,
      @Parameter(NumTasks.class) final int initialTaskNum,
      final DriverToMasterMessage driverToMasterMessage,
      final ClientToMasterMessage clientToMasterMessage,
      final TaskToMasterMessage taskToMasterMessage,
      final TaskRequestor taskRequestor,
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      final MasterSetupFinished masterSetupFinished) {
    // Initialize countdown latch
    this.countDownLatch = new CountDownLatch(1);
    // Launch servers for RPC
    this.driverToMasterServer = AvroUtils.createAvroServer(DriverToMasterMessage.class, driverToMasterMessage,
        new InetSocketAddress(driverToMasterPort));
    this.clientToMasterServer = AvroUtils.createAvroServer(ClientToMasterMessage.class, clientToMasterMessage,
        new InetSocketAddress(clientToMasterPort));
    this.taskToMasterServer = AvroUtils.createAvroServer(TaskToMasterMessage.class, taskToMasterMessage,
        new InetSocketAddress(taskToMasterPort));
    // Start task allocation.
    final Thread thread = new Thread(new TaskSetupRunnable(
        taskRequestor,
        taskStatsMap,
        proxyToTaskMap,
        initialTaskNum,
        masterToTaskPort,
        masterSetupFinished));
    thread.start();
  }

  @Override
  public byte[] call(final byte[] memento) throws Exception {
    LOG.log(Level.INFO, "MistMaster is started");
    this.countDownLatch.await();
    // MistMaster has been terminated
    this.driverToMasterServer.close();
    this.clientToMasterServer.close();
    this.taskToMasterServer.close();
    return null;
  }

  public final class MasterCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOG.log(Level.INFO, "Closing Master");
      countDownLatch.countDown();
    }
  }
}
