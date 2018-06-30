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

import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.TaskHostname;
import edu.snu.mist.core.parameters.TaskId;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.ipc.Server;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.ports.TcpPortProvider;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
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
   * The avro server for client-to-task communication.
   */
  private final Server clientToTaskServer;

  /**
   * Default constructor of MistTask.
   * @throws InjectionException
   */
  @Inject
  private MistTask(final QueryManager queryManager,
                   final CheckpointManager checkpointManager,
                   @Parameter(TaskId.class) final String taskId,
                   @Parameter(TaskHostname.class) final String taskHostname,
                   @Parameter(MasterHostname.class) final String masterHostname,
                   @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
                   final MasterToTaskMessage masterToTaskMessage,
                   final ClientToTaskMessage clientToTaskMessage,
                   final TcpPortProvider tcpPortProvider) throws InjectionException, IOException, InterruptedException {
    this.countDownLatch = new CountDownLatch(1);
    this.queryManager = queryManager;

    // Find the empty ports for starting servers.
    final Iterator<Integer> portNumIterator = tcpPortProvider.iterator();
    final int clientToTaskPort = portNumIterator.next();
    this.clientToTaskServer = AvroUtils.createAvroServer(ClientToTaskMessage.class, clientToTaskMessage,
        new InetSocketAddress(clientToTaskPort));
    LOG.log(Level.INFO, "Client to task port = " + clientToTaskPort);
    final int masterToTaskPort = portNumIterator.next();
    this.masterToTaskServer = AvroUtils.createAvroServer(MasterToTaskMessage.class, masterToTaskMessage,
        new InetSocketAddress(masterToTaskPort));
    // Send the task information to the master
    LOG.log(Level.INFO, "Master to task port = " + masterToTaskPort);
    final TaskToMasterMessage proxyToMaster = AvroUtils.createAvroProxy(TaskToMasterMessage.class,
        new InetSocketAddress(masterHostname, taskToMasterPort));
    final TaskInfo taskInfo = TaskInfo.newBuilder()
        .setTaskId(taskId)
        .setTaskHostname(taskHostname)
        .setClientToTaskPort(clientToTaskPort)
        .setMasterToTaskPort(masterToTaskPort)
        .build();
    // Send the task info asynchronously.
    final Thread t = new Thread(new RegisterTaskInfoRunner(proxyToMaster, taskInfo));
    t.start();
    t.join();
    this.checkpointManager = checkpointManager;
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    masterToTaskServer.close();
    clientToTaskServer.close();
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
