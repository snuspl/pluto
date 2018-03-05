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

import edu.snu.mist.core.master.MistMaster;
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.task.MistTask;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.*;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MistDriver communicates with 1) MistClients and 2) MistTasks.
 * For 1), avro RPC is used.
 * For 2), reef NCS is used.
 *
 * 1) MistDriver returns a list of MistTasks' ip addresses to MistClients,
 * when they send messages to MistDriver.
 * With the list of ip addresses, MistClients can connect to the mist tasks directly,
 * in order to send their queries to the tasks.
 *
 * 2) MistDriver communicates with MistTasks in order to collect information about MistTasks' loads.
 * With the information, MistDriver can decide some tasks to run the clients' queries.
 * This logic is performed by TaskSelector.
 *
 * Current MistDriver cannot add/remove Tasks at runtime.
 * TODO[MIST-#]: We need to support this feature to dynamically scale in/out Tasks.
 */
@Unit
public final class MistDriver {
  private static final Logger LOG = Logger.getLogger(MistDriver.class.getName());

  /**
   * Mist connection factory id of NCS.
   */
  private static final String MIST_CONN_FACTORY_ID = "MIST";

  /**
   * Mist driver end point id of NCS.
   */
  private static final String MIST_DRIVER_ID = "MIST_DRIVER";

  /**
   * The ID of ActiveContexts for MistMaster.
   */
  private static final String MIST_MASTER_ID = "MIST_MASTER";

  /**
   * The ID prefix of ActiveContexts for MistTask.
   */
  private static final String MIST_TASK_ID_PREFIX = "MIST_TASK_";

  /**
   * Static Tang object.
   */
  private final Tang tang = Tang.Factory.getTang();

  /**
   * An evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * Index of MistTasks.
   */
  private final AtomicInteger taskIndex;

  /**
   * A name server.
   */
  private final NameServer nameServer;

  /**
   * A local address provider.
   */
  private final LocalAddressProvider localAddressProvider;

  /**
   * Configurations necessary for the driver.
   */
  private final MistDriverConfigs mistDriverConfigs;

  /**
   * Configurations for mist task.
   */
  private final MistTaskConfigs mistTaskConfigs;

  /**
   * A JVM process factory for configuring parameters.
   */
  private final JVMProcessFactory jvmProcessFactory;

  /**
   * Indicates whether the master evaluator is allocated or not.
   */
  private final AtomicBoolean isMasterEvaluatorAllocated;

  /**
   * Indicates whether the master task is running or not.
   */
  private final AtomicBoolean isMasterRunning;

  /**
   * The atomic counter for the number of active contexts.
   */
  private final AtomicInteger activeContextCounter;

  /**
   * The active context of master.
   */
  private ActiveContext masterContext;

  /**
   * The queue for mist task active contexts.
   */
  private final Queue<ActiveContext> mistTaskContextQueue;

  /**
   * The queue for mist task configurations.
   */
  private final Queue<Configuration> mistTaskConfQueue;

  /**
   * The Avro RPC proxy for master.
   */
  private DriverToMasterMessage proxyToMaster;

  @Inject
  private MistDriver(final EvaluatorRequestor requestor,
                     final JVMProcessFactory jvmProcessFactory,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final Server server,
                     final MistDriverConfigs mistDriverConfigs,
                     final MistTaskConfigs mistTaskConfigs) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    this.taskIndex = new AtomicInteger(0);
    this.mistDriverConfigs = mistDriverConfigs;
    this.mistTaskConfigs = mistTaskConfigs;
    this.isMasterEvaluatorAllocated = new AtomicBoolean(false);
    this.isMasterRunning = new AtomicBoolean(false);
    this.activeContextCounter = new AtomicInteger(0);
    this.masterContext = null;
    this.mistTaskConfQueue = new ConcurrentLinkedQueue<>();
    this.mistTaskContextQueue = new ConcurrentLinkedQueue<>();
    this.proxyToMaster = null;
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(mistDriverConfigs.getNumTasks())
          .setMemory(mistDriverConfigs.getTaskMemSize())
          .setNumberOfCores(mistDriverConfigs.getNumTaskCores())
          .build());
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(mistDriverConfigs.getMasterMemSize())
          .setNumberOfCores(mistDriverConfigs.getNumMasterCores())
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final EvaluatorDescriptor descriptor = allocatedEvaluator.getEvaluatorDescriptor();
      if (descriptor.getMemory() == mistDriverConfigs.getMasterMemSize()
          && isMasterEvaluatorAllocated.compareAndSet(false, true)) {
        LOG.log(Level.INFO, "A MistMaster allocated to {0}", descriptor.getNodeDescriptor().getName());
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, MIST_MASTER_ID)
            .build());
      } else if (
          descriptor.getMemory() == mistDriverConfigs.getTaskMemSize()) {
        final String taskId = MIST_TASK_ID_PREFIX + taskIndex.getAndIncrement();
        final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
            .setMemory(mistDriverConfigs.getTaskMemSize())
            .addOption("-XX:NewRatio=" + mistDriverConfigs.getNewRatio())
            .addOption("-XX:ReservedCodeCacheSize=" + mistDriverConfigs.getReservedCodeCacheSize() + "m");
        LOG.log(Level.INFO, "A MistTask allocated to {0}", descriptor.getNodeDescriptor().getName());
        allocatedEvaluator.setProcess(jvmProcess);
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, taskId)
            .build());
      } else {
        LOG.log(Level.SEVERE, "Invalid runtime name!");
      }
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      LOG.log(Level.INFO, "Submitting Task to Context: {0}", activeContext);
      final String taskId = activeContext.getId();
      // Configuration for NCS of mist task.
      final Configuration nameResolverConf = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, nameServer.getPort())
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, localAddressProvider.getLocalAddress())
          .build();
      // Configuration for NCS of mist task.
      if (taskId.equals(MIST_MASTER_ID)) {
        masterContext = activeContext;
      } else if (taskId.startsWith(MIST_TASK_ID_PREFIX)) {
        mistTaskContextQueue.add(activeContext);
      } else {
        LOG.log(Level.SEVERE, "Invalid contextId: {0}", taskId);
        throw new RuntimeException("Internal error: Invalid contextId!");
      }
      // All the active contexts are now submitted
      if (activeContextCounter.incrementAndGet() == 1 + mistDriverConfigs.getNumTasks()) {
        // Get Master host address
        final String masterHostAddress =
            masterContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
        final int clientToMasterPort = mistDriverConfigs.getClientToMasterPort();
        final int taskToMasterPort = mistDriverConfigs.getTaskToMasterPort();
        final int masterToTaskPort = mistDriverConfigs.getMasterToTaskPort();
        final int clientToTaskPort = mistDriverConfigs.getClientToTaskPort();
        final int driverToMasterPort = mistDriverConfigs.getDriverToMasterPort();

        final JavaConfigurationBuilder masterConfBuilder = tang.newConfigurationBuilder();
        for (final ActiveContext taskContext: mistTaskContextQueue) {
          // Task configurations
          final String taskHostAddress =
              taskContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
          // Task configuration
          final Configuration taskConfiguration = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, taskContext.getId())
              .set(TaskConfiguration.TASK, MistTask.class)
              .set(TaskConfiguration.ON_CLOSE, MistTask.TaskCloseHandler.class)
              .build();
          final JavaConfigurationBuilder taskConfBuilder = tang.newConfigurationBuilder();
          taskConfBuilder.bindNamedParameter(ClientToTaskPort.class, String.valueOf(clientToTaskPort));
          masterConfBuilder.bindSetEntry(TaskHostAddressSet.class,
              taskHostAddress);
          taskConfBuilder.bindNamedParameter(MasterToTaskPort.class, String.valueOf(masterToTaskPort));
          taskConfBuilder.bindNamedParameter(MasterHostAddress.class, masterHostAddress);
          LOG.info("Set master host address: " + masterHostAddress);
          taskConfBuilder.bindNamedParameter(TaskToMasterPort.class, String.valueOf(taskToMasterPort));
          // Store task configuration.
          mistTaskConfQueue.add(Configurations.merge(
              nameResolverConf,
              taskConfiguration,
              mistTaskConfigs.getConfiguration(),
              taskConfBuilder.build()));
        }
        // Master configuration
        masterConfBuilder.bindNamedParameter(MasterToTaskPort.class, String.valueOf(masterToTaskPort));
        masterConfBuilder.bindNamedParameter(ClientToMasterPort.class, String.valueOf(clientToMasterPort));
        masterConfBuilder.bindNamedParameter(TaskToMasterPort.class, String.valueOf(taskToMasterPort));
        masterConfBuilder.bindNamedParameter(DriverToMasterPort.class, String.valueOf(driverToMasterPort));
        final Configuration masterConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, masterContext.getId())
            .set(TaskConfiguration.TASK, MistMaster.class)
            .set(TaskConfiguration.ON_CLOSE, MistMaster.MasterCloseHandler.class)
            .build();
        // submit master
        masterContext.submitTask(
            Configurations.merge(nameResolverConf, masterConfiguration, masterConfBuilder.build()));
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task {0} is running", runningTask.getId());
      if (isMasterRunning.compareAndSet(false, true)) {
        // The running task is master. Time to submit MistTasks.
        int taskCount = 0;
        // Submit Task tasks after making sure that master is running.
        while (taskCount < mistDriverConfigs.getNumTasks()) {
          final ActiveContext taskContext = mistTaskContextQueue.remove();
          final Configuration taskConf = mistTaskConfQueue.remove();
          taskContext.submitTask(taskConf);
          taskCount += 1;
        }
        // Establish driver-to-master connection.
        final String masterHostAddress = runningTask.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
        final int driverToMasterPort = mistDriverConfigs.getDriverToMasterPort();
        // Master is running. Setup avro connection between driver and master.
        try {
          final NettyTransceiver driverToMaster = new NettyTransceiver(new InetSocketAddress(masterHostAddress,
              driverToMasterPort));
          proxyToMaster = SpecificRequestor.getClient(DriverToMasterMessage.class, driverToMaster);
        } catch (final IOException e) {
          LOG.log(Level.SEVERE, "IOException occurred during setting up driver-to-master avro connection!");
          throw new RuntimeException("driver-to-master avro connection failed");
        }
      } else {
        // The running task is MistTask.
        final String taskHostAddress = runningTask.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
        try {
          proxyToMaster.addTask(taskHostAddress);
        } catch (final AvroRemoteException e) {
          LOG.log(Level.SEVERE, "AvroRemoteException occurred during adding a task to master!");
          throw new RuntimeException("Avro addTask failed!");
        }
      }
    }
  }
}
