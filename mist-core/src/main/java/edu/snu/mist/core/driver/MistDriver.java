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
import edu.snu.mist.core.rpc.DefaultClientToMasterMessageImpl;
import edu.snu.mist.core.rpc.DefaultDriverToMasterMessageImpl;
import edu.snu.mist.core.rpc.DefaultMasterToTaskMessageImpl;
import edu.snu.mist.core.rpc.DefaultTaskToMasterMessageImpl;
import edu.snu.mist.core.task.MistTask;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
   * The ID prefix of ActiveContexts for recovery task.
   */
  private static final String MIST_RECOVERY_TASK_ID_PREFIX = "MIST_RECOVERY_";

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
   * Configuration for mist master.
   */
  private final MistMasterConfigs mistMasterConfigs;

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
   * The number of running MIST tasks.
   */
  private final AtomicInteger runningTaskNum;

  /**
   * The index number for recovery tasks.
   */
  private final AtomicInteger recoveryTaskIndex;

  /**
   * The number of recovery task requests which should be recovered.
   */
  private final AtomicInteger currentRecoveryRequestNum;

  /**
   * The number of currently failed tasks.
   */
  private final AtomicInteger currentFailedTaskNum;

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

  /**
   * The lock for critical section of failure recovery context.
   */
  private final Lock failureRecoveryContextLock;

  @Inject
  private MistDriver(final EvaluatorRequestor requestor,
                     final JVMProcessFactory jvmProcessFactory,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final MistDriverConfigs mistDriverConfigs,
                     final MistTaskConfigs mistTaskConfigs,
                     final MistMasterConfigs mistMasterConfigs) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    this.taskIndex = new AtomicInteger(0);
    this.runningTaskNum = new AtomicInteger(0);
    this.recoveryTaskIndex = new AtomicInteger(0);
    this.currentRecoveryRequestNum = new AtomicInteger(0);
    this.currentFailedTaskNum = new AtomicInteger(0);
    this.mistDriverConfigs = mistDriverConfigs;
    this.mistTaskConfigs = mistTaskConfigs;
    this.mistMasterConfigs = mistMasterConfigs;
    this.isMasterEvaluatorAllocated = new AtomicBoolean(false);
    this.isMasterRunning = new AtomicBoolean(false);
    this.activeContextCounter = new AtomicInteger(0);
    this.masterContext = null;
    this.mistTaskConfQueue = new ConcurrentLinkedQueue<>();
    this.mistTaskContextQueue = new ConcurrentLinkedQueue<>();
    this.proxyToMaster = null;
    this.failureRecoveryContextLock = new ReentrantLock();
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Submit master evaluator request firstly.
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
      // Use TTS (Test-Test & Set) method to avoid lock contention.
      if (currentRecoveryRequestNum.get() > 0) {
        // If there is a failed task, then launch a recovery task.
        failureRecoveryContextLock.lock();
        if (currentRecoveryRequestNum.get() > 0) {
          LOG.log(Level.INFO, "A recovery task allocated to {0}", descriptor.getNodeDescriptor().getName());
          final String recoveryTaskId = MIST_RECOVERY_TASK_ID_PREFIX + recoveryTaskIndex.getAndIncrement();
          allocatedEvaluator.submitContext(ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, recoveryTaskId)
              .build());
          currentRecoveryRequestNum.getAndDecrement();
        }
        failureRecoveryContextLock.unlock();
      } else {
        if (isMasterEvaluatorAllocated.compareAndSet(false, true)) {
          LOG.log(Level.INFO, "A MistMaster allocated to {0}", descriptor.getNodeDescriptor().getName());
          allocatedEvaluator.submitContext(ContextConfiguration.CONF
              .set(ContextConfiguration.IDENTIFIER, MIST_MASTER_ID)
              .build());
          // Submit task evaluator requests.
          requestor.submit(EvaluatorRequest.newBuilder()
              .setNumber(mistDriverConfigs.getNumTasks())
              .setMemory(mistDriverConfigs.getTaskMemSize())
              .setNumberOfCores(mistDriverConfigs.getNumTaskCores())
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
          LOG.log(Level.SEVERE, "Invalid runtime configuration!");
        }
      }
    }
  }


  private Configuration getTaskConfiguration(
      final ActiveContext taskContext,
      final Configuration nameResolverConf,
      final String masterHostname,
      final String taskHostname) {
    // Task configuration
    final Configuration taskConfiguration = TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, taskContext.getId())
        .set(TaskConfiguration.TASK, MistTask.class)
        .set(TaskConfiguration.ON_CLOSE, MistTask.TaskCloseHandler.class)
        .build();
    final JavaConfigurationBuilder taskConfBuilder = tang.newConfigurationBuilder();
    taskConfBuilder.bindNamedParameter(ClientToTaskPort.class, String.valueOf(mistDriverConfigs.getClientToTaskPort()));
    taskConfBuilder.bindNamedParameter(MasterToTaskPort.class, String.valueOf(mistDriverConfigs.getMasterToTaskPort()));
    taskConfBuilder.bindNamedParameter(MasterHostname.class, masterHostname);
    taskConfBuilder.bindNamedParameter(TaskHostname.class, taskHostname);
    taskConfBuilder.bindNamedParameter(TaskToMasterPort.class, String.valueOf(mistDriverConfigs.getTaskToMasterPort()));
    taskConfBuilder.bindImplementation(MasterToTaskMessage.class, DefaultMasterToTaskMessageImpl.class);
    taskConfBuilder.bindNamedParameter(SharedStorePath.class, String.valueOf(mistDriverConfigs
        .getSharedStorePath()));

    // Store task configuration.
    return Configurations.merge(
        nameResolverConf,
        taskConfiguration,
        mistTaskConfigs.getConfiguration(),
        taskConfBuilder.build());
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
      if (taskId.equals(MIST_MASTER_ID)) {
        masterContext = activeContext;
      } else if (taskId.startsWith(MIST_TASK_ID_PREFIX)) {
        mistTaskContextQueue.add(activeContext);
      } else if (taskId.startsWith(MIST_RECOVERY_TASK_ID_PREFIX)) {
        // In this case, recovery task should be submitted.
        // We don't support failure recovery occurred before master running.
        if (!isMasterRunning.get()) {
          throw new IllegalStateException("Node failure occurred before the master started!");
        }
        final String masterHostname =
            masterContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
        final String taskHostname =
            activeContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
        // Launch recovery task.
        activeContext.submitTask(getTaskConfiguration(activeContext, nameResolverConf, masterHostname, taskHostname));
      } else {
        LOG.log(Level.SEVERE, "Invalid contextId: {0}", taskId);
        throw new RuntimeException("Internal error: Invalid contextId!");
      }
      // All the active contexts are now submitted
      if (activeContextCounter.incrementAndGet() == 1 + mistDriverConfigs.getNumTasks()) {
        // Get Master host address
        final String masterHostname =
            masterContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
        LOG.info("Set master host address: " + masterHostname);

        final JavaConfigurationBuilder masterConfBuilder = tang.newConfigurationBuilder();
        for (final ActiveContext taskContext: mistTaskContextQueue) {
          final String taskHostname =
              taskContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
          mistTaskConfQueue.add(getTaskConfiguration(taskContext, nameResolverConf, masterHostname, taskHostname));
        }
        // Master configuration
        masterConfBuilder.bindNamedParameter(SharedStorePath.class, String.valueOf(mistDriverConfigs
            .getSharedStorePath()));
        masterConfBuilder.bindNamedParameter(MasterToTaskPort.class,
            String.valueOf(mistDriverConfigs.getMasterToTaskPort()));
        masterConfBuilder.bindNamedParameter(ClientToMasterPort.class,
            String.valueOf(mistDriverConfigs.getClientToMasterPort()));
        masterConfBuilder.bindNamedParameter(TaskToMasterPort.class,
            String.valueOf(mistDriverConfigs.getTaskToMasterPort()));
        masterConfBuilder.bindNamedParameter(DriverToMasterPort.class,
            String.valueOf(mistDriverConfigs.getDriverToMasterPort()));
        masterConfBuilder.bindNamedParameter(ClientToTaskPort.class,
            String.valueOf(mistDriverConfigs.getClientToTaskPort()));
        masterConfBuilder.bindImplementation(DriverToMasterMessage.class, DefaultDriverToMasterMessageImpl.class);
        masterConfBuilder.bindImplementation(ClientToMasterMessage.class, DefaultClientToMasterMessageImpl.class);
        masterConfBuilder.bindImplementation(TaskToMasterMessage.class, DefaultTaskToMasterMessageImpl.class);
        final Configuration masterConfiguration = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, masterContext.getId())
            .set(TaskConfiguration.TASK, MistMaster.class)
            .set(TaskConfiguration.ON_CLOSE, MistMaster.MasterCloseHandler.class)
            .build();
        // submit master
        masterContext.submitTask(
            Configurations.merge(nameResolverConf,
                masterConfiguration,
                mistMasterConfigs.getConfiguration(),
                masterConfBuilder.build()));
      }
    }
  }

  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "Evaluator {0} has failed!", failedEvaluator.getId());
      final InetSocketAddress failedNodeInetSocketAddress =
          failedEvaluator.getFailedContextList().get(0)
              .getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress();
      try {
        proxyToMaster.notifyFailedTask(failedNodeInetSocketAddress.getHostName());
      } catch (final AvroRemoteException e) {
        LOG.log(Level.SEVERE, "Cannot connect to MistMaster for notifying failure! " + e.toString());
        throw new IllegalStateException("Cannot connect to MistMaster while failure recovery");
      }
      currentRecoveryRequestNum.getAndIncrement();
      currentFailedTaskNum.getAndIncrement();
      // Request the MistTask evaluator for recovery.
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(mistDriverConfigs.getTaskMemSize())
          .setNumberOfCores(mistDriverConfigs.getNumTaskCores())
          .build());
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task {0} is running", runningTask.getId());
      if (runningTask.getId().startsWith(MIST_RECOVERY_TASK_ID_PREFIX)) {
        final String taskHostAddress = runningTask.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
        try {
          proxyToMaster.addTask(taskHostAddress);
          proxyToMaster.setupMasterToTaskConn(taskHostAddress);
          // All the failed tasks are recovered. Start recovery from master.
          if (currentFailedTaskNum.decrementAndGet() == 0) {
            proxyToMaster.startRecovery();
          }
        } catch (final AvroRemoteException e) {
          LOG.log(Level.SEVERE, "AvroRemoteException occurred during recovery process! " + e.toString());
          throw new RuntimeException("Task recovery failed during setting up avro connection!");
        }
      } else {
        if (isMasterRunning.compareAndSet(false, true)) {
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
          // The master is running. Time to submit MistTasks.
          int taskCount = 0;
          // Submit Task tasks after making sure that master is running.
          while (taskCount < mistDriverConfigs.getNumTasks()) {
            final ActiveContext taskContext = mistTaskContextQueue.remove();
            final Configuration taskConf = mistTaskConfQueue.remove();
            taskContext.submitTask(taskConf);
            taskCount += 1;
          }
        } else {
          // The running task is MistTask.
          final String taskHostAddress = runningTask.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor()
              .getInetSocketAddress().getHostName();
          try {
            proxyToMaster.addTask(taskHostAddress);
            proxyToMaster.setupMasterToTaskConn(taskHostAddress);
            if (runningTaskNum.incrementAndGet() == mistDriverConfigs.getNumTasks()) {
              // Notify that all the tasks are running now... Start gathering task information from master.
              proxyToMaster.taskSetupFinished();
            }
          } catch (final AvroRemoteException e) {
            LOG.log(Level.SEVERE, "AvroRemoteException occurred during adding a task to master!");
            throw new RuntimeException("Avro addTask failed!");
          }
        }
      }
    }
  }
}
