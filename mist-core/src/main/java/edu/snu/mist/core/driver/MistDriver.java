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

import edu.snu.mist.core.configs.MistCommonConfigs;
import edu.snu.mist.core.configs.MistMasterConfigs;
import edu.snu.mist.core.configs.MistTaskConfigs;
import edu.snu.mist.core.master.MistMaster;
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.MistTask;
import edu.snu.mist.formats.avro.AllocatedTask;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
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
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.remote.address.LocalAddressProvider;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
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
   * The number of cpu cores for MistMaster.
   */
  private final int masterCpuCores;

  /**
   * The size of memory for MistMaster.
   */
  private final int masterMemSize;

  /**
   * The driver-to-master avro rpc port.
   */
  private final int driverToMasterPort;

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
   * Common configrations for mist master and task.
   */
  private final MistCommonConfigs mistCommonConfigs;

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
   * Indicates whether the master task is running or not.
   */
  private final AtomicBoolean isMasterRunning;

  /**
   * The Avro RPC proxy for master.
   */
  private DriverToMasterMessage proxyToMaster;

  /**
   * The shared store for necessary informations for launching MistTask.
   */
  private MistTaskSubmitInfo mistTaskSubmitInfo;

  /**
   * The avro master-to-driver server.
   */
  private Server masterToDriverServer;

  /**
   * The master hostname.
   */
  private String masterHostname;

  @Inject
  private MistDriver(final EvaluatorRequestor requestor,
                     final JVMProcessFactory jvmProcessFactory,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     @Parameter(NumMasterCores.class) final int masterCpuCores,
                     @Parameter(MasterMemorySize.class) final int masterMemsize,
                     @Parameter(DriverToMasterPort.class) final int driverToMasterPort,
                     final MistCommonConfigs mistCommonConfigs,
                     final MistTaskConfigs mistTaskConfigs,
                     final MistMasterConfigs mistMasterConfigs,
                     final MistTaskSubmitInfo mistTaskSubmitInfo,
                     final MasterToDriverMessage masterToDriverMessage,
                     @Parameter(MasterToDriverPort.class) final int masterToDriverPort) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    this.masterCpuCores = masterCpuCores;
    this.masterMemSize = masterMemsize;
    this.driverToMasterPort = driverToMasterPort;
    this.taskIndex = new AtomicInteger(0);
    this.mistCommonConfigs = mistCommonConfigs;
    this.mistTaskConfigs = mistTaskConfigs;
    this.mistMasterConfigs = mistMasterConfigs;
    this.isMasterRunning = new AtomicBoolean(false);
    this.proxyToMaster = null;
    this.mistTaskSubmitInfo = mistTaskSubmitInfo;
    this.masterToDriverServer = AvroUtils.createAvroServer(MasterToDriverMessage.class,
        masterToDriverMessage, new InetSocketAddress(masterToDriverPort));
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Submit master evaluator request firstly.
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setNumberOfCores(masterCpuCores)
          .setMemory(masterMemSize)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      final EvaluatorDescriptor descriptor = allocatedEvaluator.getEvaluatorDescriptor();
      if (!isMasterRunning.get()) {
        // This is for MistMaster.
        LOG.log(Level.INFO, "A MistMaster allocated to {0}", descriptor.getNodeDescriptor().getName());
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, MIST_MASTER_ID)
            .build());
      } else {
        // This is for MistTask.
        final String taskId = MIST_TASK_ID_PREFIX + taskIndex.getAndIncrement();
        final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
            .addOption("-XX:NewRatio=" + mistTaskSubmitInfo.getNewRatio())
            .addOption("-XX:ReservedCodeCacheSize=" + mistTaskSubmitInfo.getReservedCodeCacheSize() + "m");
        LOG.log(Level.INFO, "A MistTask allocated to {0}", descriptor.getNodeDescriptor().getName());
        allocatedEvaluator.setProcess(jvmProcess);
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, taskId)
            .build());
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
      if (taskId.equals(MIST_MASTER_ID)) {
        // This is for master.
        masterHostname = activeContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress()
            .getHostName();
        final Configuration basicMasterConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, activeContext.getId())
            .set(TaskConfiguration.TASK, MistMaster.class)
            .set(TaskConfiguration.ON_CLOSE, MistMaster.MasterCloseHandler.class)
            .build();
        final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
        jcb.bindNamedParameter(DriverHostname.class, localAddressProvider.getLocalAddress());
        final Configuration driverHostnameConf = jcb.build();
        activeContext.submitTask(
            Configurations.merge(
                basicMasterConf,
                driverHostnameConf,
                mistCommonConfigs.getConfiguration(),
                mistTaskConfigs.getConfiguration(),
                mistMasterConfigs.getConfiguration()));
      } else if (taskId.startsWith(MIST_TASK_ID_PREFIX)) {
        final String taskHostname = activeContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress()
            .getHostName();
        final Configuration basicTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, activeContext.getId())
            .set(TaskConfiguration.TASK, MistTask.class)
            .set(TaskConfiguration.ON_CLOSE, MistTask.TaskCloseHandler.class)
            .build();
        final JavaConfigurationBuilder jcb = tang.newConfigurationBuilder();
        jcb.bindNamedParameter(MasterHostname.class, masterHostname);
        jcb.bindNamedParameter(TaskHostname.class, taskHostname);
        final Configuration hostnameConf = jcb.build();
        activeContext.submitTask(
            Configurations.merge(
                basicTaskConf,
                hostnameConf,
                mistTaskSubmitInfo.getTaskConfiguration()));
      } else {
        LOG.log(Level.SEVERE, "Invalid contextId: {0}", taskId);
        throw new RuntimeException("Internal error: Invalid contextId!");
      }
    }
  }

  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "Evaluator {0} has failed!", failedEvaluator.getId());
      final InetSocketAddress failedNodeInetSocketAddress = failedEvaluator.getFailedContextList().get(0)
          .getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress();
      try {
        proxyToMaster.notifyFailedTask(failedNodeInetSocketAddress.getHostName());
      } catch (final AvroRemoteException e) {
        LOG.log(Level.SEVERE, "Cannot connect to MistMaster for notifying failure! " + e.toString());
        throw new IllegalStateException("Cannot connect to MistMaster while failure recovery");
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task {0} is running", runningTask.getId());
      if (isMasterRunning.compareAndSet(false, true)) {
        // Master is running. Setup avro connection between driver and master.
        try {
          final NettyTransceiver driverToMaster = new NettyTransceiver(new InetSocketAddress(masterHostname,
              driverToMasterPort));
          proxyToMaster = SpecificRequestor.getClient(DriverToMasterMessage.class, driverToMaster);
        } catch (final IOException e) {
          LOG.log(Level.SEVERE, "IOException occurred during setting up driver-to-master avro connection!");
          throw new RuntimeException("driver-to-master avro connection failed");
        }
      } else {
        // The running task is MistTask.
        final String taskHostname = runningTask.getActiveContext().getEvaluatorDescriptor().getNodeDescriptor()
            .getInetSocketAddress().getHostName();
        try {
          // Notify to the master.
          proxyToMaster.notifyTaskAllocated(AllocatedTask.newBuilder()
              .setTaskId(runningTask.getId())
              .setTaskHostname(taskHostname)
              .build());
        } catch (final AvroRemoteException e) {
          LOG.log(Level.SEVERE, "AvroRemoteException occurred during adding a task to master!");
          throw new RuntimeException("Avro addTask failed!");
        }
      }
    }
  }

  public final class DriverStopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      masterToDriverServer.close();
    }
  }
}