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
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.DriverToMasterPort;
import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.MasterIndex;
import edu.snu.mist.core.parameters.MasterMemorySize;
import edu.snu.mist.core.parameters.MasterRecovery;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.core.parameters.NumMasterCores;
import edu.snu.mist.core.parameters.TaskHostname;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.MistTask;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.CompletedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.evaluator.JVMProcess;
import org.apache.reef.driver.evaluator.JVMProcessFactory;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
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
  private static final String MIST_MASTER_ID_PREFIX = "MIST_MASTER_";

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
   * Index of MistMasters.
   */
  private final AtomicInteger masterIndex;

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
   * Indicates whether the master has failed before or not.
   */
  private final AtomicBoolean isMasterFailed;

  /**
   * The shared running task information.
   */
  private RunningTaskInfoStore runningTaskInfoStore;

  /**
   * The Avro RPC proxy for master.
   */
  private DriverToMasterMessage proxyToMaster;

  /**
   * The shared store for necessary informations for launching MistTask.
   */
  private TaskSubmitInfoStore taskSubmitInfoStore;

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
                     final LocalAddressProvider localAddressProvider,
                     @Parameter(NumMasterCores.class) final int masterCpuCores,
                     @Parameter(MasterMemorySize.class) final int masterMemsize,
                     @Parameter(DriverToMasterPort.class) final int driverToMasterPort,
                     final MistCommonConfigs mistCommonConfigs,
                     final MistTaskConfigs mistTaskConfigs,
                     final MistMasterConfigs mistMasterConfigs,
                     final TaskSubmitInfoStore taskSubmitInfoStore,
                     final MasterToDriverMessage masterToDriverMessage,
                     @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
                     final RunningTaskInfoStore runningTaskInfoStore) throws Exception {
    this.localAddressProvider = localAddressProvider;
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    this.masterCpuCores = masterCpuCores;
    this.masterMemSize = masterMemsize;
    this.driverToMasterPort = driverToMasterPort;
    this.masterIndex = new AtomicInteger(0);
    this.mistCommonConfigs = mistCommonConfigs;
    this.mistTaskConfigs = mistTaskConfigs;
    this.mistMasterConfigs = mistMasterConfigs;
    this.isMasterRunning = new AtomicBoolean(false);
    this.isMasterFailed = new AtomicBoolean(false);
    this.proxyToMaster = null;
    this.taskSubmitInfoStore = taskSubmitInfoStore;
    this.masterToDriverServer = AvroUtils.createAvroServer(MasterToDriverMessage.class,
        masterToDriverMessage, new InetSocketAddress(masterToDriverPort));
    this.runningTaskInfoStore = runningTaskInfoStore;
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
            .set(ContextConfiguration.IDENTIFIER, MIST_MASTER_ID_PREFIX + masterIndex.getAndIncrement())
            .build());
      } else {
        // This is for MistTask.
        final TaskSubmitInfo taskSubmitInfo = taskSubmitInfoStore.getCurrentSubmitInfo();
        final String taskId = taskSubmitInfo.getTaskId();
        final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
            .addOption("-XX:NewRatio=" + taskSubmitInfo.getNewRatio())
            .addOption("-XX:ReservedCodeCacheSize=" + taskSubmitInfo.getReservedCodeCacheSize() + "m");
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
      if (taskId.startsWith(MIST_MASTER_ID_PREFIX)) {
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
        final JavaConfigurationBuilder jcb2 = tang.newConfigurationBuilder();
        jcb2.bindNamedParameter(MasterRecovery.class, String.valueOf(isMasterFailed.get()));
        jcb2.bindNamedParameter(MasterIndex.class, String.valueOf(masterIndex.get() - 1));
        final Configuration additionalMasterConf = jcb2.build();
        activeContext.submitTask(
            Configurations.merge(
                basicMasterConf,
                driverHostnameConf,
                additionalMasterConf,
                mistCommonConfigs.getConfiguration(),
                mistTaskConfigs.getConfiguration(),
                mistMasterConfigs.getConfiguration()));
      } else {
        runningTaskInfoStore.put(taskId);
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
        final TaskSubmitInfo taskSubmitInfo = taskSubmitInfoStore.getCurrentSubmitInfo();
        activeContext.submitTask(
            Configurations.merge(
                basicTaskConf,
                hostnameConf,
                taskSubmitInfo.getTaskConfiguration()));
      }
    }
  }

  public final class FailedEvaluatorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(final FailedEvaluator failedEvaluator) {
      LOG.log(Level.INFO, "Evaluator {0} has failed!", failedEvaluator.getId());
      final InetSocketAddress failedNodeInetSocketAddress = failedEvaluator.getFailedContextList().get(0)
          .getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress();
      if (failedEvaluator.getFailedContextList().get(0).getId().startsWith(MIST_MASTER_ID_PREFIX)) {
        // Master has failed. We need to recovery master.
        isMasterRunning.set(false);
        isMasterFailed.set(true);
        // Submit master evaluator request firstly.
        requestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setNumberOfCores(masterCpuCores)
            .setMemory(masterMemSize)
            .build());
        LOG.log(Level.INFO, "Requested Evaluator.");
      } else {
        // Worker has failed. we need to recover worker.
        try {
          final String failedTaskId = failedEvaluator.getFailedContextList().get(0).getId();
          runningTaskInfoStore.remove(failedTaskId);
          proxyToMaster.notifyFailedTask(failedTaskId);
        } catch (final AvroRemoteException e) {
          e.printStackTrace();
          LOG.log(Level.SEVERE, "Cannot connect to MistMaster for notifying failure! " + e.toString());
          throw new IllegalStateException("Cannot connect to MistMaster while failure recovery");
        }
      }
    }
  }

  public final class CompletedEvaluatorHandler implements EventHandler<CompletedEvaluator> {
    @Override
    public void onNext(final CompletedEvaluator completedEvaluator) {
      // Do nothing.
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
          isMasterFailed.compareAndSet(true, false);
        } catch (final IOException e) {
          LOG.log(Level.SEVERE, "IOException occurred during setting up driver-to-master avro connection!");
          throw new RuntimeException("driver-to-master avro connection failed");
        }
      } else {
        // Remove the current task submit info.
        taskSubmitInfoStore.removeCurrentSubmitInfo();
        // The running task is MistTask.
        final String taskId = runningTask.getId();
        runningTaskInfoStore.updateRunningTask(taskId, runningTask);
        try {
          // Notify to the master.
          proxyToMaster.notifyTaskAllocated(runningTask.getId());
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