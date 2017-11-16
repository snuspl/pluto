/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.mist.core.parameters.ClientToTaskServerPortNum;
import edu.snu.mist.core.parameters.MasterToTaskServerPortNum;
import edu.snu.mist.core.task.MistTask;
import edu.snu.mist.core.master.MistMaster;
import edu.snu.mist.core.master.TaskSelector;
import edu.snu.mist.core.master.parameters.ClientToMasterServerPortNum;
import edu.snu.mist.core.master.parameters.ClientToTaskServerAddressSet;
import edu.snu.mist.core.master.parameters.TaskToMasterServerPortNum;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
  public static final String MIST_CONN_FACTORY_ID = "MIST";

  /**
   * Mist driver end point id of NCS.
   */
  public static final String MIST_DRIVER_ID = "MIST_DRIVER";

  /**
   * An evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * An jvm process factory.
   */
  private final JVMProcessFactory jvmProcessFactory;

  /**
   * Index of all running tasks and masters.
   */
  private final AtomicInteger runningEvaluators;

  /**
   * Index of Mist masters.
   */
  private final AtomicInteger masterIndex;

  /**
   * Index of Mist tasks.
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
   * A task selector which selects MistTasks for executing queries.
   */
  private final TaskSelector taskSelector;

  /**
   * Configurations necessary for the driver.
   */
  private final MistDriverConfigs mistDriverConfigs;

  /**
   * Configurations for mist task.
   */
  private final MistTaskConfigs mistTaskConfigs;

  /**
   * The runtime name for mist tasks.
   */
  private static final String MIST_TASK_RUNTIME_NAME = "mist-task";

  /**
   * The runtime name for mist masters.
   */
  private static final String MIST_MASTER_RUNTIME_NAME = "mist-master";

  /**
   * The submitter submits evaluators for Mist tasks after all of the Mist masters launched.
   */

  /**
   * This is a queue that holds the tasks that were submitted as contexts.
   */
  private final Queue<ActiveContext> mistTaskQueue;

  /**
   * This is a queue that holds the masters that were submitted as contexts.
   */
  private final Queue<ActiveContext> mistMasterQueue;

  /**
   * This counts the number of contexts submitted.
   */
  private final AtomicInteger activeContextCounter;

  private final Tang tang = Tang.Factory.getTang();

  @Inject
  private MistDriver(final EvaluatorRequestor requestor,
                     final JVMProcessFactory jvmProcessFactory,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final TaskSelector taskSelector,
                     final MistDriverConfigs mistDriverConfigs,
                     final MistTaskConfigs mistTaskConfigs) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.requestor = requestor;
    this.jvmProcessFactory = jvmProcessFactory;
    this.runningEvaluators = new AtomicInteger(0);
    this.taskIndex = new AtomicInteger(0);
    this.masterIndex = new AtomicInteger(0);
    this.taskSelector = taskSelector;
    this.mistDriverConfigs = mistDriverConfigs;
    this.mistTaskConfigs = mistTaskConfigs;
    this.mistTaskQueue = new ConcurrentLinkedQueue<>();
    this.mistMasterQueue = new ConcurrentLinkedQueue<>();
    this.activeContextCounter = new AtomicInteger(0);
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      // Request master resources
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(mistDriverConfigs.getNumMasters())
          .setMemory(mistDriverConfigs.getMasterMemSize())
          .setNumberOfCores(mistDriverConfigs.getNumMasterCores())
          .setRuntimeName(MIST_MASTER_RUNTIME_NAME)
          .build());
      LOG.log(Level.INFO, "Requested {0} evaluators with {1} cores and {2}m memory for MIST masters.",
          new Object[] {mistDriverConfigs.getNumMasters(), mistDriverConfigs.getMasterMemSize(), mistDriverConfigs
              .getNumMasterCores()});

      // Request task resources
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(mistDriverConfigs.getNumTasks())
          .setMemory(mistDriverConfigs.getTaskMemSize())
          .setNumberOfCores(mistDriverConfigs.getNumTaskCores())
          .setRuntimeName(MIST_TASK_RUNTIME_NAME)
          .build());
      LOG.log(Level.INFO,
          "Requested {0} evaluators with {1} cores and {2}m memory for MIST tasks.",
          new Object[] {mistDriverConfigs.getNumTasks(),
              mistDriverConfigs.getTaskMemSize(),
              mistDriverConfigs.getNumTaskCores()});
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Context to AllocatedEvaluator: {0}", allocatedEvaluator);
      if (allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName().equals(MIST_MASTER_RUNTIME_NAME)) {
        // Submit master tasks
        final String masterId = "MistMaster-" + masterIndex.getAndIncrement();
        allocatedEvaluator.submitContext(ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, masterId)
        .build());
      } else if (allocatedEvaluator.getEvaluatorDescriptor().getRuntimeName().equals(MIST_TASK_RUNTIME_NAME)) {
        // Store allocated task evaluators and launch them after
        final String taskId = "MistTask-" + taskIndex.getAndIncrement();
        final JVMProcess jvmProcess = jvmProcessFactory.newEvaluatorProcess()
            .setMemory(mistDriverConfigs.getTaskMemSize())
            .addOption("-XX:NewRatio=" + mistDriverConfigs.getNewRatio())
            .addOption("-XX:ReservedCodeCacheSize=" + mistDriverConfigs.getReservedCodeCacheSize() + "m");
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
      if (taskId.startsWith("MistMaster")) {
        mistMasterQueue.add(activeContext);
      } else if (taskId.startsWith("MistTask")) {
        mistTaskQueue.add(activeContext);
      } else {
        LOG.log(Level.SEVERE, "Invalid contextId: {0}", taskId);
      }
      // All the active contexts are now submitted
      if (activeContextCounter.incrementAndGet()
          == mistDriverConfigs.getNumMasters() + mistDriverConfigs.getNumTasks()) {
        final Map<String, AtomicInteger> hostPortMap = new HashMap<>();
        final int taskNumPerMaster = (int) Math.ceil(
            (double) mistDriverConfigs.getNumTasks() / (double) mistDriverConfigs.getNumMasters());

        while (!mistMasterQueue.isEmpty()) {
          final ActiveContext masterContext = mistMasterQueue.remove();
          int taskCount = 0;
          final JavaConfigurationBuilder masterConfBuilder = tang.newConfigurationBuilder();
          final String masterHostAddress =
              masterContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
          if (!hostPortMap.containsKey(masterHostAddress)) {
            hostPortMap.put(masterHostAddress,
                new AtomicInteger(mistDriverConfigs.getAvroRpcServerPortStart()));
          }
          final int clientToMasterRpcPort = hostPortMap.get(masterHostAddress).getAndIncrement();
          final int taskToMasterRpcPort = hostPortMap.get(masterHostAddress).getAndIncrement();
          masterConfBuilder.bindNamedParameter(ClientToMasterServerPortNum.class,
              String.valueOf(clientToMasterRpcPort));
          masterConfBuilder.bindNamedParameter(TaskToMasterServerPortNum.class,
              String.valueOf(taskToMasterRpcPort));

          while (!mistTaskQueue.isEmpty() && taskCount < taskNumPerMaster) {
            final ActiveContext taskContext = mistTaskQueue.remove();
            // Allocate the port to the thread
            final String taskHostAddress =
                taskContext.getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress().getHostName();
            if (!hostPortMap.containsKey(taskHostAddress)) {
              hostPortMap.put(taskHostAddress,
                  new AtomicInteger(mistDriverConfigs.getAvroRpcServerPortStart()));
            }
            final int clientToTaskRpcPort = hostPortMap.get(taskHostAddress).getAndIncrement();
            final int masterToTaskRpcPort = hostPortMap.get(taskHostAddress).getAndIncrement();
            // Task configuration
            final Configuration taskConfiguration = TaskConfiguration.CONF
                .set(TaskConfiguration.IDENTIFIER, taskContext.getId())
                .set(TaskConfiguration.TASK, MistTask.class)
                .set(TaskConfiguration.ON_CLOSE, MistTask.TaskCloseHandler.class)
                .build();
            final JavaConfigurationBuilder taskConfBuilder = tang.newConfigurationBuilder();
            taskConfBuilder.bindNamedParameter(ClientToTaskServerPortNum.class, String.valueOf(clientToTaskRpcPort));
            masterConfBuilder.bindSetEntry(ClientToTaskServerAddressSet.class,
                taskHostAddress + ":" + clientToTaskRpcPort);
            taskConfBuilder.bindNamedParameter(MasterToTaskServerPortNum.class, String.valueOf(masterToTaskRpcPort));
            // submit a task
            taskContext.submitTask(
                Configurations.merge(nameResolverConf, taskConfiguration, mistTaskConfigs.getConfiguration(),
                    taskConfBuilder.build()));
            taskCount++;
          }
          // Master configuration
          final Configuration masterConfiguration = TaskConfiguration.CONF
              .set(TaskConfiguration.IDENTIFIER, masterContext.getId())
              .set(TaskConfiguration.TASK, MistMaster.class)
              .set(TaskConfiguration.ON_CLOSE, MistMaster.MasterCloseHandler.class)
              .build();
          // submit a task
          masterContext.submitTask(
              Configurations.merge(nameResolverConf, masterConfiguration, mistTaskConfigs.getConfiguration(),
                  masterConfBuilder.build()));
        }
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task {0} is running", runningTask.getId());
    }
  }
}
