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
package edu.snu.mist.driver;

import edu.snu.mist.driver.parameters.NumTaskCores;
import edu.snu.mist.driver.parameters.NumTasks;
import edu.snu.mist.driver.parameters.TaskMemorySize;
import edu.snu.mist.task.MistTask;
import edu.snu.mist.task.parameter.NumExecutors;
import org.apache.avro.ipc.Server;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
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

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MistDriver communicates with 1) MistClients and 2) MistTasks.
 * For 1), avro RPC is used.
 * For 2), NCS is used.
 *
 * 1) MistDriver returns a list of MistTasks' ip addresses to MistClients,
 * when they send messages to MistDriver.
 * With the list of ip addresses, MistClients can connect to the mist tasks directly,
 * in order to send their queries to the tasks.
 *
 * 2) MistDriver communicates with MistTasks in order to collect information about MistTasks' loads.
 * With the information, MistDriver can decide some tasks to run the clients' queries.
 * This logic is performed by TaskSelector.
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
   * The number of MistTasks.
   */
  private final int numTasks;

  /**
   * An evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * The memory size of a MistTask.
   */
  private final int taskMemSize;

  /**
   * The number of cores of a MistTask.
   */
  private final int numTaskCores;

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
   * A task selector which selects MistTasks for executing queries.
   */
  private final TaskSelector taskSelector;

  @Inject
  private MistDriver(final EvaluatorRequestor requestor,
                     final NameServer nameServer,
                     final LocalAddressProvider localAddressProvider,
                     final TaskSelector taskSelector,
                     final Server server,
                     @Parameter(NumTasks.class) final int numTasks,
                     @Parameter(TaskMemorySize.class) final int taskMemSize,
                     @Parameter(NumTaskCores.class) final int numTaskCores) {
    this.nameServer = nameServer;
    this.localAddressProvider = localAddressProvider;
    this.numTasks = numTasks;
    this.requestor = requestor;
    this.taskMemSize = taskMemSize;
    this.numTaskCores = numTaskCores;
    this.taskIndex = new AtomicInteger(0);
    this.taskSelector = taskSelector;
  }

  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(numTasks)
          .setMemory(taskMemSize)
          .setNumberOfCores(numTaskCores)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Context to AllocatedEvaluator: {0}", allocatedEvaluator);
      final String taskId = "MistTask-" + taskIndex.getAndIncrement();
      allocatedEvaluator.submitContext(ContextConfiguration.CONF
      .set(ContextConfiguration.IDENTIFIER, taskId)
      .build());
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
      // Task configuration
      final Configuration taskConfiguration = TaskConfiguration.CONF
          .set(TaskConfiguration.IDENTIFIER, taskId)
          .set(TaskConfiguration.TASK, MistTask.class)
          .build();
      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      jcb.bindNamedParameter(NumExecutors.class, numTaskCores+"");
      // submit a task
      activeContext.submitTask(
          Configurations.merge(nameResolverConf, taskConfiguration, jcb.build()));
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask runningTask) {
      LOG.log(Level.INFO, "Task {0} is running", runningTask.getId());
      // Add the running task to TaskSelector
      taskSelector.addRunningTask(runningTask);
    }
  }
}
