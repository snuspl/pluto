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

import edu.snu.mist.common.shared.MQTTNoSharedResource;
import edu.snu.mist.common.shared.MQTTResource;
import edu.snu.mist.core.driver.parameters.*;
import edu.snu.mist.core.parameters.Pinning;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;
import edu.snu.mist.core.task.codeshare.NoSharingURLClassLoaderProvider;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.groupaware.eventprocessor.AffinityEventProcessorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.NextGroupSelectorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.dispatch.DispatcherGroupSelectorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.GroupAssignerType;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.Rebalancing;
import edu.snu.mist.core.task.groupaware.groupassigner.GroupAssigner;
import edu.snu.mist.core.task.groupaware.groupassigner.MinLoadGroupAssignerImpl;
import edu.snu.mist.core.task.groupaware.groupassigner.RoundRobinGroupAssignerImpl;
import edu.snu.mist.core.task.groupaware.parameters.GroupSchedModelType;
import edu.snu.mist.core.task.ptq.PTQGroupAllocationTableModifier;
import edu.snu.mist.core.task.ptq.PTQQueryManagerImpl;
import edu.snu.mist.core.task.threadbased.ThreadBasedQueryManagerImpl;
import edu.snu.mist.core.task.threadpool.ThreadPoolQueryManagerImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configuration for mist evaluation.
 */
public final class MistEvaluationConfigs {

  /**
   * Turn on/off group-aware model.
   * If we turn off, queries are not grouped.
   */
  private final boolean groupAware;

  /**
   * Group assigner.
   */
  private final String groupAssignerType;

  /**
   * Turn on/off rebalancing.
   */
  private final boolean rebalancing;

  /**
   * Pin event processors to cores.
   */
  private final boolean pinning;

  /**
   * Turn on/off code sharing.
   */
  private final boolean jarSharing;

  /**
   * Turn on/off network sharing.
   */
  private final boolean networkSharing;

  /**
   * Turn on/off merging.
   */
  private final boolean mergingEnabled;

  /**
   * Execution model option.
   */
  private final String executionModelOption;

  @Inject
  private MistEvaluationConfigs(
      @Parameter(Pinning.class) final boolean pinning,
      @Parameter(MergingEnabled.class) final boolean mergingEnabled,
      @Parameter(ExecutionModelOption.class) final String executionModelOption,
      @Parameter(GroupAware.class) final boolean groupAware,
      @Parameter(GroupAssignerType.class) final String groupAssignerType,
      @Parameter(Rebalancing.class) final boolean rebalancing,
      @Parameter(JarSharing.class) final boolean jarSharing,
      @Parameter(NetworkSharing.class) final boolean networkSharing) {
    this.pinning = pinning;
    this.groupAware = groupAware;
    this.groupAssignerType = groupAssignerType;
    this.rebalancing = rebalancing;
    this.jarSharing = jarSharing;
    this.networkSharing = networkSharing;
    this.mergingEnabled = mergingEnabled;
    this.executionModelOption = executionModelOption;
  }

  private Configuration getExecutionModelConf() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    switch (executionModelOption) {
      case "tpq":
        jcb.bindImplementation(QueryManager.class, ThreadBasedQueryManagerImpl.class);
        break;
      case "tp":
        jcb.bindImplementation(QueryManager.class, ThreadPoolQueryManagerImpl.class);
        break;
      case "ptq":
        jcb.bindImplementation(QueryManager.class, PTQQueryManagerImpl.class);
        jcb.bindImplementation(GroupAllocationTableModifier.class, PTQGroupAllocationTableModifier.class);
        break;
      case "mist":
        jcb.bindImplementation(QueryManager.class, GroupAwareQueryManagerImpl.class);
        break;
      default:
        throw new RuntimeException("Undefined execution model: " + executionModelOption);
    }

    return jcb.build();
  }

  private Class<? extends GroupAssigner> getGroupAssigner() {
    switch (groupAssignerType) {
      case "min": {
        return MinLoadGroupAssignerImpl.class;
      }
      case "rr": {
        return RoundRobinGroupAssignerImpl.class;
      }
      default: {
        throw new RuntimeException("Undefined group assigner: " + groupAssignerType);
      }
    }
  }

  /**
   * Get the configuration for execution model 2 that creates a shared thread pool
   * where each thread executes multiple groups.
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(EventProcessorManager.class, DefaultEventProcessorManager.class);
    jcb.bindImplementation(GroupAssigner.class, getGroupAssigner());
    jcb.bindNamedParameter(MergingEnabled.class, Boolean.toString(mergingEnabled));
    jcb.bindNamedParameter(Rebalancing.class, Boolean.toString(rebalancing));

    if (!this.jarSharing) {
      jcb.bindImplementation(ClassLoaderProvider.class, NoSharingURLClassLoaderProvider.class);
    }

    if (!this.networkSharing) {
      jcb.bindImplementation(MQTTResource.class, MQTTNoSharedResource.class);
    }

    if (pinning) {
      jcb.bindImplementation(EventProcessorFactory.class, AffinityEventProcessorFactory.class);
    } else {
      jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
    }


    jcb.bindImplementation(NextGroupSelectorFactory.class, DispatcherGroupSelectorFactory.class);

    jcb.bindNamedParameter(GroupAware.class, Boolean.toString(groupAware));

    return Configurations.merge(getExecutionModelConf(), jcb.build());
  }

  /**
   * Get command line arguments for evaluation parameters.
   * @param commandLine command line
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(GroupSchedModelType.class)
        .registerShortNameOfClass(ExecutionModelOption.class)
        .registerShortNameOfClass(GroupAware.class)
        .registerShortNameOfClass(Rebalancing.class)
        .registerShortNameOfClass(GroupAssignerType.class)
        .registerShortNameOfClass(MergingEnabled.class)
        .registerShortNameOfClass(JarSharing.class)
        .registerShortNameOfClass(NetworkSharing.class)
        .registerShortNameOfClass(Pinning.class);
  }
}