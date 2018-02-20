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

import edu.snu.mist.core.driver.parameters.EventProcessorNumAssignerType;
import edu.snu.mist.core.driver.parameters.GroupAware;
import edu.snu.mist.core.driver.parameters.GroupIsolationEnabled;
import edu.snu.mist.core.parameters.Pinning;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.groupaware.eventprocessor.*;
import edu.snu.mist.core.task.groupaware.groupassigner.GroupAssigner;
import edu.snu.mist.core.task.groupaware.groupassigner.MinLoadGroupAssignerImpl;
import edu.snu.mist.core.task.groupaware.groupassigner.RoundRobinGroupAssignerImpl;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.*;
import edu.snu.mist.core.task.metrics.AIADEventProcessorNumAssigner;
import edu.snu.mist.core.task.metrics.MISDEventProcessorNumAssigner;
import edu.snu.mist.core.task.groupaware.rebalancer.*;
import edu.snu.mist.core.task.groupaware.*;
import edu.snu.mist.core.task.groupaware.eventprocessor.dispatch.DispatcherGroupSelectorFactory;
import edu.snu.mist.core.task.metrics.DefaultEventProcessorNumAssigner;
import edu.snu.mist.core.task.groupaware.parameters.*;
import edu.snu.mist.core.task.metrics.EventProcessorNumAssigner;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configuration of the mist task that performs two level scheduling:
 * first-level: cfs scheduling of groups
 * second-level: round-robin operator chain scheduling with active/inactive operator chain management.
 *
 * An event processor picks a group by using cfs scheduling.
 * After picking a group, the processor processes events within the group,
 * by selecting active operator chain that has events in its queue.
 */
public final class MistGroupSchedulingTaskConfigs {

  private final String epaType;
  private final double cpuUtilLowThreshold;
  private final double eventNumHighThreshold;
  private final double eventNumLowThreshold;
  private final int eventProcessorDecreaseNum;
  private final double eventProcessorIncreaseRate;
  private final int eventProcessorIncreaseNum;
  private final int dispatcherThreadNum;
  private final long rebalancingPeriod;
  private final long isolationTriggerPeriod;
  private final double underloadedGroupThreshold;

  // TODO[REMOVE]
  private final boolean groupAware;
  private final String groupAssignerType;
  private final boolean rebalancing;
  private final boolean groupIsolation;

  private final boolean pinning;
  private final long processingTimeout;
  private final long groupPinningTime;

  @Inject
  private MistGroupSchedulingTaskConfigs(
      @Parameter(EventProcessorNumAssignerType.class) final String epaType,
      @Parameter(CpuUtilLowThreshold.class) final double cpuUtilLowThreshold,
      @Parameter(EventNumHighThreshold.class) final double eventNumHighThreshold,
      @Parameter(EventNumLowThreshold.class) final double eventNumLowThreshold,
      @Parameter(EventProcessorDecreaseNum.class) final int eventProcessorDecreaseNum,
      @Parameter(EventProcessorIncreaseRate.class) final double eventProcessorIncreaseRate,
      @Parameter(EventProcessorIncreaseNum.class) final int eventProcessorIncreaseNum,
      @Parameter(Pinning.class) final boolean pinning,
      @Parameter(DispatcherThreadNum.class) final int dispatcherThreadNum,
      @Parameter(GroupAware.class) final boolean groupAware,
      @Parameter(GroupAssignerType.class) final String groupAssignerType,
      @Parameter(Rebalancing.class) final boolean rebalancing,
      @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod,
      @Parameter(GroupIsolationEnabled.class) final boolean groupIsolation,
      @Parameter(IsolationTriggerPeriod.class) final long isolationTriggerPeriod,
      @Parameter(UnderloadedGroupThreshold.class) final double underloadedGroupThreshold,
      @Parameter(GroupPinningTime.class) final long groupPinningTime,
      @Parameter(ProcessingTimeout.class) final long processingTimeout) {
    this.epaType = epaType;
    this.cpuUtilLowThreshold = cpuUtilLowThreshold;
    this.eventNumHighThreshold = eventNumHighThreshold;
    this.eventNumLowThreshold = eventNumLowThreshold;
    this.eventProcessorDecreaseNum = eventProcessorDecreaseNum;
    this.eventProcessorIncreaseRate = eventProcessorIncreaseRate;
    this.eventProcessorIncreaseNum = eventProcessorIncreaseNum;
    this.pinning = pinning;
    this.groupAware = groupAware;
    this.dispatcherThreadNum = dispatcherThreadNum;
    this.groupAssignerType = groupAssignerType;
    this.rebalancing = rebalancing;
    this.groupPinningTime = groupPinningTime;
    this.rebalancingPeriod = rebalancingPeriod;
    this.groupIsolation = groupIsolation;
    this.isolationTriggerPeriod = isolationTriggerPeriod;
    this.underloadedGroupThreshold = underloadedGroupThreshold;
    this.processingTimeout = processingTimeout;
  }

  /**
   * Get event processor num assigner class.
   */
  private Class<? extends EventProcessorNumAssigner> getEpaClass() {
    switch (epaType) {
      case "none":
        return DefaultEventProcessorNumAssigner.class;
      case "miad":
        return MISDEventProcessorNumAssigner.class;
      case "aiad":
        return AIADEventProcessorNumAssigner.class;
      default:
        throw new RuntimeException("No event processor num assigner type: " + epaType);
    }
  }

  /**
   * Get the configuration for execution model.
   * - nonblocking: round-robin without blocking
   * - polling: round-robin without blocking + activate/deactivate group in polling approach
   */
  private Configuration getConfigurationForExecutionModel() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    if (pinning) {
      jcb.bindImplementation(EventProcessorFactory.class, AffinityEventProcessorFactory.class);
    } else {
      jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
    }

    jcb.bindImplementation(NextGroupSelectorFactory.class, DispatcherGroupSelectorFactory.class);
    return jcb.build();
  }

  /**
   * Get the configuration for execution model 2 that creates a shared thread pool
   * where each thread executes multiple groups.
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(QueryManager.class, GroupAwareQueryManagerImpl.class);
    jcb.bindImplementation(EventProcessorNumAssigner.class, getEpaClass());
    jcb.bindImplementation(EventProcessorManager.class, DefaultEventProcessorManager.class);

    switch (groupAssignerType) {
      case "min": {
        jcb.bindImplementation(GroupAssigner.class, MinLoadGroupAssignerImpl.class);
        break;
      }
      case "rr": {
        jcb.bindImplementation(GroupAssigner.class, RoundRobinGroupAssignerImpl.class);
        break;
      }
      default: {
        throw new RuntimeException("Undefined group assigner: " + groupAssignerType);
      }
    }

    if (rebalancing) {
      jcb.bindImplementation(GroupRebalancer.class, DefaultGroupRebalancerImpl.class);
    } else {
      jcb.bindImplementation(GroupRebalancer.class, NoGroupRebalancerImpl.class);
    }

    if (!groupIsolation) {
      jcb.bindImplementation(GroupIsolator.class, NoGroupIsolator.class);
    } else {
      jcb.bindImplementation(GroupIsolator.class, DefaultGroupIsolatorImpl.class);
    }

    jcb.bindNamedParameter(CpuUtilLowThreshold.class, Double.toString(cpuUtilLowThreshold));
    jcb.bindNamedParameter(EventNumHighThreshold.class, Double.toString(eventNumHighThreshold));
    jcb.bindNamedParameter(EventNumLowThreshold.class, Double.toString(eventNumLowThreshold));
    jcb.bindNamedParameter(EventProcessorDecreaseNum.class, Integer.toString(eventProcessorDecreaseNum));
    jcb.bindNamedParameter(EventProcessorIncreaseRate.class, Double.toString(eventProcessorIncreaseRate));
    jcb.bindNamedParameter(EventProcessorIncreaseNum.class, Integer.toString(eventProcessorIncreaseNum));
    jcb.bindNamedParameter(GroupAware.class, Boolean.toString(groupAware));
    jcb.bindNamedParameter(DispatcherThreadNum.class, Integer.toString(dispatcherThreadNum));
    jcb.bindNamedParameter(GroupRebalancingPeriod.class, Long.toString(rebalancingPeriod));
    jcb.bindNamedParameter(IsolationTriggerPeriod.class, Long.toString(isolationTriggerPeriod));
    jcb.bindNamedParameter(UnderloadedGroupThreshold.class, Double.toString(underloadedGroupThreshold));
    jcb.bindNamedParameter(ProcessingTimeout.class, Long.toString(processingTimeout));
    jcb.bindNamedParameter(GroupPinningTime.class, Long.toString(groupPinningTime));

    return Configurations.merge(getConfigurationForExecutionModel(), jcb.build());
  }

  /**
   * Get command line arguments for option 2.
   * @param commandLine command line
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    return commandLine
        .registerShortNameOfClass(CpuUtilLowThreshold.class)
        .registerShortNameOfClass(EventNumHighThreshold.class)
        .registerShortNameOfClass(EventNumLowThreshold.class)
        .registerShortNameOfClass(EventProcessorDecreaseNum.class)
        .registerShortNameOfClass(EventProcessorIncreaseRate.class)
        .registerShortNameOfClass(EventProcessorNumAssignerType.class)
        .registerShortNameOfClass(EventProcessorIncreaseNum.class)
        .registerShortNameOfClass(GroupSchedModelType.class)
        .registerShortNameOfClass(DispatcherThreadNum.class)
        .registerShortNameOfClass(GroupAware.class)
        .registerShortNameOfClass(Rebalancing.class)
        .registerShortNameOfClass(GroupAssignerType.class)
        .registerShortNameOfClass(GroupRebalancingPeriod.class)
        .registerShortNameOfClass(GroupIsolationEnabled.class)
        .registerShortNameOfClass(IsolationTriggerPeriod.class)
        .registerShortNameOfClass(UnderloadedGroupThreshold.class)
        .registerShortNameOfClass(ProcessingTimeout.class)
        .registerShortNameOfClass(Pinning.class)
        .registerShortNameOfClass(GroupPinningTime.class);
  }
}
