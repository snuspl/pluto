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

import edu.snu.mist.core.driver.parameters.EventProcessorNumAssignerType;
import edu.snu.mist.core.driver.parameters.GroupAware;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.eventProcessors.DefaultEventProcessorManager;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.eventProcessors.groupAssigner.GroupAssigner;
import edu.snu.mist.core.task.eventProcessors.groupAssigner.MinLoadGroupAssignerImpl;
import edu.snu.mist.core.task.eventProcessors.groupAssigner.RoundRobinGroupAssignerImpl;
import edu.snu.mist.core.task.eventProcessors.parameters.DispatcherThreadNum;
import edu.snu.mist.core.task.eventProcessors.parameters.GroupAssignerType;
import edu.snu.mist.core.task.eventProcessors.parameters.GroupRebalancingPeriod;
import edu.snu.mist.core.task.eventProcessors.parameters.Rebalancing;
import edu.snu.mist.core.task.eventProcessors.rebalancer.DefaultGroupRebalancerImpl;
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.eventProcessors.rebalancer.NoGroupRebalancerImpl;
import edu.snu.mist.core.task.globalsched.*;
import edu.snu.mist.core.task.globalsched.cfs.CfsSchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.dispatch.DispatcherGroupSelectorFactory;
import edu.snu.mist.core.task.globalsched.metrics.DefaultEventProcessorNumAssigner;
import edu.snu.mist.core.task.globalsched.parameters.*;
import edu.snu.mist.core.task.globalsched.roundrobin.polling.InactiveGroupCheckerFactory;
import edu.snu.mist.core.task.globalsched.roundrobin.polling.NaiveInactiveGroupCheckerFactory;
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
  private final long cfsSchedulingPeriod;
  private final long minSchedulingPeriod;
  private final int eventProcessorIncreaseNum;
  private final String groupSchedModelType;
  private final int dispatcherThreadNum;
  private final long rebalancingPeriod;

  // TODO[REMOVE]
  private final boolean groupAware;
  private final String groupAssignerType;
  private final boolean rebalancing;

  @Inject
  private MistGroupSchedulingTaskConfigs(
      @Parameter(EventProcessorNumAssignerType.class) final String epaType,
      @Parameter(CpuUtilLowThreshold.class) final double cpuUtilLowThreshold,
      @Parameter(EventNumHighThreshold.class) final double eventNumHighThreshold,
      @Parameter(EventNumLowThreshold.class) final double eventNumLowThreshold,
      @Parameter(EventProcessorDecreaseNum.class) final int eventProcessorDecreaseNum,
      @Parameter(EventProcessorIncreaseRate.class) final double eventProcessorIncreaseRate,
      @Parameter(CfsSchedulingPeriod.class) final long cfsSchedulingPeriod,
      @Parameter(MinSchedulingPeriod.class) final long minSchedulingPeriod,
      @Parameter(EventProcessorIncreaseNum.class) final int eventProcessorIncreaseNum,
      @Parameter(GroupSchedModelType.class) final String groupSchedModelType,
      @Parameter(DispatcherThreadNum.class) final int dispatcherThreadNum,
      @Parameter(GroupAware.class) final boolean groupAware,
      @Parameter(GroupAssignerType.class) final String groupAssignerType,
      @Parameter(Rebalancing.class) final boolean rebalancing,
      @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod) {
    this.epaType = epaType;
    this.cpuUtilLowThreshold = cpuUtilLowThreshold;
    this.eventNumHighThreshold = eventNumHighThreshold;
    this.eventNumLowThreshold = eventNumLowThreshold;
    this.eventProcessorDecreaseNum = eventProcessorDecreaseNum;
    this.eventProcessorIncreaseRate = eventProcessorIncreaseRate;
    this.cfsSchedulingPeriod = cfsSchedulingPeriod;
    this.minSchedulingPeriod = minSchedulingPeriod;
    this.eventProcessorIncreaseNum = eventProcessorIncreaseNum;
    this.groupSchedModelType = groupSchedModelType;
    this.groupAware = groupAware;
    this.dispatcherThreadNum = dispatcherThreadNum;
    this.groupAssignerType = groupAssignerType;
    this.rebalancing = rebalancing;
    this.rebalancingPeriod = rebalancingPeriod;
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
    switch (groupSchedModelType) {
      case "dispatching":
        jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
        jcb.bindImplementation(NextGroupSelectorFactory.class, DispatcherGroupSelectorFactory.class);
        jcb.bindImplementation(InactiveGroupCheckerFactory.class, NaiveInactiveGroupCheckerFactory.class);
        break;
      default:
        throw new RuntimeException("Invalid group scheduling model: " + groupSchedModelType);
    }
    return jcb.build();
  }

  /**
   * Get the configuration for execution model 2 that creates a shared thread pool
   * where each thread executes multiple groups.
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    jcb.bindImplementation(EventProcessorNumAssigner.class, getEpaClass());
    jcb.bindImplementation(EventProcessorManager.class, DefaultEventProcessorManager.class);
    jcb.bindImplementation(SchedulingPeriodCalculator.class, CfsSchedulingPeriodCalculator.class);

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

    jcb.bindNamedParameter(CpuUtilLowThreshold.class, Double.toString(cpuUtilLowThreshold));
    jcb.bindNamedParameter(EventNumHighThreshold.class, Double.toString(eventNumHighThreshold));
    jcb.bindNamedParameter(EventNumLowThreshold.class, Double.toString(eventNumLowThreshold));
    jcb.bindNamedParameter(EventProcessorDecreaseNum.class, Integer.toString(eventProcessorDecreaseNum));
    jcb.bindNamedParameter(EventProcessorIncreaseRate.class, Double.toString(eventProcessorIncreaseRate));
    jcb.bindNamedParameter(CfsSchedulingPeriod.class, Long.toString(cfsSchedulingPeriod));
    jcb.bindNamedParameter(MinSchedulingPeriod.class, Long.toString(minSchedulingPeriod));
    jcb.bindNamedParameter(EventProcessorIncreaseNum.class, Integer.toString(eventProcessorIncreaseNum));
    jcb.bindNamedParameter(GroupSchedModelType.class, groupSchedModelType);
    jcb.bindNamedParameter(GroupAware.class, Boolean.toString(groupAware));
    jcb.bindNamedParameter(DispatcherThreadNum.class, Integer.toString(dispatcherThreadNum));
    jcb.bindNamedParameter(GroupRebalancingPeriod.class, Long.toString(rebalancingPeriod));

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
        .registerShortNameOfClass(CfsSchedulingPeriod.class)
        .registerShortNameOfClass(MinSchedulingPeriod.class)
        .registerShortNameOfClass(EventProcessorIncreaseNum.class)
        .registerShortNameOfClass(GroupSchedModelType.class)
        .registerShortNameOfClass(DispatcherThreadNum.class)
        .registerShortNameOfClass(GroupAware.class)
        .registerShortNameOfClass(Rebalancing.class)
        .registerShortNameOfClass(GroupAssignerType.class)
        .registerShortNameOfClass(GroupRebalancingPeriod.class);
  }
}
