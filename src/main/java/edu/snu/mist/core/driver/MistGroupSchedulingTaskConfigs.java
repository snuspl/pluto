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
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.eventProcessors.DefaultEventProcessorManager;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.globalsched.*;
import edu.snu.mist.core.task.globalsched.cfs.CfsSchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.metrics.DefaultEventProcessorNumAssigner;
import edu.snu.mist.core.task.globalsched.metrics.MISDEventProcessorNumAssigner;
import edu.snu.mist.core.task.globalsched.parameters.*;
import edu.snu.mist.core.task.globalsched.roundrobin.WeightedRRNextGroupSelectorFactory;
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
      @Parameter(GroupSchedModelType.class) final String groupSchedModelType) {
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
   * - blocking: round-robin with blocking
   * - nonblocking: round-robin without blocking
   */
  private Configuration getConfigurationForExecutionModel() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    switch (groupSchedModelType) {
      case "blocking":
        jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedEventProcessorFactory.class);
        break;
      case "nonblocking":
        jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
        jcb.bindImplementation(NextGroupSelectorFactory.class, WeightedRRNextGroupSelectorFactory.class);
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

    jcb.bindNamedParameter(CpuUtilLowThreshold.class, Double.toString(cpuUtilLowThreshold));
    jcb.bindNamedParameter(EventNumHighThreshold.class, Double.toString(eventNumHighThreshold));
    jcb.bindNamedParameter(EventNumLowThreshold.class, Double.toString(eventNumLowThreshold));
    jcb.bindNamedParameter(EventProcessorDecreaseNum.class, Integer.toString(eventProcessorDecreaseNum));
    jcb.bindNamedParameter(EventProcessorIncreaseRate.class, Double.toString(eventProcessorIncreaseRate));
    jcb.bindNamedParameter(CfsSchedulingPeriod.class, Long.toString(cfsSchedulingPeriod));
    jcb.bindNamedParameter(MinSchedulingPeriod.class, Long.toString(minSchedulingPeriod));
    jcb.bindNamedParameter(EventProcessorIncreaseNum.class, Integer.toString(eventProcessorIncreaseNum));
    jcb.bindNamedParameter(GroupSchedModelType.class, groupSchedModelType);

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
        .registerShortNameOfClass(GroupSchedModelType.class);
  }
}