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
import edu.snu.mist.core.task.globalsched.GlobalSchedEventProcessorFactory;
import edu.snu.mist.core.task.globalsched.GroupAwareGlobalSchedQueryManagerImpl;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import edu.snu.mist.core.task.globalsched.SchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.CfsSchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.VtimeBasedNextGroupSelectorFactory;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.metrics.DefaultEventProcessorNumAssigner;
import edu.snu.mist.core.task.globalsched.metrics.MISDEventProcessorNumAssigner;
import edu.snu.mist.core.task.globalsched.parameters.*;
import edu.snu.mist.core.task.metrics.EventProcessorNumAssigner;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configuration of the mist task for option 2.
 */
public final class MistOption2TaskConfigs {

  private final String epaType;
  private final double cpuUtilLowThreshold;
  private final long eventNumHighThreshold;
  private final long eventNumLowThreshold;
  private final int eventProcessorDecreaseNum;
  private final double eventProcessorIncreaseRate;
  private final long cfsSchedulingPeriod;
  private final long minSchedulingPeriod;

  @Inject
  private MistOption2TaskConfigs(
      @Parameter(EventProcessorNumAssignerType.class) final String epaType,
      @Parameter(CpuUtilLowThreshold.class) final double cpuUtilLowThreshold,
      @Parameter(EventNumHighThreshold.class) final long eventNumHighThreshold,
      @Parameter(EventNumLowThreshold.class) final long eventNumLowThreshold,
      @Parameter(EventProcessorDecreaseNum.class) final int eventProcessorDecreaseNum,
      @Parameter(EventProcessorIncreaseRate.class) final double eventProcessorIncreaseRate,
      @Parameter(CfsSchedulingPeriod.class) final long cfsSchedulingPeriod,
      @Parameter(MinSchedulingPeriod.class) final long minSchedulingPeriod) {
    this.epaType = epaType;
    this.cpuUtilLowThreshold = cpuUtilLowThreshold;
    this.eventNumHighThreshold = eventNumHighThreshold;
    this.eventNumLowThreshold = eventNumLowThreshold;
    this.eventProcessorDecreaseNum = eventProcessorDecreaseNum;
    this.eventProcessorIncreaseRate = eventProcessorIncreaseRate;
    this.cfsSchedulingPeriod = cfsSchedulingPeriod;
    this.minSchedulingPeriod = minSchedulingPeriod;
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
      default:
        throw new RuntimeException("No event processor num assigner type: " + epaType);
    }
  }

  /**
   * Get the configuration for execution model 2 that creates a shared thread pool
   * where each thread executes multiple groups.
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedEventProcessorFactory.class);
    jcb.bindImplementation(EventProcessorNumAssigner.class, getEpaClass());
    jcb.bindImplementation(EventProcessorManager.class, DefaultEventProcessorManager.class);
    jcb.bindImplementation(SchedulingPeriodCalculator.class, CfsSchedulingPeriodCalculator.class);
    jcb.bindImplementation(NextGroupSelectorFactory.class, VtimeBasedNextGroupSelectorFactory.class);

    jcb.bindNamedParameter(CpuUtilLowThreshold.class, Double.toString(cpuUtilLowThreshold));
    jcb.bindNamedParameter(EventNumHighThreshold.class, Long.toString(eventNumHighThreshold));
    jcb.bindNamedParameter(EventNumLowThreshold.class, Long.toString(eventNumLowThreshold));
    jcb.bindNamedParameter(EventProcessorDecreaseNum.class, Integer.toString(eventProcessorDecreaseNum));
    jcb.bindNamedParameter(EventProcessorIncreaseRate.class, Double.toString(eventProcessorIncreaseRate));
    jcb.bindNamedParameter(CfsSchedulingPeriod.class, Long.toString(cfsSchedulingPeriod));
    jcb.bindNamedParameter(MinSchedulingPeriod.class, Long.toString(minSchedulingPeriod));

    return jcb.build();
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
        .registerShortNameOfClass(MinSchedulingPeriod.class);
  }
}
