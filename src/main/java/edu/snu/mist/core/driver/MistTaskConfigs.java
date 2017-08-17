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

import edu.snu.mist.core.driver.parameters.DeactivationEnabled;
import edu.snu.mist.core.driver.parameters.ExecutionModelOption;
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.parameters.MqttSinkKeepAliveSec;
import edu.snu.mist.core.parameters.MqttSourceKeepAliveSec;
import edu.snu.mist.core.parameters.NumPeriodicSchedulerThreads;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.core.task.ClassLoaderProvider;
import edu.snu.mist.core.task.NoSharingURLClassLoaderProvider;
import edu.snu.mist.core.task.OperatorChainFactory;
import edu.snu.mist.core.task.QueryManager;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.eventProcessors.parameters.EventProcessorLowerBound;
import edu.snu.mist.core.task.eventProcessors.parameters.EventProcessorUpperBound;
import edu.snu.mist.core.task.eventProcessors.parameters.GracePeriod;
import edu.snu.mist.core.task.threadbased.ThreadBasedOperatorChainFactory;
import edu.snu.mist.core.task.threadbased.ThreadBasedQueryManagerImpl;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.CommandLine;

import javax.inject.Inject;

/**
 * Configuration of the mist task.
 */
public final class MistTaskConfigs {

  private static final int MAX_PORT_NUM = 65535;

  /**
   * The number of event processors of a MistTask.
   */
  private final int numEventProcessors;

  /**
   * Temporary folder path for storing temporay jar files.
   */
  private final String tempFolderPath;

  /**
   * The number of threads for the periodic service scheduler.
   */
  private final int numSchedulerThreads;

  /**
   * Enabling the merging of queries in Mist task.
   */
  private final boolean mergingEnabled;

  /**
   * Enabling the deactivation of queries in Mist task.
   */
  private final boolean deactivationEnabled;

  /**
   * The execution model of Mist.
   */
  private final int executionModelOption;

  /**
   * Configuration for execution model 2 (global scheduling).
   */
  private final MistGroupSchedulingTaskConfigs option2TaskConfigs;

  /**
   * The lowest number of event processors.
   */
  private final int eventProcessorLowerBound;

  /**
   * The highest number of event processors.
   */
  private final int eventProcessorUpperBound;

  /**
   * The grace period for adjusting the number of event processors.
   */
  private final int gracePeriod;

  /**
   * The mqtt keep alive time for sources in seconds.
   */
  private final int mqttSourceKeepAliveSec;

  /**
   * The mqtt keep alive time for sinks in seconds.
   */
  private final int mqttSinkKeepAliveSec;

  @Inject
  private MistTaskConfigs(@Parameter(DefaultNumEventProcessors.class) final int numEventProcessors,
                          @Parameter(TempFolderPath.class) final String tempFolderPath,
                          @Parameter(NumPeriodicSchedulerThreads.class) final int numSchedulerThreads,
                          @Parameter(MergingEnabled.class) final boolean mergingEnabled,
                          @Parameter(DeactivationEnabled.class) final boolean deactivationEnabled,
                          @Parameter(ExecutionModelOption.class) final int executionModelOption,
                          @Parameter(EventProcessorLowerBound.class) final int eventProcessorLowerBound,
                          @Parameter(EventProcessorUpperBound.class) final int eventProcessorUpperBound,
                          @Parameter(GracePeriod.class) final int gracePeriod,
                          @Parameter(MqttSourceKeepAliveSec.class) final int mqttSourceKeepAliveSec,
                          @Parameter(MqttSinkKeepAliveSec.class) final int mqttSinkKeepAliveSec,
                          final MistGroupSchedulingTaskConfigs option2TaskConfigs) {
    this.numEventProcessors = numEventProcessors;
    this.tempFolderPath = tempFolderPath;
    this.numSchedulerThreads = numSchedulerThreads;
    this.mergingEnabled = mergingEnabled;
    this.deactivationEnabled = deactivationEnabled;
    this.eventProcessorUpperBound = eventProcessorUpperBound;
    this.eventProcessorLowerBound = eventProcessorLowerBound;
    this.executionModelOption = executionModelOption;
    this.gracePeriod = gracePeriod;
    this.option2TaskConfigs = option2TaskConfigs;
    this.mqttSourceKeepAliveSec = mqttSourceKeepAliveSec;
    this.mqttSinkKeepAliveSec = mqttSinkKeepAliveSec;
  }

  /**
   * Get the configuration for the execution model.
   */
  private Configuration getConfigurationForExecutionModel() {
    switch (executionModelOption) {
      case 2:
        return option2TaskConfigs.getConfiguration();
      case 3:
        return getOption3Configuration();
      default:
        throw new RuntimeException("Undefined execution model: " + executionModelOption);
    }
  }

  /**
   * Get the configuration for thread-based execution model
   * that creates a new thread per operator chain.
   */
  private Configuration getOption3Configuration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(QueryManager.class, ThreadBasedQueryManagerImpl.class);
    jcb.bindImplementation(OperatorChainFactory.class, ThreadBasedOperatorChainFactory.class);
    jcb.bindImplementation(ClassLoaderProvider.class, NoSharingURLClassLoaderProvider.class);
    return jcb.build();
  }
  /**
   * Get the task configuration.
   * @return configuration
   */
  public Configuration getConfiguration() {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    // Parameter
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(numEventProcessors));
    jcb.bindNamedParameter(TempFolderPath.class, tempFolderPath);
    jcb.bindNamedParameter(NumPeriodicSchedulerThreads.class, Integer.toString(numSchedulerThreads));
    jcb.bindNamedParameter(MergingEnabled.class, Boolean.toString(mergingEnabled));
    jcb.bindNamedParameter(DeactivationEnabled.class, Boolean.toString(deactivationEnabled));
    jcb.bindNamedParameter(EventProcessorLowerBound.class, Integer.toString(eventProcessorLowerBound));
    jcb.bindNamedParameter(EventProcessorUpperBound.class, Integer.toString(eventProcessorUpperBound));
    jcb.bindNamedParameter(GracePeriod.class, Integer.toString(gracePeriod));
    jcb.bindNamedParameter(MqttSourceKeepAliveSec.class, Integer.toString(mqttSourceKeepAliveSec));
    jcb.bindNamedParameter(MqttSinkKeepAliveSec.class, Integer.toString(mqttSinkKeepAliveSec));

    return Configurations.merge(jcb.build(), getConfigurationForExecutionModel());
  }

  /**
   * Add parameters to the command line.
   * @param commandLine command line
   * @return command line where parameters are added
   */
  public static CommandLine addCommandLineConf(final CommandLine commandLine) {
    final CommandLine cmd = commandLine
        .registerShortNameOfClass(DefaultNumEventProcessors.class)
        .registerShortNameOfClass(TempFolderPath.class)
        .registerShortNameOfClass(NumPeriodicSchedulerThreads.class)
        .registerShortNameOfClass(MergingEnabled.class)
        .registerShortNameOfClass(DeactivationEnabled.class)
        .registerShortNameOfClass(EventProcessorLowerBound.class)
        .registerShortNameOfClass(EventProcessorUpperBound.class)
        .registerShortNameOfClass(ExecutionModelOption.class)
        .registerShortNameOfClass(GracePeriod.class)
        .registerShortNameOfClass(MqttSourceKeepAliveSec.class)
        .registerShortNameOfClass(MqttSinkKeepAliveSec.class);
    return MistGroupSchedulingTaskConfigs.addCommandLineConf(cmd);
  }
}
