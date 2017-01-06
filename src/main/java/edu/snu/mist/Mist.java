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

package edu.snu.mist;

import edu.snu.mist.common.rpc.RPCServerPort;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.NumTasks;
import edu.snu.mist.core.parameters.TaskMemorySize;
import edu.snu.mist.core.parameters.DriverRuntimeType;
import edu.snu.mist.core.MistLauncher;
import edu.snu.mist.core.parameters.NumThreads;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.CommandLine;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main class for the MIST.
 */
public final class Mist {
  private static final Logger LOG = Logger.getLogger(Mist.class.getName());

  /**
   * Gets configurations from command line args.
   */
  private static Configuration getCommandLineConf(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final CommandLine commandLine = new CommandLine(jcb)
        .registerShortNameOfClass(DriverRuntimeType.class)
        .registerShortNameOfClass(NumTaskCores.class)
        .registerShortNameOfClass(NumThreads.class)
        .registerShortNameOfClass(NumTasks.class)
        .registerShortNameOfClass(RPCServerPort.class)
        .registerShortNameOfClass(TaskMemorySize.class)
        .processCommandLine(args);
    if (commandLine == null) { // Option '?' was entered and processCommandLine printed the help.
      return null;
    }
    return jcb.build();
  }

  /**
   * Start Mist Driver.
   * @param args command line parameters.
   */
  public static void main(final String[] args) throws Exception {
    final Configuration commandLineConf = getCommandLineConf(args);
    if (commandLineConf == null) {
      return;
    }
    final LauncherStatus status = MistLauncher
        .getLauncherFromConf(commandLineConf)
        .runFromConf(commandLineConf);
    LOG.log(Level.INFO, "Mist completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private Mist(){
  }
}
