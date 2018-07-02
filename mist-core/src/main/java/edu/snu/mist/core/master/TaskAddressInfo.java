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
package edu.snu.mist.core.master;

/**
 * The class which contains port allocation info for MistTasks.
 */
public final class TaskAddressInfo {

  /**
   * The hostname of the task.
   */
  private final String hostname;

  /**
   * The master-to-task port.
   */
  private final int masterToTaskPort;

  /**
   * The client-to-task port.
   */
  private final int clientToTaskPort;

  public TaskAddressInfo(
      final String hostname,
      final int clientToTaskPort,
      final int masterToTaskPort) {
    this.hostname = hostname;
    this.clientToTaskPort = clientToTaskPort;
    this.masterToTaskPort = masterToTaskPort;
  }

  public String getHostname() {
    return hostname;
  }

  public int getMasterToTaskPort() {
    return masterToTaskPort;
  }

  public int getClientToTaskPort() {
    return clientToTaskPort;
  }
}
