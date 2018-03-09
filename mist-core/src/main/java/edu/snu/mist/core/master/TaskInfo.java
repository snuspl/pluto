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
 * The class which contains MistTask information maintained by master.
 */
public final class TaskInfo {

  /**
   * The cpu load of the task.
   */
  private double cpuLoad;

  public TaskInfo() {
    this.cpuLoad = 0.0;
  }

  public double getCpuLoad() {
    return this.cpuLoad;
  }

  public void setCpuLoad(final double updatedCpuLoad) {
    this.cpuLoad = updatedCpuLoad;
  }
}
