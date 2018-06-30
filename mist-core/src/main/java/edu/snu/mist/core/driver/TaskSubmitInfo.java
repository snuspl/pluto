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

import org.apache.reef.tang.Configuration;

/**
 * The shared store for MistTask Tang configuration.
 */
public final class TaskSubmitInfo {

  /**
   * The task id.
   */
  private String taskId;

  /**
   * The stored task configuration.
   */
  private Configuration taskConfiguration;

  /**
   * The new ratio for configuring JVM GC.
   */
  private int newRatio;

  /**
   * The reserved code cache size for Java JIT interpreter.
   */
  private int reservedCodeCacheSize;

  private TaskSubmitInfo(final String taskId,
                         final Configuration taskConfiguration,
                         final int newRatio,
                         final int reservedCodeCacheSize) {
    this.taskId = taskId;
    this.taskConfiguration = taskConfiguration;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
  }

  public String getTaskId() {
    return taskId;
  }

  public Configuration getTaskConfiguration() {
    return taskConfiguration;
  }

  public int getNewRatio() {
    return newRatio;
  }

  public int getReservedCodeCacheSize() {
    return reservedCodeCacheSize;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String taskId;

    private Configuration taskConfiguration;

    private int newRatio;

    private int reservedCodeCacheSize;

    Builder() {
      this.taskId = null;
      this.taskConfiguration = null;
      this.newRatio = -1;
      this.reservedCodeCacheSize = -1;
    }

    public Builder setTaskId(final String taskId) {
      this.taskId = taskId;
      return this;
    }

    public Builder setTaskConfiguration(final Configuration taskConfiguration) {
      this.taskConfiguration = taskConfiguration;
      return this;
    }

    public Builder setNewRatio(final int newRatio) {
      this.newRatio = newRatio;
      return this;
    }

    public Builder setReservedCodeCacheSize(final int reservedCodeCacheSize) {
      this.reservedCodeCacheSize = reservedCodeCacheSize;
      return this;
    }

    public TaskSubmitInfo build() {
      return new TaskSubmitInfo(taskId, taskConfiguration, newRatio, reservedCodeCacheSize);
    }
  }

}
