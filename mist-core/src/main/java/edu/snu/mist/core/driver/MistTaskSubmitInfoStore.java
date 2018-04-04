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

import javax.inject.Inject;

/**
 * The shared store for MistTask Tang configuration.
 */
public final class MistTaskSubmitInfoStore {

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

  @Inject
  private MistTaskSubmitInfoStore() {
    this.taskConfiguration = null;
    this.newRatio = -1;
    this.reservedCodeCacheSize = -1;
  }

  public void setTaskConfiguration(final Configuration taskConfiguration) {
    this.taskConfiguration = taskConfiguration;
  }

  public Configuration getTaskConfiguration() {
    return taskConfiguration;
  }

  public int getNewRatio() {
    return newRatio;
  }

  public void setNewRatio(final int newRatio) {
    this.newRatio = newRatio;
  }

  public int getReservedCodeCacheSize() {
    return reservedCodeCacheSize;
  }

  public void setReservedCodeCacheSize(final int reservedCodeCacheSize) {
    this.reservedCodeCacheSize = reservedCodeCacheSize;
  }

  public void reset() {
    this.taskConfiguration = null;
    this.newRatio = -1;
    this.reservedCodeCacheSize = -1;
  }

}
