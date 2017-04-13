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
package edu.snu.mist.core.task.utils;

/**
 * This class is for unique id and configuration generation.
 */
public final class IdAndConfGenerator {

  /**
   * A variable for creating configurations.
   */
  private int confCount;

  /**
   * A variable for creating identifiers.
   */
  private int idCount;

  public IdAndConfGenerator() {
    this.confCount = 0;
    this.idCount = 0;
  }

  /**
   * Generate an identifier.
   * @return identifier
   */
  public String generateId() {
    idCount += 1;
    return Integer.toString(idCount);
  }

  /**
   * Generate a configuration for source, operator, and sink.
   * @return configuration
   */
  public String generateConf() {
    confCount += 1;
    return Integer.toString(confCount);
  }
}
