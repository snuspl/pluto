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
package edu.snu.mist.core.task;

/**
 * This class holds the configuration of the execution vertex.
 */
public final class ConfigVertex {

  /**
   * The type of this vertex (src, operator, sink).
   */
  private final ExecutionVertex.Type type;

  /**
   * The configuration of the vertex.
   */
  private final String configuration;

  public ConfigVertex(final ExecutionVertex.Type type,
                      final String configuration) {
    this.type = type;
    this.configuration = configuration;
  }

  public ExecutionVertex.Type getType() {
    return type;
  }

  public String getConfiguration() {
    return configuration;
  }
}
