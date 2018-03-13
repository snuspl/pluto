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
package edu.snu.mist.core.task;

import java.util.HashMap;
import java.util.Map;

/**
 * This class holds the configuration of the execution vertex.
 */
public final class ConfigVertex {

  /**
   * The id of this vertex.
   */
  private final String id;

  /**
   * The type of this vertex (src, operator, sink).
   */
  private final ExecutionVertex.Type type;

  /**
   * State of this ConfigVertex, if this is a stateful operator from a checkpoint.
   */
  private final Map<String, Object> state;

  /**
   * The latest Checkpoint timestamp.
   * It is initially 0, and stays 0 if this vertex is stateless.
   */
  private final long latestCheckpointTimestamp;

  /**
   * The configuration of the vertex.
   */
  private final String configuration;

  public ConfigVertex(final String id,
                      final ExecutionVertex.Type type,
                      final String configuration,
                      final Map<String, Object> state,
                      final long latestCheckpointTimestamp) {
    this.id = id;
    this.type = type;
    this.configuration = configuration;
    this.state = new HashMap<>();
    if (state != null) {
      this.state.putAll(state);
    }
    this.latestCheckpointTimestamp = latestCheckpointTimestamp;
  }

  public ConfigVertex(final String id,
                      final ExecutionVertex.Type type,
                      final String configuration) {
    this(id, type, configuration, null, 0L);
  }

  public String getId() {
    return id;
  }

  public ExecutionVertex.Type getType() {
    return type;
  }

  public String getConfiguration() {
    return configuration;
  }

  public Map<String, Object> getState() {
    return state;
  }

  public long getLatestCheckpointTimestamp() {
    return latestCheckpointTimestamp;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ConfigVertex that = (ConfigVertex) o;

    if (!id.equals(that.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }
}
