/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api;

import edu.snu.mist.api.sink.Sink;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The basic implementation class for MISTQuery.
 */
public final class MISTQueryImpl implements MISTQuery {

  /**
   * A set of sink.
   */
  private final Set<Sink> querySinks;

  public MISTQueryImpl(final Set<Sink> querySinks) {
    this.querySinks = querySinks;
  }

  public MISTQueryImpl(final Sink querySink) {
    this.querySinks = new HashSet<>(Arrays.asList(querySink));
  }

  /**
   * @return A set of sinks
   */
  @Override
  public Set<Sink> getQuerySinks() {
    return this.querySinks;
  }
}
