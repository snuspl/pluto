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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.ExecutionDags;
import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class DefaultMetaGroupImpl implements MetaGroup {

  private final QueryStarter queryStarter;
  private final QueryRemover queryRemover;
  private final ExecutionDags executionDags;
  private final List<Group> groups;

  private final AtomicInteger numGroups = new AtomicInteger(0);

  @Inject
  private DefaultMetaGroupImpl(final QueryStarter queryStarter,
                               final QueryRemover queryRemover,
                               final ExecutionDags executionDags) {
    this.queryStarter = queryStarter;
    this.queryRemover = queryRemover;
    this.executionDags = executionDags;
    this.groups = new LinkedList<>();
  }

  @Override
  public QueryStarter getQueryStarter() {
    return queryStarter;
  }

  @Override
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }

  @Override
  public ExecutionDags getExecutionDags() {
    return executionDags;
  }

  @Override
  public List<Group> getGroups() {
    return groups;
  }

  @Override
  public boolean addGroup(final Group group) {
    group.setMetaGroup(this);
    numGroups.incrementAndGet();
    return groups.add(group);
  }

  @Override
  public AtomicInteger numGroups() {
    return numGroups;
  }
}