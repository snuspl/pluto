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
package edu.snu.mist.task;

import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.executor.parameters.MistExecutorId;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A chain allocator for simple executor model.
 * The simple executor model creates an executor for each query.
 */
final class SimplePartitionedQueryImpl implements PartitionedQueryAllocator {

  private final List<MistExecutor> executors;
  private final AtomicInteger index;

  @Inject
  private SimplePartitionedQueryImpl() {
    this.executors = new LinkedList<>();
    this.index = new AtomicInteger();
  }

  /**
   * This creates a new MistExecutor for the query.
   * @param dag a DAG of PartitionedQuery
   */
  @Override
  public void allocate(final DAG<PartitionedQuery> dag) {
    final Iterator<PartitionedQuery> partitionedQueryIterator = GraphUtils.topologicalSort(dag);
    while (partitionedQueryIterator.hasNext()) {
      final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
      final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
      final StringBuffer sb = new StringBuffer();
      sb.append("MistExecutor-"); sb.append(index.getAndIncrement());
      jcb.bindNamedParameter(MistExecutorId.class, sb.toString());
      final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
      final MistExecutor executor;
      try {
        executor = injector.getInstance(MistExecutor.class);
        executors.add(executor);
        partitionedQuery.setExecutor(executor);
      } catch (final InjectionException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  public List<MistExecutor> getExecutors() {
    return executors;
  }
}
