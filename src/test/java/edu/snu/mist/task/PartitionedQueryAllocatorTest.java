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

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.parameters.NumExecutors;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public final class PartitionedQueryAllocatorTest {

  /**
   * Test if the PartitionedQueryAllocator allocates 10 partitionedQueries which are sequentially connected
   * to 4 executors in round-robin way.
   * @throws InjectionException
   */
  @Test
  public void roundRobinAllocationTest() throws InjectionException {
    final int numExecutors = 4;
    final int numPartitionedQueries = 10;
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Create 4 MistExecutors
    jcb.bindNamedParameter(NumExecutors.class, Integer.toString(4));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ExecutorListProvider executorListProvider = injector.getInstance(ExecutorListProvider.class);
    final DAG<PartitionedQuery> partitionedQueryDAG = new AdjacentListDAG<>();
    final PartitionedQueryAllocator partitionedQueryAllocator =
        injector.getInstance(DefaultPartitionedQueryAllocatorImpl.class);

    // Create 10 PartitionedQueries which are sequentially connected.
    PartitionedQuery src = new DefaultPartitionedQuery();
    partitionedQueryDAG.addVertex(src);
    for (int i = 1; i < numPartitionedQueries; i++) {
      final PartitionedQuery dest = new DefaultPartitionedQuery();
      partitionedQueryDAG.addVertex(dest);
      partitionedQueryDAG.addEdge(src, dest);
      src = dest;
    }

    partitionedQueryAllocator.allocate(partitionedQueryDAG);
    // check if the PartitionedQueries are allocated to the MistExecutors in round-robin way
    final Iterator<PartitionedQuery> partitionedQueryIterator = GraphUtils.topologicalSort(partitionedQueryDAG);
    final List<MistExecutor> executors = executorListProvider.getExecutors();
    int index = 0;
    while (partitionedQueryIterator.hasNext()) {
      final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
      Assert.assertEquals("Assigned executor should be " + executors.get(index).getIdentifier(),
          executors.get(index), partitionedQuery.getExecutor());
      // circular
      index = (index + 1) % numExecutors;
    }
  }

  @Test
  public void simpleExecutorChainAllocatorTest() throws InjectionException {
    final int numQueries = 10;
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Create a chain allocator which creates a executor for each query.
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final SimpleExecutorChainAllocatorImpl simpleExecutorChainAllocator =
        injector.getInstance(SimpleExecutorChainAllocatorImpl.class);

    for (int i = 0; i < numQueries; i++) {
      // DAG<OperatorChain> == a query
      final DAG<OperatorChain> operatorChainDAG = new AdjacentListDAG<>();
      final OperatorChain operatorChain = new DefaultOperatorChain();
      operatorChainDAG.addVertex(operatorChain);
      simpleExecutorChainAllocator.allocate(operatorChainDAG);
      final List<MistExecutor> executors = simpleExecutorChainAllocator.getExecutors();
      Assert.assertEquals("Assigned executor should be " + executors.get(i).getIdentifier(),
          executors.get(i), operatorChain.getExecutor());
    }
  }
}
