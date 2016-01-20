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
import edu.snu.mist.task.parameter.NumExecutors;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public final class OperatorChainAllocatorTest {

  /**
   * Test if the OperatorChainAllocator allocates 10 operatorChains to 4 executors in round-robin way.
   * @throws InjectionException
   */
  @Test
  public void roundRobinAllocationTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Set 4 mist executors
    jcb.bindNamedParameter(NumExecutors.class, "4");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ExecutorListProvider executorListProvider = injector.getInstance(ExecutorListProvider.class);
    final DAG<OperatorChain> operatorChainDAG = new AdjacentListDAG<>();
    final OperatorChainAllocator operatorChainAllocator =
        injector.getInstance(DefaultOperatorChainAllocatorImpl.class);

    // Set operator chain: 10 vertices and linear DAG
    OperatorChain src = injector.getInstance(OperatorChain.class);
    operatorChainDAG.addVertex(src);
    for (int i = 1; i < 10; i++) {
      final Injector newInjector = Tang.Factory.getTang().newInjector();
      final OperatorChain dest = newInjector.getInstance(OperatorChain.class);
      operatorChainDAG.addVertex(dest);
      operatorChainDAG.addEdge(src, dest);
      src = dest;
    }

    operatorChainAllocator.allocate(operatorChainDAG);
    // check validation
    final Iterator<OperatorChain> operatorChainIterator = GraphUtils.topologicalSort(operatorChainDAG);
    final List<MistExecutor> executors = executorListProvider.getExecutors();
    int index = 0;
    while (operatorChainIterator.hasNext()) {
      final OperatorChain operatorChain = operatorChainIterator.next();
      Assert.assertEquals(executors.get(index), operatorChain.getExecutor());
      index += 1;
    }
  }
}
