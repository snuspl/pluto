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

import javax.inject.Inject;
import java.util.Iterator;
import java.util.List;

/**
 * A default operator chain allocator
 * which allocates OperatorChains to MistExecutors in round-robin way.
 */
final class DefaultOperatorChainAllocatorImpl implements OperatorChainAllocator {

  /**
   * A list of MistExecutors.
   */
  private final List<MistExecutor> executors;

  /**
   * An index of MistExecutors.
   */
  private int index;

  @Inject
  private DefaultOperatorChainAllocatorImpl(
      final ExecutorListProvider executorListProvider) {
    this.executors = executorListProvider.getExecutors();
    this.index = 0;
  }

  /**
   * This allocates OperatorChains to MistExecutors in round-robin way.
   * @param dag a DAG of OperatorChain
   */
  @Override
  public void allocate(final DAG<OperatorChain> dag) {
    final Iterator<OperatorChain> operatorChainIterator = GraphUtils.topologicalSort(dag);
    while (operatorChainIterator.hasNext()) {
      final OperatorChain operatorChain = operatorChainIterator.next();
      operatorChain.setExecutor(executors.get(index));
      index = (index + 1) % executors.size();
    }
  }
}
