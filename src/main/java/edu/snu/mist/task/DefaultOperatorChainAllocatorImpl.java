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
import edu.snu.mist.task.executor.MistExecutor;

import javax.inject.Inject;
import java.util.Set;

// TODO[MIST-42]: Implement a simple round-robin allocator
final class DefaultOperatorChainAllocatorImpl implements OperatorChainAllocator {

  @Inject
  private DefaultOperatorChainAllocatorImpl() {
  }

  @Override
  public void allocate(final Set<MistExecutor> executors, final DAG<OperatorChain> dag) {
    throw new RuntimeException("DefaultOperatorChainAllocatorImpl.allocate is not implemented yet");
  }
}
