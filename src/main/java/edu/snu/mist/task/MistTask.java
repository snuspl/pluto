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
import edu.snu.mist.task.sources.SourceGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * MistTask does the following things:
 * 1) creates MistExecutors,
 * 2) receives logical plans from clients and converts the logical plans to physical plans,
 * 3) chains the physical operators and make OperatorChain,
 * 4) allocates the OperatorChains to the MistExecutors,
 * 5) and sets the OutputEmitters of the SourceGenerator and OperatorChains
 * to forward their outputs to next OperatorChains.
 */
@SuppressWarnings("unchecked")
public final class MistTask implements Task {
  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  /**
   * Default constructor of MistTask.
   * @param chainAllocator an allocator which allocates a OperatorChain to a MistExecutor
   * @param physicalToChainedPlan a converter which chains operator and make OperatorChains
   * @param receiver logical plan receiver which converts the logical plans to physical plans
   * @param executorListProvider executor list provider which returns the list of executors
   * @throws InjectionException
   */
  @Inject
  private MistTask(final OperatorChainAllocator chainAllocator,
                   final PhysicalToChainedPlan physicalToChainedPlan,
                   final LogicalPlanReceiver receiver,
                   final ExecutorListProvider executorListProvider) throws InjectionException {
    this.countDownLatch = new CountDownLatch(1);
    final Injector injector = Tang.Factory.getTang().newInjector();

    // 1) creates MistExecutors in executorListProvider
    // 2) Receives logical plans and converts to physical plans
    receiver.setHandler(physicalPlan -> {
      // 3) Chains the physical operators and make OperatorChain.
      final PhysicalPlan<OperatorChain> chainedPlan =
          physicalToChainedPlan.convertToChainedPlan(physicalPlan);

      final DAG<OperatorChain> chainedOperators = chainedPlan.getOperators();
      // 4) Allocates the OperatorChains to the MistExecutors
      chainAllocator.allocate(chainedOperators);

      // 5) Sets the OutputEmitters of the OperatorChains
      final Iterator<OperatorChain> iterator = GraphUtils.topologicalSort(chainedOperators);
      while (iterator.hasNext()) {
        final OperatorChain operatorChain = iterator.next();
        final Set<OperatorChain> neighbors = chainedOperators.getNeighbors(operatorChain);
        operatorChain.setOutputEmitter(new DefaultOutputEmitter(operatorChain, neighbors));
      }

      // 5) Sets the OutputEmitters of the SourceGenerator and OperatorChains
      for (final SourceGenerator src : chainedPlan.getSourceMap().keySet()) {
        // Submits a job to the MistExecutor
        src.setOutputEmitter(inputs -> {
          final Set<OperatorChain> nextOps = chainedPlan.getSourceMap().get(src);
          for (final OperatorChain nextOp : nextOps) {
            final MistExecutor executor = nextOp.getExecutor();
            final OperatorChainJob operatorChainJob = new DefaultOperatorChainJob(nextOp, inputs);
            executor.submit(operatorChainJob);
          }
        });
        // start to process input streams
        src.start();
      }
    });
  }


  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    return new byte[0];
  }
}
