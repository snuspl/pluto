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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroLogicalPlan;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default implementation of QueryManager.
 */
@SuppressWarnings("unchecked")
final class DefaultQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(DefaultQueryManagerImpl.class.getName());

  /**
   * Map of query id and logical plan.
   */
  private final ConcurrentMap<String, DAG<LogicalVertex, MISTEdge>> logicalPlanMap;

  /**
   * An operator chain manager.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * A thread manager.
   */
  private final ThreadManager threadManager;

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A physical and logical plan generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * Default query manager in MistTask.
   * @param dagGenerator the generator that generates the logical and physical plan from avro logical plan.
   * @param threadManager thread manager
   */
  @Inject
  private DefaultQueryManagerImpl(final DagGenerator dagGenerator,
                                  final ThreadManager threadManager,
                                  final OperatorChainManager operatorChainManager,
                                  final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final QueryInfoStore planStore) {
    this.logicalPlanMap = new ConcurrentHashMap<>();
    this.operatorChainManager = operatorChainManager;
    this.dagGenerator = dagGenerator;
    this.threadManager = threadManager;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
  }

  /**
   * It converts the avro logical plan (query) to the physical plan,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro logical plan into disk.
   * We can regenerate the queries from the stored avro logical plan.
   * @param tuple a pair of the query id and the avro logical plan
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroLogicalPlan> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // 1) Saves the avro logical plan to the PlanStore and
      // converts the avro logical plan to the logical and physical plan
      planStore.savePlan(tuple);
      final LogicalAndExecutionDag logicalAndExecutionDag = dagGenerator.generate(tuple);
      // Store the logical plan in memory
      logicalPlanMap.putIfAbsent(tuple.getKey(), logicalAndExecutionDag.getLogicalDag());
      // Execute the physical plan
      start(logicalAndExecutionDag.getExecutionDag());
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.getMessage()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    threadManager.close();
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of the query
   */
  private void start(final DAG<ExecutionVertex, MISTEdge> physicalPlan) {
    final List<PhysicalSource> sources = new LinkedList<>();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(physicalPlan);
    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = physicalPlan.getEdges(source);
          // 3) Sets output emitters
          source.getEventGenerator().setOutputEmitter(new SourceOutputEmitter<>(nextOps));
          sources.add(source);
          break;
        }
        case OPERATOR_CHIAN: {
          // 2) Inserts the OperatorChain to OperatorChainManager.
          final OperatorChain operatorChain = (OperatorChain)executionVertex;
          operatorChainManager.insert(operatorChain);
          final Map<ExecutionVertex, MISTEdge> edges =
              physicalPlan.getEdges(operatorChain);
          // 3) Sets output emitters
          operatorChain.setOutputEmitter(new OperatorOutputEmitter(edges));
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + executionVertex.getType());
      }
    }

    // 4) starts to receive input data stream from the sources
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  /**
   * Deletes queries from MIST.
   * TODO[MIST-431] To delete the queries, we need to implement reference counting mechanism.
   */
  @Override
  public QueryControlResult delete(final String queryId) {
    throw new RuntimeException("Deleting queries is not implemented yet. We need to address [MIST-431]");
  }
}
