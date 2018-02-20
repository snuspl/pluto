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
package edu.snu.mist.core.task.threadpool;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
public final class ThreadPoolQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(ThreadPoolQueryManagerImpl.class.getName());

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A execution and logical dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  //private final List<BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>>> threadsQueue;

  private final AtomicLong numQueries = new AtomicLong(0);

  private final int numThreads;

  private final ConcurrentMap<String, QueryProgress> queryStatus = new ConcurrentHashMap<>();

  private final AtomicLong queryIdCounter = new AtomicLong();

  private final ExecutorService executorService;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private ThreadPoolQueryManagerImpl(final DagGenerator dagGenerator,
                                     final ScheduledExecutorServiceWrapper schedulerWrapper,
                                     final QueryInfoStore planStore,
                                     @Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                     final ConfigDagGenerator configDagGenerator,
                                     final BatchQueryCreator batchQueryCreator) {
    this.executorService = Executors.newFixedThreadPool(defaultNumEventProcessors);
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.batchQueryCreator = batchQueryCreator;
    this.configDagGenerator = configDagGenerator;
    this.numThreads = defaultNumEventProcessors;
    //this.threadsQueue = new ArrayList<>(defaultNumEventProcessors);
  }

  /**
   * Create a submitted query.
   * It converts the avro operator chain dag (query) to the execution dag,
   * and executes the sources in order to receives data streams.
   * Before the queries are executed, it stores the avro dag into disk.
   * We can regenerate the queries from the stored avro dag.
   * @param tuple a pair of the query id and the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult create(final Tuple<String, AvroDag> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    final String queryId = Long.toString(queryIdCounter.getAndIncrement());

    queryStatus.put(queryId, new QueryProgress());

    try {
      // Create the submitted query
      // 1) Saves the avro dag to the PlanStore and
      // converts the avro dag to the logical and execution dag
      planStore.saveAvroDag(tuple);

      final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(tuple.getValue());
      final ExecutionDag executionDag =
          dagGenerator.generate(configDag, tuple.getValue().getJarFilePaths());

      // Execute the execution dag
      start(executionDag, queryStatus.get(queryId));

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.getMessage()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  /**
   * TODO[DELETE] this code is for test.
   * Start submitted queries in batch manner.
   * The operator chain dag will be duplicated for test.
   * @param tuple a pair of the query id and the avro dag
   * @return submission result
   */
  @Override
  public QueryControlResult createBatch(final Tuple<List<String>, AvroDag> tuple) {
    final List<String> queryIdList = tuple.getKey();
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryIdList.get(0));
    try {
      batchQueryCreator.duplicate(tuple, this);

      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey().get(0)));
      return queryControlResult;
    } catch (final Exception e) {
      e.printStackTrace();
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting from {0} to {1} batch query: {2}",
          new Object[] {queryIdList.get(0), queryIdList.get(queryIdList.size() - 1), e.toString()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }


  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of the query
   */
  private void start(final ExecutionDag physicalPlan,
                     final QueryProgress queryProgress) {
    final List<PhysicalSource> sources = new LinkedList<>();
    final DAG<ExecutionVertex, MISTEdge> dag = physicalPlan.getDag();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(dag);

    final int index = (int)numQueries.getAndIncrement() % numThreads;
    //final BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue = threadsQueue.get(index);
    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = dag.getEdges(source);
          // 3) Sets output emitters
          source.setOutputEmitter(new ThreadPoolOutputEmitter<>(nextOps, executorService, queryProgress));
          sources.add(source);
          break;
        }
        case OPERATOR: {
          // 2) Inserts the OperatorChain to OperatorChainManager.
          final PhysicalOperator physicalOp = (PhysicalOperator)executionVertex;
          final Map<ExecutionVertex, MISTEdge> edges =
              dag.getEdges(physicalOp);
          physicalOp.getOperator().setOutputEmitter(new OperatorOutputEmitter(edges));
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

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    planStore.close();
    executorService.shutdown();
  }

  /**
   * Deletes queries from MIST.
   */
  @Override
  public QueryControlResult delete(final String groupId, final String queryId) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    queryControlResult.setIsSuccess(true);
    queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    return queryControlResult;
  }

  @Override
  public GroupSourceManager getGroupSourceManager(final String groupId) {
    // This method should not be used in option 3.
    throw new RuntimeException("getGroupSourceManager should not be used in option 3.");
  }

}
