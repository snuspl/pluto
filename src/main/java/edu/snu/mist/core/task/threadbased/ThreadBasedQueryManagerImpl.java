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
package edu.snu.mist.core.task.threadbased;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.QueryControlResult;
import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
public final class ThreadBasedQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(ThreadBasedQueryManagerImpl.class.getName());

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
   * Map that has the Operator chain as a key and the thread as a value.
   */
  private final Set<Thread> threads;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;

  /**
   * The Manager for collecting task load information and sending it to the master.
   */
  private final TaskLoadManager taskLoadManager;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private ThreadBasedQueryManagerImpl(final DagGenerator dagGenerator,
                                      final ScheduledExecutorServiceWrapper schedulerWrapper,
                                      final QueryInfoStore planStore,
                                      final ConfigDagGenerator configDagGenerator,
                                      final BatchQueryCreator batchQueryCreator,
                                      final TaskLoadManager taskLoadManager) {
    this.dagGenerator = dagGenerator;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
    this.threads = new ConcurrentSet<>();
    this.batchQueryCreator = batchQueryCreator;
    this.configDagGenerator = configDagGenerator;
    this.taskLoadManager = taskLoadManager;
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
    try {
      // Create the submitted query
      // 1) Saves the avro dag to the PlanStore and
      // converts the avro dag to the logical and execution dag
      planStore.saveAvroDag(tuple);

      final DAG<ConfigVertex, MISTEdge> configDag = configDagGenerator.generate(tuple.getValue());
      final ExecutionDag executionDag =
          dagGenerator.generate(configDag, tuple.getValue().getJarFilePaths());

      // Execute the execution dag
      start(executionDag);

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
  // Return false if the queue is empty or the previously event processing is not finished.
  private boolean processNextEvent(
      final BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue) throws InterruptedException {
    final Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>> event = queue.take();
      for (final Map.Entry<ExecutionVertex, MISTEdge> entry : event.getValue().entrySet()) {
        process(event.getKey(), entry.getValue().getDirection(), (PhysicalOperator)entry.getKey());
      }
    return true;
  }

  private void process(final MistEvent event,
                       final Direction direction,
                       final PhysicalOperator operator) {
    try {
      if (event.isData()) {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftData((MistDataEvent) event);
        } else {
          operator.getOperator().processRightData((MistDataEvent) event);
        }
        operator.setLatestDataTimestamp(event.getTimestamp());
      } else {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftWatermark((MistWatermarkEvent) event);
        } else {
          operator.getOperator().processRightWatermark((MistWatermarkEvent) event);
        }
        operator.setLatestWatermarkTimestamp(event.getTimestamp());
      }
    } catch (final NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of the query
   */
  private void start(final ExecutionDag physicalPlan) {
    final List<PhysicalSource> sources = new LinkedList<>();
    final DAG<ExecutionVertex, MISTEdge> dag = physicalPlan.getDag();
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(dag);
    final BlockingQueue<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> eventQueue = new LinkedBlockingQueue<>();

    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = dag.getEdges(source);
          // 3) Sets output emitters
          source.setOutputEmitter(new ThreadBasedSourceOutputEmitter<>(nextOps, eventQueue));
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

    // Create a thread
    final Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          try {
            processNextEvent(eventQueue);
          } catch (InterruptedException e) {
            // try again
          }
        }
      }
    });

    threads.add(t);
    t.start();

    // 4) starts to receive input data stream from the sources
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    planStore.close();
    for (final Thread thread : threads) {
      thread.interrupt();
    }
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
