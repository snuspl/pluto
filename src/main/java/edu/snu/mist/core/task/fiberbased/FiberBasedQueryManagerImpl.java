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
package edu.snu.mist.core.task.fiberbased;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.FiberForkJoinScheduler;
import co.paralleluniverse.fibers.FiberScheduler;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.SuspendableRunnable;
import co.paralleluniverse.strands.channels.Channel;
import co.paralleluniverse.strands.channels.Channels;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.batchsub.BatchQueryCreator;
import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.QueryControlResult;
import io.netty.util.internal.ConcurrentSet;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This QueryManager is aware of the group and manages queries per group.
 */
@SuppressWarnings("unchecked")
public final class FiberBasedQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(FiberBasedQueryManagerImpl.class.getName());

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
  private final Set<Fiber> fibers;

  /**
   * A batch query submission helper.
   */
  private final BatchQueryCreator batchQueryCreator;

  /**
   * A dag generator that creates DAG<ConfigVertex, MISTEdge> from avro dag.
   */
  private final ConfigDagGenerator configDagGenerator;


  private final FiberScheduler scheduler;

  /**
   * Default query manager in MistTask.
   */
  @Inject
  private FiberBasedQueryManagerImpl(final DagGenerator dagGenerator,
                                     final ScheduledExecutorServiceWrapper schedulerWrapper,
                                     final QueryInfoStore planStore,
                                     final ConfigDagGenerator configDagGenerator,
                                     @Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                     final BatchQueryCreator batchQueryCreator) {
    this.dagGenerator = dagGenerator;
    this.scheduler = new FiberForkJoinScheduler("test", defaultNumEventProcessors, null, false);
    this.planStore = planStore;
    this.fibers = new ConcurrentSet<>();
    this.batchQueryCreator = batchQueryCreator;
    this.configDagGenerator = configDagGenerator;
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
      final long st = System.currentTimeMillis();

      batchQueryCreator.duplicate(tuple, this);
      final long et = System.currentTimeMillis();

      System.out.println("Creation time: " + (et - st) + "ms");

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
      final Channel<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue)
      throws InterruptedException, SuspendExecution {
    final Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>> event = queue.receive();
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
    final Channel<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> eventQueue = Channels.newChannel(50);

    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = dag.getEdges(source);
          // 3) Sets output emitters
          source.setOutputEmitter(new FiberBasedSourceOutputEmitter<>(nextOps, eventQueue));
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
    final Fiber fiber = new Fiber(scheduler, new SuspendableRunnable() {
      @Override
      public void run() throws SuspendExecution {
        while (!Fiber.interrupted()) {
          try {
            processNextEvent(eventQueue);
          } catch (InterruptedException e) {
            break;
          }
        }
      }
    }).start();

    fibers.add(fiber);

    // 4) starts to receive input data stream from the sources
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  @Override
  public void close() throws Exception {
    //scheduler.shutdown();
    planStore.close();
    for (final Fiber fiber : fibers) {
      fiber.interrupt();
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
