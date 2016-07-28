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
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.Source;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * DefaultQueryDeleterImpl does the following things:
 * 1) receives queryId from clients and receives chained physical plan from QueryReceiver,
 * 2) and deletes PartitionedQueries from the PartitionedQueryManager,
 * 3) and sets the OutputEmitters of the Source and PartitionedQueries to null,
 * 4) and closes the channel of Source and Sink.
 */
@SuppressWarnings("unchecked")
final class DefaultQueryDeleterImpl implements QueryDeleter {

  private static final Logger LOG = Logger.getLogger(DefaultQueryDeleterImpl.class.getName());
  /**
   * A query receiver which receives the submitted query.
   */
  private final QueryReceiver queryReceiver;

  /**
   * A partitioned query manager.
   */
  private final PartitionedQueryManager queryManager;

  @Inject
  private DefaultQueryDeleterImpl(final QueryReceiver queryReceiver,
                                  final PartitionedQueryManager queryManager) {
    this.queryManager = queryManager;
    this.queryReceiver = queryReceiver;
  }

  @Override
  public boolean delete(final String queryId) {
    final PhysicalPlan<PartitionedQuery> chainedPlan = getChainedPlan(queryId);

    if (chainedPlan != null) {
      // Deletes the PartitionedQueries to PartitionedQueryManager.
      final DAG<PartitionedQuery> chainedOperators = chainedPlan.getOperators();
      final Iterator<PartitionedQuery> partitionedQueryIterator = GraphUtils.topologicalSort(chainedOperators);
      while (partitionedQueryIterator.hasNext()) {
        final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
        queryManager.delete(partitionedQuery);
      }
      close(chainedPlan);
      return true;
    }
    return false;
  }

  private PhysicalPlan<PartitionedQuery> getChainedPlan(final String queryId) {
    return queryReceiver.getPhysicalPlanMap().remove(queryId);
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks to null
   * and closes the channel of Sink and Source.
   * @param chainPhysicalPlan
   */
  private void close(final PhysicalPlan<PartitionedQuery> chainPhysicalPlan) {
    for (final Map.Entry<Source, Set<PartitionedQuery>> entry :
        chainPhysicalPlan.getSourceMap().entrySet()) {
      final Source src = entry.getKey();
      try {
        src.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      src.setOutputEmitter(null);
    }

    for (final Map.Entry<PartitionedQuery, Set<Sink>> entry :
        chainPhysicalPlan.getSinkMap().entrySet()) {
      final PartitionedQuery partitionedQuery = entry.getKey();
      partitionedQuery.setOutputEmitter(null);

      try {
        Iterator<Sink> iterator = entry.getValue().iterator();
        while (iterator.hasNext()) {
          final Sink sink = iterator.next();
          sink.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
