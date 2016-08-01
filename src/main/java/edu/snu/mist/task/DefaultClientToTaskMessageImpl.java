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

import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.formats.avro.QuerySubmissionResult;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;

/**
 * This class implements the RPC protocol of ClientToTaskMessage.
 * It creates the query id and returns it to users.
 * Also, it submits the tuple of queryId and logical plan to QueryManager in order to execute the query,
 * or submits the queryId to delete query.
 */
public final class DefaultClientToTaskMessageImpl implements ClientToTaskMessage {
  /**
   * A query manager which manages the submitted query.
   */
  private final QueryManager queryManager;

  /**
   * A generator of query id.
   */
  private final QueryIdGenerator queryIdGenerator;

  @Inject
  private DefaultClientToTaskMessageImpl(final QueryIdGenerator queryIdGenerator,
                                         final QueryManager queryManager) {
    this.queryIdGenerator = queryIdGenerator;
    this.queryManager = queryManager;
  }

  @Override
  public QuerySubmissionResult sendQueries(final LogicalPlan logicalPlan) throws AvroRemoteException {
    final String queryId = queryIdGenerator.generate(logicalPlan);
    queryManager.create(new Tuple<>(queryId, logicalPlan));
    // Return the query Id
    final QuerySubmissionResult querySubmissionResult = new QuerySubmissionResult();
    querySubmissionResult.setQueryId(queryId);
    return querySubmissionResult;
  }

  @Override
  public boolean deleteQueries(final CharSequence queryId) throws AvroRemoteException {
    return queryManager.delete(queryId.toString());
  }
}
