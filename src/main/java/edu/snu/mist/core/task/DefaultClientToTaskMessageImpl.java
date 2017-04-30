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

import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.JarUploadResult;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class implements the RPC protocol of ClientToTaskMessage.
 * It creates the query id and returns it to users.
 * Also, it submits the tuple of queryId and logical plan to QueryManager in order to execute the query,
 * or submits the queryId to delete, stop and resume the corresponding query.
 */
public final class DefaultClientToTaskMessageImpl implements ClientToTaskMessage {
  private static final Logger LOG = Logger.getLogger(DefaultClientToTaskMessageImpl.class.getName());
  /**
   * A query manager which manages the submitted query.
   */
  private final QueryManager queryManager;

  /**
   * A generator of query id.
   */
  private final QueryIdGenerator queryIdGenerator;

  /**
   * A query info store that saves jar files and logical plans.
   */
  private final QueryInfoStore queryInfoStore;

  @Inject
  private DefaultClientToTaskMessageImpl(final QueryIdGenerator queryIdGenerator,
                                         final QueryManager queryManager,
                                         final QueryInfoStore queryInfoStore) {
    this.queryIdGenerator = queryIdGenerator;
    this.queryManager = queryManager;
    this.queryInfoStore = queryInfoStore;
  }

  @Override
  public JarUploadResult uploadJarFiles(final List<ByteBuffer> jarFiles) throws AvroRemoteException {
    try {
      final List<String> paths = queryInfoStore.saveJar(jarFiles);
      final JarUploadResult result = JarUploadResult.newBuilder()
          .setIsSuccess(true)
          .setMsg("Success")
          .setPaths(paths)
          .build();
      return result;
    } catch (final IOException e) {
      LOG.log(Level.SEVERE, "An exception occurred while storing jar files {0}", e.getMessage());
      final JarUploadResult result = JarUploadResult.newBuilder()
          .setIsSuccess(false)
          .setMsg(e.getMessage())
          .setPaths(null)
          .build();
      return result;
    }
  }

  @Override
  public QueryControlResult sendQueries(final AvroOperatorChainDag chainDag) throws AvroRemoteException {
    final String queryId = queryIdGenerator.generate(chainDag);
    return queryManager.create(new Tuple<>(queryId, chainDag));
  }

  /**
   *  TODO[DELETE] this code is for test.
   */
  @Override
  public QueryControlResult sendBatchQueries(final AvroOperatorChainDag chainDag,
                                             final int batchSize) throws AvroRemoteException {
    final List<String> queryIdList = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      queryIdList.add(queryIdGenerator.generate(chainDag));
    }
    return queryManager.createBatch(new Tuple<>(queryIdList, chainDag));
  }

  @Override
  public QueryControlResult deleteQueries(final String groupId, final String queryId) throws AvroRemoteException {
    return queryManager.delete(groupId, queryId);
  }
}
