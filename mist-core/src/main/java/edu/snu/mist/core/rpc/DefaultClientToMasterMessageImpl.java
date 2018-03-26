/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.master.ApplicationCodeManager;
import edu.snu.mist.core.master.allocation.QueryAllocationManager;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.JarUploadResult;
import edu.snu.mist.formats.avro.QuerySubmitInfo;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * The default implementation for ClientToMasterMessage.
 */
public final class DefaultClientToMasterMessageImpl implements ClientToMasterMessage {

  private static final Logger LOG = Logger.getLogger(DefaultClientToMasterMessageImpl.class.getName());

  /**
   * The query allocation manager.
   */
  private final QueryAllocationManager queryAllocationManager;

  /**
   * The application-code manager.
   */
  private final ApplicationCodeManager appCodeManager;

  /**
   * The query Id generator.
   */
  private final AtomicLong queryIdGenerator;

  @Inject
  private DefaultClientToMasterMessageImpl(final QueryAllocationManager queryAllocationManager,
                                           final ApplicationCodeManager appCodeManager) {
    this.queryAllocationManager = queryAllocationManager;
    this.appCodeManager = appCodeManager;
    this.queryIdGenerator = new AtomicLong();
  }

  @Override
  public JarUploadResult uploadJarFiles(final List<ByteBuffer> jarFile) throws AvroRemoteException {
    return appCodeManager.registerNewAppCode(jarFile);
  }

  @Override
  public QuerySubmitInfo getQuerySubmitInfo(final String appId) {
    // TODO: [MIST-997] Support application-aware query allocation.
    return QuerySubmitInfo.newBuilder()
        .setJarPaths(appCodeManager.getJarPaths(appId))
        .setTask(queryAllocationManager.getAllocatedTask(appId))
        .setQueryId(String.valueOf(queryIdGenerator.getAndIncrement()))
        .build();
  }
}
