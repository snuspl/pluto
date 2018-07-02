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
import edu.snu.mist.core.master.MasterSetupFinished;
import edu.snu.mist.core.master.lb.allocation.QueryAllocationManager;
import edu.snu.mist.core.master.QueryIdGenerator;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.JarUploadResult;
import edu.snu.mist.formats.avro.QuerySubmitInfo;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.List;
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
   * A variable that represents master setup is finished or not.
   */
  private final MasterSetupFinished masterSetupFinished;

  /**
   * The query Id generator.
   */
  private final QueryIdGenerator queryIdGenerator;

  @Inject
  private DefaultClientToMasterMessageImpl(final QueryAllocationManager queryAllocationManager,
                                           final ApplicationCodeManager appCodeManager,
                                           final MasterSetupFinished masterSetupFinished,
                                           final QueryIdGenerator queryIdGenerator) {
    this.queryAllocationManager = queryAllocationManager;
    this.appCodeManager = appCodeManager;
    this.masterSetupFinished = masterSetupFinished;
    this.queryIdGenerator = queryIdGenerator;
  }

  @Override
  public boolean isReady() throws AvroRemoteException {
    return masterSetupFinished.isFinished();
  }

  @Override
  public JarUploadResult uploadJarFiles(final List<ByteBuffer> jarFile) {
    try {
      return appCodeManager.registerNewAppCode(jarFile);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public QuerySubmitInfo getQuerySubmitInfo(final String appId) {
    try {
      return QuerySubmitInfo.newBuilder()
          .setJarPaths(appCodeManager.getJarPaths(appId))
          .setQueryId(queryIdGenerator.generate())
          .setTask(queryAllocationManager.getAllocatedTask(appId))
          .build();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
