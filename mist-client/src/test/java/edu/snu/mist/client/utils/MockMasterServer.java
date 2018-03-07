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
package edu.snu.mist.client.utils;

import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.JarUploadResult;
import edu.snu.mist.formats.avro.QuerySubmitInfo;
import org.apache.avro.AvroRemoteException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A task provider for test.
 */
public class MockMasterServer implements ClientToMasterMessage {
  private final String taskHost;
  private final int taskPortNum;

  public MockMasterServer(final String taskHost,
                          final int taskPortNum) {
    this.taskHost = taskHost;
    this.taskPortNum = taskPortNum;
  }

  @Override
  public JarUploadResult uploadJarFiles(final List<ByteBuffer> jarFiles) {
    return JarUploadResult.newBuilder()
        .setIsSuccess(true)
        .setMsg("Hi!")
        .setIdentifier("app_id")
        .setJarPaths(new ArrayList<>())
        .build();
  }

  @Override
  public QuerySubmitInfo getQuerySubmitInfo(final String appId) throws AvroRemoteException {
    return QuerySubmitInfo.newBuilder()
        .setJarPaths(new ArrayList())
        .setTask(new IPAddress(taskHost, taskPortNum))
        .build();
  }
}