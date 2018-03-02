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

import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.JarUploadResult;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.avro.AvroRemoteException;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A task server for test.
 */
public class MockTaskServer implements ClientToTaskMessage {

  private final String testQueryResult;

  public MockTaskServer(final String testQueryResult) {
    this.testQueryResult = testQueryResult;
  }

  @Override
  public JarUploadResult uploadJarFiles(final List<ByteBuffer> jarFile) throws AvroRemoteException {
    return new JarUploadResult(true, "success", "test1");
  }

  @Override
  public QueryControlResult sendQueries(final AvroDag avroDag) throws AvroRemoteException {
    return new QueryControlResult(testQueryResult, true, testQueryResult);
  }

  @Override
  public QueryControlResult deleteQueries(final String groupId, final String queryId) throws AvroRemoteException {
    return new QueryControlResult(testQueryResult, true, testQueryResult);
  }
}