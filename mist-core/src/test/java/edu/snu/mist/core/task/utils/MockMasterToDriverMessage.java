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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;

import java.util.List;
import java.util.Map;

/**
 * The mock master-to-driver avro server for test.
 */
public final class MockMasterToDriverMessage implements MasterToDriverMessage {

  public MockMasterToDriverMessage() {
    // Do nothing...
  }

  @Override
  public Void requestNewTask(final TaskRequest taskRequest) throws AvroRemoteException {
    return null;
  }

  @Override
  public boolean stopTask(final String taskHostname) throws AvroRemoteException {
    return true;
  }

  @Override
  public boolean saveJarInfo(final String appId,
                             final List<String> jarPaths) throws AvroRemoteException {
    return true;
  }

  @Override
  public Map<String, List<String>> retrieveJarInfo() throws AvroRemoteException {
    return null;
  }

  @Override
  public List<String> retrieveRunningTaskInfo() throws AvroRemoteException {
    return null;
  }
}
