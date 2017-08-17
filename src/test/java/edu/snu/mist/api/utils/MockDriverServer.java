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
package edu.snu.mist.api.utils;

import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.MistTaskProvider;
import edu.snu.mist.formats.avro.QueryInfo;
import edu.snu.mist.formats.avro.TaskList;
import org.apache.avro.AvroRemoteException;

import java.util.Arrays;

/**
 * A task provider for test.
 */
public class MockDriverServer implements ClientToMasterMessage {
  private final String driverHost;
  private final int taskPortNum;

  public MockDriverServer(final String driverHost,
                          final int taskPortNum) {
    this.driverHost = driverHost;
    this.taskPortNum = taskPortNum;
  }

  @Override
  public ClientToMasterMessage getTask(final QueryInfo queryInfo) throws AvroRemoteException {
    return new TaskList(Arrays.asList(new IPAddress(driverHost, taskPortNum)));
  }
}