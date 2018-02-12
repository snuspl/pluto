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
package edu.snu.mist.common.rpc;

import edu.snu.mist.core.master.TaskAddressAndLoadInfoMap;
import edu.snu.mist.formats.avro.SendTaskAddressAndLoadInfoResult;
import edu.snu.mist.formats.avro.TaskAddressAndLoadInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;

/**
 * Default implementation of task to master message.
 */
public final class DefaultTaskToMasterMessageImpl implements TaskToMasterMessage {

  private final TaskAddressAndLoadInfoMap taskAddressAndLoadInfoMap;

  @Inject
  private DefaultTaskToMasterMessageImpl(final TaskAddressAndLoadInfoMap taskAddressAndLoadInfoMap) {
    this.taskAddressAndLoadInfoMap = taskAddressAndLoadInfoMap;
  }

  @Override
  public SendTaskAddressAndLoadInfoResult sendTaskAddressAndLoadInfo(
      final TaskAddressAndLoadInfo taskAddressAndLoadInfo) throws AvroRemoteException {
    taskAddressAndLoadInfoMap.put(taskAddressAndLoadInfo.getTaskIPAddress(),
                                  taskAddressAndLoadInfo.getTaskLoadInfo());
    final SendTaskAddressAndLoadInfoResult result = SendTaskAddressAndLoadInfoResult.newBuilder()
            .setIsSuccess(true)
            .setMsg("Success")
            .build();
    return result;
  }
}
